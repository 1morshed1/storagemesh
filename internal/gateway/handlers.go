package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/1morshed1/storagemesh/internal/chunk"
	"github.com/1morshed1/storagemesh/internal/events"
	"github.com/1morshed1/storagemesh/internal/metadata"
)

// ---- Buckets ----

func (s *Server) handleCreateBucket(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("bucket")
	b := &metadata.Bucket{Name: name, OwnerID: s.ownerID}
	// Optional ?versioning=true.
	if r.URL.Query().Get("versioning") == "true" {
		b.Versioning = true
	}
	err := s.store.CreateBucket(r.Context(), b)
	if errors.Is(err, metadata.ErrBucketExists) {
		writeError(w, http.StatusConflict, "bucket already exists")
		return
	}
	if err != nil {
		s.serverError(w, "create bucket", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"bucket": b.Name, "id": b.ID})
}

func (s *Server) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := s.store.ListBuckets(r.Context(), "")
	if err != nil {
		s.serverError(w, "list buckets", err)
		return
	}
	names := make([]string, 0, len(buckets))
	for _, b := range buckets {
		names = append(names, b.Name)
	}
	writeJSON(w, http.StatusOK, map[string]any{"buckets": names})
}

func (s *Server) handleDeleteBucket(w http.ResponseWriter, r *http.Request) {
	err := s.store.DeleteBucket(r.Context(), r.PathValue("bucket"))
	switch {
	case errors.Is(err, metadata.ErrBucketNotFound):
		writeError(w, http.StatusNotFound, "bucket not found")
	case errors.Is(err, metadata.ErrBucketNotEmpty):
		writeError(w, http.StatusConflict, "bucket not empty")
	case err != nil:
		s.serverError(w, "delete bucket", err)
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

func (s *Server) handleListObjects(w http.ResponseWriter, r *http.Request) {
	b, err := s.store.GetBucket(r.Context(), r.PathValue("bucket"))
	if errors.Is(err, metadata.ErrBucketNotFound) {
		writeError(w, http.StatusNotFound, "bucket not found")
		return
	}
	if err != nil {
		s.serverError(w, "get bucket", err)
		return
	}
	objs, err := s.store.ListObjects(r.Context(), b.ID, r.URL.Query().Get("prefix"), 1000)
	if err != nil {
		s.serverError(w, "list objects", err)
		return
	}
	type item struct {
		Key  string `json:"key"`
		Size int64  `json:"size"`
		ETag string `json:"etag"`
	}
	out := make([]item, 0, len(objs))
	for _, o := range objs {
		out = append(out, item{Key: o.Key, Size: o.SizeBytes, ETag: o.ChecksumSHA})
	}
	writeJSON(w, http.StatusOK, map[string]any{"bucket": b.Name, "objects": out})
}

// ---- Object PUT (the upload flow) ----

// handlePutObject implements the streaming upload described in
// docs/05-upload-download-flow.md: chunk the body, dedup + quorum-replicate each
// chunk, then commit the object metadata, then publish an event.
func (s *Server) handlePutObject(w http.ResponseWriter, r *http.Request) {
	bucket, key := r.PathValue("bucket"), r.PathValue("key")
	if !s.authorizeObject(r, bucket, key) {
		writeError(w, http.StatusForbidden, "not authorized")
		return
	}

	// A PUT carrying ?uploadId&partNumber is a multipart part upload, not a
	// whole-object PUT. Route it there.
	if uploadID := r.URL.Query().Get("uploadId"); uploadID != "" {
		if pn, ok := parsePartNumber(r); ok {
			s.uploadPart(w, r, uploadID, pn)
			return
		}
		writeError(w, http.StatusBadRequest, "partNumber required with uploadId")
		return
	}
	defer r.Body.Close()

	b, err := s.store.GetBucket(r.Context(), bucket)
	if errors.Is(err, metadata.ErrBucketNotFound) {
		writeError(w, http.StatusNotFound, "bucket not found")
		return
	}
	if err != nil {
		s.serverError(w, "get bucket", err)
		return
	}

	manifest, etag, err := s.ingest(r.Context(), r.Body)
	if err != nil {
		s.serverError(w, "ingest object", err)
		return
	}

	obj := &metadata.Object{
		BucketID:    b.ID,
		Key:         key,
		SizeBytes:   manifest.TotalSize(),
		ContentType: headerOr(r, "Content-Type", "application/octet-stream"),
		ChecksumSHA: etag,
		Manifest:    manifest,
	}
	if err := s.store.PutObject(r.Context(), obj); err != nil {
		s.serverError(w, "put object metadata", err)
		return
	}

	// Best-effort event; never fails the request.
	_ = s.publisher.Publish(r.Context(), events.Event{
		Type: events.ObjectCreated, Bucket: bucket, Key: key,
		VersionID: obj.VersionID, Size: obj.SizeBytes, ETag: etag,
	})

	w.Header().Set("ETag", `"`+etag+`"`)
	w.Header().Set("x-version-id", obj.VersionID)
	writeJSON(w, http.StatusOK, map[string]any{"bucket": bucket, "key": key, "etag": etag, "size": obj.SizeBytes})
}

// ingest streams the body through the chunker, dedups and replicates each chunk,
// and returns the assembled manifest plus the whole-object SHA-256 (the ETag).
// It is shared by PutObject and multipart part upload.
func (s *Server) ingest(ctx context.Context, body io.Reader) (*chunk.Manifest, string, error) {
	chunker := chunk.NewChunker(body, s.chunkSize)
	manifest := chunk.NewManifest(s.chunkSize)
	fileHash := sha256.New() // running hash of the WHOLE object -> ETag

	for {
		c, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, "", err
		}
		fileHash.Write(c.Data)

		if err := s.storeChunk(ctx, c); err != nil {
			return nil, "", err
		}
		manifest.Append(c)
	}
	return manifest, hex.EncodeToString(fileHash.Sum(nil)), nil
}

// storeChunk handles one chunk: the atomic ref-count upsert doubles as the dedup
// gate. If we created the row (isNew), we must replicate the bytes; if it already
// existed, the bytes are already stored elsewhere and we skip the upload
// (deduplication). On a replication failure we roll back the ref increment so we
// don't leak a reference to bytes that were never stored.
func (s *Server) storeChunk(ctx context.Context, c chunk.Chunk) error {
	targets := s.cluster.GetReplicas(c.Hash)
	nodeIDs := make([]string, len(targets))
	for i, t := range targets {
		nodeIDs[i] = t.nodeID
	}

	isNew, err := s.store.IncrementChunkRef(ctx, metadata.ChunkRef{
		Hash: c.Hash, SizeBytes: c.Size(), Nodes: nodeIDs,
	})
	if err != nil {
		return err
	}
	if !isNew {
		return nil // dedup hit: bytes already stored, ref count bumped
	}

	stored, err := replicateChunk(ctx, c.Hash, c.Data, targets, s.cluster.ReplicationFactor())
	if err != nil {
		// Roll back the reference we just took so GC won't think bytes exist.
		_ = s.store.DecrementChunkRefs(ctx, []string{c.Hash})
		return err
	}
	s.log.Debug("chunk replicated", "hash", short(c.Hash), "nodes", stored)
	return nil
}

// ---- Object GET (the read flow) ----

func (s *Server) handleGetObject(w http.ResponseWriter, r *http.Request) {
	bucket, key := r.PathValue("bucket"), r.PathValue("key")

	// ?presign=<METHOD>[&expiry=<seconds>] mints a pre-signed URL for the
	// object instead of downloading it. Minting requires real credentials (the
	// access key), because a pre-signed URL is a bearer capability.
	if method := r.URL.Query().Get("presign"); method != "" {
		s.handleGeneratePresign(w, r, bucket, key, method)
		return
	}

	if !s.authorizeObject(r, bucket, key) {
		writeError(w, http.StatusForbidden, "not authorized")
		return
	}

	b, err := s.store.GetBucket(r.Context(), bucket)
	if errors.Is(err, metadata.ErrBucketNotFound) {
		writeError(w, http.StatusNotFound, "bucket not found")
		return
	}
	if err != nil {
		s.serverError(w, "get bucket", err)
		return
	}

	obj, err := s.store.GetObject(r.Context(), b.ID, key)
	if errors.Is(err, metadata.ErrObjectNotFound) {
		writeError(w, http.StatusNotFound, "object not found")
		return
	}
	if err != nil {
		s.serverError(w, "get object", err)
		return
	}

	w.Header().Set("Content-Type", obj.ContentType)
	w.Header().Set("ETag", `"`+obj.ChecksumSHA+`"`)
	w.Header().Set("x-version-id", obj.VersionID)
	// Stream chunk by chunk. We do NOT set Content-Length from a buffer; we
	// stream straight to the socket so memory stays flat regardless of size.
	for _, entry := range obj.Manifest.Chunks {
		if err := s.streamChunk(r.Context(), w, entry.Hash); err != nil {
			// Headers/body may be partially written already; we can only log.
			s.log.Error("stream chunk to client", "hash", short(entry.Hash), "err", err)
			return
		}
	}
}

func (s *Server) streamChunk(ctx context.Context, w io.Writer, hash string) error {
	rc, err := fetchChunk(ctx, hash, s.cluster.GetReplicas(hash))
	if err != nil {
		return err
	}
	defer rc.Close()
	_, err = io.Copy(w, rc)
	return err
}

// ---- Object DELETE ----

func (s *Server) handleDeleteObject(w http.ResponseWriter, r *http.Request) {
	bucket, key := r.PathValue("bucket"), r.PathValue("key")

	// DELETE with ?uploadId aborts an in-progress multipart upload.
	if uploadID := r.URL.Query().Get("uploadId"); uploadID != "" {
		if err := s.store.AbortMultipartUpload(r.Context(), uploadID); err != nil {
			writeError(w, http.StatusNotFound, "upload not found")
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}

	b, err := s.store.GetBucket(r.Context(), bucket)
	if errors.Is(err, metadata.ErrBucketNotFound) {
		writeError(w, http.StatusNotFound, "bucket not found")
		return
	}
	if err != nil {
		s.serverError(w, "get bucket", err)
		return
	}

	// Read the object first so we know which chunks to de-reference. GC will
	// physically remove the bytes once ref_count hits zero.
	obj, err := s.store.GetObject(r.Context(), b.ID, key)
	if errors.Is(err, metadata.ErrObjectNotFound) {
		writeError(w, http.StatusNotFound, "object not found")
		return
	}
	if err != nil {
		s.serverError(w, "get object", err)
		return
	}

	if err := s.store.DeleteObject(r.Context(), b.ID, key); err != nil {
		s.serverError(w, "delete object", err)
		return
	}
	if err := s.store.DecrementChunkRefs(r.Context(), obj.Manifest.Hashes()); err != nil {
		// Metadata is gone but a decrement failed: chunks become slightly
		// over-referenced (leaked), never under. Safe direction; log it.
		s.log.Warn("decrement chunk refs after delete", "key", key, "err", err)
	}

	_ = s.publisher.Publish(r.Context(), events.Event{
		Type: events.ObjectDeleted, Bucket: bucket, Key: key,
	})
	w.WriteHeader(http.StatusNoContent)
}

// ---- helpers ----

func headerOr(r *http.Request, h, def string) string {
	if v := r.Header.Get(h); v != "" {
		return v
	}
	return def
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *Server) serverError(w http.ResponseWriter, op string, err error) {
	s.log.Error(op, "err", err)
	writeError(w, http.StatusInternalServerError, op+" failed")
}
