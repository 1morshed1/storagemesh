package gateway

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"sort"
	"strconv"

	"github.com/1morshed1/storagemesh/internal/chunk"
	"github.com/1morshed1/storagemesh/internal/metadata"
)

// Multipart upload lets a client send a large object in independent parts,
// possibly in parallel, and assemble them server-side. The S3 flow is:
//
//  1. POST /bucket/key?uploads            -> initiate, returns uploadId
//  2. PUT  /bucket/key?uploadId&partNumber-> upload each part (repeatable)
//  3. POST /bucket/key?uploadId           -> complete: stitch parts into object
//     DELETE /bucket/key?uploadId         -> abort
//
// Each part is itself chunked and its chunks are replicated exactly like a whole
// object. On completion we concatenate the parts' chunk manifests (in part-number
// order) into the final object manifest — no bytes move, we just record the
// combined recipe. That is another payoff of content addressing: assembly is a
// metadata operation.

// handleMultipart routes POST requests: ?uploads initiates, ?uploadId completes.
func (s *Server) handleMultipart(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	switch {
	case q.Has("uploads"):
		s.initiateMultipart(w, r)
	case q.Get("uploadId") != "":
		s.completeMultipart(w, r)
	default:
		writeError(w, http.StatusBadRequest, "POST requires ?uploads or ?uploadId")
	}
}

func (s *Server) initiateMultipart(w http.ResponseWriter, r *http.Request) {
	bucket, key := r.PathValue("bucket"), r.PathValue("key")
	b, err := s.store.GetBucket(r.Context(), bucket)
	if errors.Is(err, metadata.ErrBucketNotFound) {
		writeError(w, http.StatusNotFound, "bucket not found")
		return
	}
	if err != nil {
		s.serverError(w, "get bucket", err)
		return
	}
	u := &metadata.MultipartUpload{BucketID: b.ID, Key: key}
	if err := s.store.CreateMultipartUpload(r.Context(), u); err != nil {
		s.serverError(w, "create multipart upload", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"bucket": bucket, "key": key, "uploadId": u.ID})
}

// uploadPart handles PUT with ?uploadId&partNumber. Called from handlePutObject.
func (s *Server) uploadPart(w http.ResponseWriter, r *http.Request, uploadID string, partNumber int) {
	defer r.Body.Close()
	if _, err := s.store.GetMultipartUpload(r.Context(), uploadID); err != nil {
		writeError(w, http.StatusNotFound, "upload not found")
		return
	}
	manifest, etag, err := s.ingest(r.Context(), r.Body)
	if err != nil {
		s.serverError(w, "ingest part", err)
		return
	}
	part := metadata.Part{PartNumber: partNumber, ETag: etag, SizeBytes: manifest.TotalSize(), Manifest: manifest}
	if err := s.store.AddPart(r.Context(), uploadID, part); err != nil {
		s.serverError(w, "add part", err)
		return
	}
	w.Header().Set("ETag", `"`+etag+`"`)
	writeJSON(w, http.StatusOK, map[string]any{"partNumber": partNumber, "etag": etag})
}

func (s *Server) completeMultipart(w http.ResponseWriter, r *http.Request) {
	bucket, key := r.PathValue("bucket"), r.PathValue("key")
	uploadID := r.URL.Query().Get("uploadId")

	u, err := s.store.GetMultipartUpload(r.Context(), uploadID)
	if err != nil {
		writeError(w, http.StatusNotFound, "upload not found")
		return
	}

	// Assemble parts in ascending part-number order.
	sort.Slice(u.Parts, func(i, j int) bool { return u.Parts[i].PartNumber < u.Parts[j].PartNumber })

	final := chunk.NewManifest(s.chunkSize)
	etagHash := sha256.New()
	idx := 0
	for _, p := range u.Parts {
		etagHash.Write([]byte(p.ETag)) // multipart ETag folds in each part's ETag
		if p.Manifest == nil {
			continue
		}
		for _, e := range p.Manifest.Chunks {
			e.Index = idx // reindex into the combined object
			idx++
			final.Chunks = append(final.Chunks, e)
		}
	}
	// S3-style multipart ETag: hash-of-part-etags "-" partCount.
	etag := hex.EncodeToString(etagHash.Sum(nil)) + "-" + strconv.Itoa(len(u.Parts))

	obj := &metadata.Object{
		BucketID:    u.BucketID,
		Key:         key,
		SizeBytes:   final.TotalSize(),
		ContentType: "application/octet-stream",
		ChecksumSHA: etag,
		Manifest:    final,
	}
	if err := s.store.PutObject(r.Context(), obj); err != nil {
		s.serverError(w, "put object metadata", err)
		return
	}
	if _, err := s.store.CompleteMultipartUpload(r.Context(), uploadID); err != nil {
		s.serverError(w, "complete upload", err)
		return
	}
	w.Header().Set("ETag", `"`+etag+`"`)
	writeJSON(w, http.StatusOK, map[string]any{"bucket": bucket, "key": key, "etag": etag, "size": obj.SizeBytes})
}

// parsePartNumber extracts a positive part number from the query, if present.
func parsePartNumber(r *http.Request) (int, bool) {
	v := r.URL.Query().Get("partNumber")
	if v == "" {
		return 0, false
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, false
	}
	return n, true
}
