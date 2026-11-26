package metadata

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// MemStore is an in-memory Store. It exists so the gateway, workers, and their
// tests can run with zero external dependencies. It is guarded by a single mutex
// — simple and correct, which is exactly what a test double should be. It is NOT
// meant for production (no durability, everything lost on restart).
//
// Keeping MemStore faithful to Postgres semantics (especially ref-count
// atomicity and delete-marker behaviour) is what makes tests trustworthy; see
// decision D-5.
type MemStore struct {
	mu      sync.Mutex
	buckets map[string]*Bucket          // name -> bucket
	objects map[string][]*Object        // bucketID+"\x00"+key -> version list (newest last)
	chunks  map[string]*ChunkRef        // hash -> ref
	uploads map[string]*MultipartUpload // id -> upload
}

// NewMemStore returns an empty in-memory store.
func NewMemStore() *MemStore {
	return &MemStore{
		buckets: make(map[string]*Bucket),
		objects: make(map[string][]*Object),
		chunks:  make(map[string]*ChunkRef),
		uploads: make(map[string]*MultipartUpload),
	}
}

func objKey(bucketID, key string) string { return bucketID + "\x00" + key }

// ---- Buckets ----

func (m *MemStore) CreateBucket(_ context.Context, b *Bucket) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.buckets[b.Name]; ok {
		return ErrBucketExists
	}
	if b.ID == "" {
		b.ID = uuid.NewString()
	}
	if b.CreatedAt.IsZero() {
		b.CreatedAt = time.Now()
	}
	cp := *b
	m.buckets[b.Name] = &cp
	return nil
}

func (m *MemStore) GetBucket(_ context.Context, name string) (*Bucket, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.buckets[name]
	if !ok {
		return nil, ErrBucketNotFound
	}
	cp := *b
	return &cp, nil
}

func (m *MemStore) ListBuckets(_ context.Context, ownerID string) ([]*Bucket, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*Bucket
	for _, b := range m.buckets {
		if ownerID == "" || b.OwnerID == ownerID {
			cp := *b
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (m *MemStore) DeleteBucket(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.buckets[name]
	if !ok {
		return ErrBucketNotFound
	}
	// Refuse to delete a bucket that still has objects (mirrors S3).
	for k, versions := range m.objects {
		if strings.HasPrefix(k, b.ID+"\x00") && len(versions) > 0 {
			for _, v := range versions {
				if !v.DeleteMarker {
					return ErrBucketNotEmpty
				}
			}
		}
	}
	delete(m.buckets, name)
	return nil
}

// ---- Objects ----

func (m *MemStore) PutObject(_ context.Context, o *Object) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if o.ID == "" {
		o.ID = uuid.NewString()
	}
	if o.VersionID == "" {
		o.VersionID = uuid.NewString()
	}
	if o.CreatedAt.IsZero() {
		o.CreatedAt = time.Now()
	}
	k := objKey(o.BucketID, o.Key)
	// New version becomes latest; demote previous versions.
	for _, prev := range m.objects[k] {
		prev.IsLatest = false
	}
	o.IsLatest = true
	cp := *o
	m.objects[k] = append(m.objects[k], &cp)
	return nil
}

func (m *MemStore) GetObject(_ context.Context, bucketID, key string) (*Object, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	versions := m.objects[objKey(bucketID, key)]
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].IsLatest {
			if versions[i].DeleteMarker {
				return nil, ErrObjectNotFound // latest is a tombstone
			}
			cp := *versions[i]
			return &cp, nil
		}
	}
	return nil, ErrObjectNotFound
}

func (m *MemStore) GetObjectVersion(_ context.Context, bucketID, key, versionID string) (*Object, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, v := range m.objects[objKey(bucketID, key)] {
		if v.VersionID == versionID {
			cp := *v
			return &cp, nil
		}
	}
	return nil, ErrObjectNotFound
}

func (m *MemStore) ListObjects(_ context.Context, bucketID, prefix string, limit int) ([]*Object, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*Object
	for k, versions := range m.objects {
		if !strings.HasPrefix(k, bucketID+"\x00") {
			continue
		}
		key := strings.TrimPrefix(k, bucketID+"\x00")
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			continue
		}
		for i := len(versions) - 1; i >= 0; i-- {
			if versions[i].IsLatest && !versions[i].DeleteMarker {
				cp := *versions[i]
				out = append(out, &cp)
				break
			}
		}
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (m *MemStore) DeleteObject(_ context.Context, bucketID, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := objKey(bucketID, key)
	versions, ok := m.objects[k]
	if !ok || len(versions) == 0 {
		return ErrObjectNotFound
	}
	// Simple (non-versioned) delete: drop all versions. Ref-count decrements are
	// handled by the caller, which reads the manifest first. We model the
	// metadata removal here.
	delete(m.objects, k)
	return nil
}

// ---- Chunk ref counting ----

func (m *MemStore) IncrementChunkRef(_ context.Context, ref ChunkRef) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	existing, ok := m.chunks[ref.Hash]
	if !ok {
		cp := ref
		if cp.RefCount == 0 {
			cp.RefCount = 1
		}
		m.chunks[ref.Hash] = &cp
		return true, nil // isNew: caller must upload the bytes
	}
	existing.RefCount++
	// Merge any newly-known replica nodes.
	if len(ref.Nodes) > 0 {
		existing.Nodes = union(existing.Nodes, ref.Nodes)
	}
	return false, nil // already present: dedup hit, skip upload
}

func (m *MemStore) DecrementChunkRefs(_ context.Context, hashes []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, h := range hashes {
		if c, ok := m.chunks[h]; ok {
			c.RefCount--
		}
	}
	return nil
}

func (m *MemStore) GetChunk(_ context.Context, hash string) (*ChunkRef, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.chunks[hash]
	if !ok {
		return nil, ErrObjectNotFound
	}
	cp := *c
	return &cp, nil
}

func (m *MemStore) ListOrphanChunks(_ context.Context, _ time.Duration) ([]*ChunkRef, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*ChunkRef
	for _, c := range m.chunks {
		if c.RefCount <= 0 {
			cp := *c
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (m *MemStore) DeleteChunkRef(_ context.Context, hash string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.chunks, hash)
	return nil
}

func (m *MemStore) MarkChunkVerified(_ context.Context, hash string, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.chunks[hash]; ok {
		c.VerifiedAt = &at
	}
	return nil
}

// ---- Multipart ----

func (m *MemStore) CreateMultipartUpload(_ context.Context, u *MultipartUpload) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if u.ID == "" {
		u.ID = uuid.NewString()
	}
	if u.Status == "" {
		u.Status = "in_progress"
	}
	if u.CreatedAt.IsZero() {
		u.CreatedAt = time.Now()
	}
	cp := *u
	m.uploads[u.ID] = &cp
	return nil
}

func (m *MemStore) GetMultipartUpload(_ context.Context, id string) (*MultipartUpload, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	u, ok := m.uploads[id]
	if !ok {
		return nil, ErrUploadNotFound
	}
	cp := *u
	return &cp, nil
}

func (m *MemStore) AddPart(_ context.Context, uploadID string, part Part) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	u, ok := m.uploads[uploadID]
	if !ok {
		return ErrUploadNotFound
	}
	// Replace an existing part with the same number (client may re-upload).
	for i, p := range u.Parts {
		if p.PartNumber == part.PartNumber {
			u.Parts[i] = part
			return nil
		}
	}
	u.Parts = append(u.Parts, part)
	return nil
}

func (m *MemStore) CompleteMultipartUpload(_ context.Context, id string) (*MultipartUpload, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	u, ok := m.uploads[id]
	if !ok {
		return nil, ErrUploadNotFound
	}
	u.Status = "completed"
	cp := *u
	return &cp, nil
}

func (m *MemStore) AbortMultipartUpload(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.uploads[id]; !ok {
		return ErrUploadNotFound
	}
	m.uploads[id].Status = "aborted"
	return nil
}

func (m *MemStore) Close() error { return nil }

func union(a, b []string) []string {
	seen := make(map[string]struct{}, len(a))
	for _, x := range a {
		seen[x] = struct{}{}
	}
	for _, x := range b {
		if _, ok := seen[x]; !ok {
			a = append(a, x)
			seen[x] = struct{}{}
		}
	}
	return a
}
