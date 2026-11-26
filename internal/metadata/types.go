// Package metadata is the metadata plane of StorageMesh: buckets, objects,
// versions, the chunk reference table, and multipart uploads.
//
// It defines a Store interface and ships two implementations:
//
//   - MemStore   — an in-memory store used by unit tests and local demos.
//   - PostgresStore — the real store backed by PostgreSQL (see postgres.go).
//
// Everything above this package (the gateway, the workers) depends only on the
// Store interface, never on Postgres directly. That inversion is what lets the
// entire upload/download/GC logic be tested with no database running.
package metadata

import (
	"context"
	"errors"
	"time"

	"github.com/1morshed1/storagemesh/internal/chunk"
)

// Sentinel errors let callers branch on outcomes (e.g. 404 vs 500) without
// string matching.
var (
	ErrBucketNotFound = errors.New("metadata: bucket not found")
	ErrBucketExists   = errors.New("metadata: bucket already exists")
	ErrObjectNotFound = errors.New("metadata: object not found")
	ErrUploadNotFound = errors.New("metadata: multipart upload not found")
	ErrBucketNotEmpty = errors.New("metadata: bucket not empty")
)

// Bucket is a namespace for objects, owned by a user. Versioning can be toggled
// per bucket (mirrors S3 semantics).
type Bucket struct {
	ID         string
	Name       string
	OwnerID    string
	Versioning bool
	CreatedAt  time.Time
}

// Object is one version of one key in a bucket. When versioning is on, a key can
// have many Object rows; exactly one is IsLatest. A delete on a versioned bucket
// writes a DeleteMarker (an Object with DeleteMarker=true) rather than removing
// history.
type Object struct {
	ID           string
	BucketID     string
	Key          string
	VersionID    string
	SizeBytes    int64
	ContentType  string
	ChecksumSHA  string          // SHA-256 of the whole object (the ETag)
	Manifest     *chunk.Manifest // ordered chunk recipe
	IsLatest     bool
	DeleteMarker bool
	Metadata     map[string]string // user-defined x-amz-meta-* style headers
	CreatedAt    time.Time
}

// ChunkRef is the reference-counted record of a stored chunk. ref_count tracks
// how many object versions point at this chunk; GC deletes the chunk only when
// it reaches zero. Nodes lists which storage nodes hold the bytes.
type ChunkRef struct {
	Hash       string
	SizeBytes  int
	RefCount   int
	Nodes      []string
	VerifiedAt *time.Time
}

// MultipartUpload tracks an in-progress multipart upload. Parts accumulate until
// the client completes (assemble into an object) or aborts the upload.
type MultipartUpload struct {
	ID        string
	BucketID  string
	Key       string
	Status    string // in_progress | completed | aborted
	Parts     []Part
	CreatedAt time.Time
	ExpiresAt time.Time
}

// Part is one uploaded piece of a multipart upload. Each part is itself chunked;
// the part's manifest is concatenated into the final object's manifest on
// completion.
type Part struct {
	PartNumber int
	ETag       string
	SizeBytes  int64
	Manifest   *chunk.Manifest
}

// Store is the metadata persistence contract. Implementations must be safe for
// concurrent use by many goroutines (the gateway is concurrent).
type Store interface {
	// Buckets
	CreateBucket(ctx context.Context, b *Bucket) error
	GetBucket(ctx context.Context, name string) (*Bucket, error)
	ListBuckets(ctx context.Context, ownerID string) ([]*Bucket, error)
	DeleteBucket(ctx context.Context, name string) error

	// Objects
	PutObject(ctx context.Context, o *Object) error
	GetObject(ctx context.Context, bucketID, key string) (*Object, error)
	GetObjectVersion(ctx context.Context, bucketID, key, versionID string) (*Object, error)
	ListObjects(ctx context.Context, bucketID, prefix string, limit int) ([]*Object, error)
	// DeleteObject removes the latest version (or writes a delete marker on a
	// versioned bucket). Returns the chunk hashes whose ref_count must drop.
	DeleteObject(ctx context.Context, bucketID, key string) error

	// Chunk reference counting
	IncrementChunkRef(ctx context.Context, ref ChunkRef) (isNew bool, err error)
	DecrementChunkRefs(ctx context.Context, hashes []string) error
	GetChunk(ctx context.Context, hash string) (*ChunkRef, error)
	// ListOrphanChunks returns chunks with ref_count<=0 older than the grace
	// period — GC candidates.
	ListOrphanChunks(ctx context.Context, olderThan time.Duration) ([]*ChunkRef, error)
	DeleteChunkRef(ctx context.Context, hash string) error
	MarkChunkVerified(ctx context.Context, hash string, at time.Time) error

	// Multipart uploads
	CreateMultipartUpload(ctx context.Context, u *MultipartUpload) error
	GetMultipartUpload(ctx context.Context, id string) (*MultipartUpload, error)
	AddPart(ctx context.Context, uploadID string, part Part) error
	CompleteMultipartUpload(ctx context.Context, id string) (*MultipartUpload, error)
	AbortMultipartUpload(ctx context.Context, id string) error

	Close() error
}
