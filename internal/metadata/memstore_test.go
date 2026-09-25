package metadata

import (
	"context"
	"testing"

	"github.com/1morshed1/storagemesh/internal/chunk"
)

func TestBucketLifecycle(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()

	b := &Bucket{Name: "photos", OwnerID: "user-1"}
	if err := s.CreateBucket(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateBucket(ctx, &Bucket{Name: "photos"}); err != ErrBucketExists {
		t.Fatalf("want ErrBucketExists, got %v", err)
	}
	got, err := s.GetBucket(ctx, "photos")
	if err != nil || got.OwnerID != "user-1" {
		t.Fatalf("GetBucket = %+v, %v", got, err)
	}
	if _, err := s.GetBucket(ctx, "nope"); err != ErrBucketNotFound {
		t.Fatalf("want ErrBucketNotFound, got %v", err)
	}
}

func TestObjectVersioningAndLatest(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	s.CreateBucket(ctx, &Bucket{ID: "b1", Name: "b"})

	man := chunk.NewManifest(4)
	man.Append(chunk.Chunk{Index: 0, Hash: "aa", Data: []byte("data")})

	// First version
	if err := s.PutObject(ctx, &Object{BucketID: "b1", Key: "k", Manifest: man, SizeBytes: 4}); err != nil {
		t.Fatal(err)
	}
	// Second version of same key
	if err := s.PutObject(ctx, &Object{BucketID: "b1", Key: "k", Manifest: man, SizeBytes: 4, ContentType: "text/plain"}); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetObject(ctx, "b1", "k")
	if err != nil {
		t.Fatal(err)
	}
	if !got.IsLatest || got.ContentType != "text/plain" {
		t.Fatalf("latest object wrong: %+v", got)
	}
}

func TestChunkRefCountingAndDedup(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()

	isNew, err := s.IncrementChunkRef(ctx, ChunkRef{Hash: "h1", SizeBytes: 10, Nodes: []string{"n1"}})
	if err != nil || !isNew {
		t.Fatalf("first ref should be new: %v %v", isNew, err)
	}
	// Second reference to same chunk = dedup hit, not new.
	isNew, _ = s.IncrementChunkRef(ctx, ChunkRef{Hash: "h1"})
	if isNew {
		t.Fatal("second ref must report dedup (isNew=false)")
	}
	c, _ := s.GetChunk(ctx, "h1")
	if c.RefCount != 2 {
		t.Fatalf("ref count = %d, want 2", c.RefCount)
	}

	// Decrement twice -> becomes orphan.
	s.DecrementChunkRefs(ctx, []string{"h1"})
	s.DecrementChunkRefs(ctx, []string{"h1"})
	orphans, _ := s.ListOrphanChunks(ctx, 0)
	if len(orphans) != 1 || orphans[0].Hash != "h1" {
		t.Fatalf("expected h1 orphaned, got %v", orphans)
	}
}

func TestMultipartFlow(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()

	u := &MultipartUpload{BucketID: "b1", Key: "big.bin"}
	if err := s.CreateMultipartUpload(ctx, u); err != nil {
		t.Fatal(err)
	}
	if u.ID == "" {
		t.Fatal("upload should get an ID")
	}
	s.AddPart(ctx, u.ID, Part{PartNumber: 1, ETag: "e1", SizeBytes: 100})
	s.AddPart(ctx, u.ID, Part{PartNumber: 2, ETag: "e2", SizeBytes: 50})
	// Re-upload part 1 -> replace, not duplicate.
	s.AddPart(ctx, u.ID, Part{PartNumber: 1, ETag: "e1b", SizeBytes: 120})

	got, _ := s.GetMultipartUpload(ctx, u.ID)
	if len(got.Parts) != 2 {
		t.Fatalf("want 2 parts, got %d", len(got.Parts))
	}

	done, err := s.CompleteMultipartUpload(ctx, u.ID)
	if err != nil || done.Status != "completed" {
		t.Fatalf("complete: %+v %v", done, err)
	}
}
