package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http/httptest"
	"os"
	"testing"
)

func hashOf(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func TestDiskStorePutGetVerify(t *testing.T) {
	dir := t.TempDir()
	ds, err := NewDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("hello content-addressed world")
	h := hashOf(data)

	if _, err := ds.Put(h, bytes.NewReader(data)); err != nil {
		t.Fatalf("put: %v", err)
	}
	if !ds.Exists(h) {
		t.Fatal("chunk should exist after put")
	}
	if err := ds.Verify(h); err != nil {
		t.Fatalf("verify healthy chunk: %v", err)
	}

	rc, err := ds.Get(h)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, data) {
		t.Fatal("read bytes differ from written")
	}
}

func TestDiskStoreRejectsWrongHash(t *testing.T) {
	ds, _ := NewDiskStore(t.TempDir())
	// Claim a hash that does not match the bytes.
	err := func() error { _, e := ds.Put(hashOf([]byte("A")), bytes.NewReader([]byte("B"))); return e }()
	if err == nil {
		t.Fatal("expected hash mismatch error, got nil")
	}
}

func TestDiskStoreDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	ds, _ := NewDiskStore(dir)
	data := []byte("original bytes")
	h := hashOf(data)
	if _, err := ds.Put(h, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	// Simulate bit rot by overwriting the stored file on disk.
	p, _ := ds.pathFor(h)
	if err := os.WriteFile(p, []byte("corrupted!!!!!"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := ds.Verify(h); err != ErrHashMismatch {
		t.Fatalf("verify should report corruption, got %v", err)
	}
}

func TestDiskStoreDedupAndDelete(t *testing.T) {
	ds, _ := NewDiskStore(t.TempDir())
	data := []byte("dup me")
	h := hashOf(data)
	if _, err := ds.Put(h, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	// Second put with same hash is a no-op but must still consume the reader.
	if _, err := ds.Put(h, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if err := ds.Delete(h); err != nil {
		t.Fatal(err)
	}
	if ds.Exists(h) {
		t.Fatal("chunk should be gone after delete")
	}
	// Deleting again is not an error.
	if err := ds.Delete(h); err != nil {
		t.Fatalf("second delete should be idempotent: %v", err)
	}
}

func TestDiskStoreRejectsPathTraversal(t *testing.T) {
	ds, _ := NewDiskStore(t.TempDir())
	if _, err := ds.Get("../../etc/passwd"); err == nil {
		t.Fatal("expected invalid-hash error for traversal attempt")
	}
}

// TestServerClientRoundTrip spins up the real HTTP server against an httptest
// server and drives it through the Client — the exact path the gateway uses.
func TestServerClientRoundTrip(t *testing.T) {
	ds, _ := NewDiskStore(t.TempDir())
	srv := httptest.NewServer(NewServer(ds, "test-node", nil).Handler())
	defer srv.Close()

	client := NewClient(srv.URL)
	ctx := context.Background()

	data := []byte("round trip through http")
	h := hashOf(data)

	if err := client.PutChunk(ctx, h, bytes.NewReader(data)); err != nil {
		t.Fatalf("PutChunk: %v", err)
	}

	ok, err := client.HasChunk(ctx, h)
	if err != nil || !ok {
		t.Fatalf("HasChunk = %v, %v", ok, err)
	}

	rc, err := client.GetChunk(ctx, h)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, data) {
		t.Fatal("round-tripped bytes differ")
	}

	healthy, err := client.VerifyChunk(ctx, h)
	if err != nil || !healthy {
		t.Fatalf("VerifyChunk = %v, %v", healthy, err)
	}

	hashes, err := client.ListChunks(ctx)
	if err != nil || len(hashes) != 1 || hashes[0] != h {
		t.Fatalf("ListChunks = %v, %v", hashes, err)
	}

	if err := client.DeleteChunk(ctx, h); err != nil {
		t.Fatalf("DeleteChunk: %v", err)
	}
	if _, err := client.GetChunk(ctx, h); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestClientGetMissingIsErrNotFound(t *testing.T) {
	ds, _ := NewDiskStore(t.TempDir())
	srv := httptest.NewServer(NewServer(ds, "n", nil).Handler())
	defer srv.Close()
	if _, err := NewClient(srv.URL).GetChunk(context.Background(), hashOf([]byte("nope"))); err != ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}
