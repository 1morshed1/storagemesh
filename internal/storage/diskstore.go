// Package storage implements a single storage node: the on-disk chunk store and
// the HTTP server/client used to move chunk bytes around the cluster.
//
// A storage node is intentionally dumb. It knows nothing about buckets, objects,
// manifests, or replication. Its whole job is: "given a content hash, store /
// return / delete / verify these bytes." All the smart placement and
// replication logic lives in the gateway. Keeping nodes dumb is what lets us run
// many of them and swap the disk backend without touching business logic.
package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ErrNotFound is returned when a chunk hash is not present on this node.
var ErrNotFound = errors.New("storage: chunk not found")

// ErrHashMismatch means the bytes did not hash to the claimed content hash.
// On a write it means the client lied or the transfer corrupted; on a verify it
// means bit rot / disk corruption of already-stored data.
var ErrHashMismatch = errors.New("storage: content hash mismatch")

// DiskStore stores chunks on the local filesystem, one file per chunk, named by
// content hash. To avoid piling millions of files into one directory (which
// wrecks filesystem performance), it shards by the first two hex characters of
// the hash into 256 subdirectories:
//
//	<root>/chunks/a3/a3f2b8c9...chunk
//	<root>/chunks/7d/7d1e4f9a...chunk
type DiskStore struct {
	root string
}

// NewDiskStore ensures <root>/chunks exists and returns a store rooted there.
func NewDiskStore(root string) (*DiskStore, error) {
	base := filepath.Join(root, "chunks")
	if err := os.MkdirAll(base, 0o755); err != nil {
		return nil, fmt.Errorf("storage: create root: %w", err)
	}
	return &DiskStore{root: root}, nil
}

// pathFor returns the sharded on-disk path for a hash. It rejects hashes that
// are not plausible hex (defence against path traversal like "../../etc").
func (d *DiskStore) pathFor(hash string) (string, error) {
	if len(hash) < 3 || !isHex(hash) {
		return "", fmt.Errorf("storage: invalid chunk hash %q", hash)
	}
	return filepath.Join(d.root, "chunks", hash[:2], hash+".chunk"), nil
}

func isHex(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// Put stores the chunk read from r under `hash`, verifying that the bytes
// actually hash to `hash`. The write is atomic: bytes go to a temp file that is
// fsync'd and renamed into place, so a crash mid-write never leaves a partial
// chunk visible under a valid name. If the chunk already exists, Put is a no-op
// (content addressing means identical hash == identical bytes).
//
// Returns the number of bytes stored.
func (d *DiskStore) Put(hash string, r io.Reader) (int64, error) {
	dst, err := d.pathFor(hash)
	if err != nil {
		return 0, err
	}
	if _, statErr := os.Stat(dst); statErr == nil {
		// Already have it. Content-addressed => bytes are identical. Drain the
		// reader so an HTTP body is fully consumed, then return.
		n, _ := io.Copy(io.Discard, r)
		return n, nil
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return 0, err
	}

	tmp, err := os.CreateTemp(filepath.Dir(dst), ".tmp-*")
	if err != nil {
		return 0, err
	}
	tmpName := tmp.Name()
	// Clean up the temp file on any failure path.
	defer func() { _ = os.Remove(tmpName) }()

	// Hash while we copy so we never read the bytes twice.
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), r)
	if err != nil {
		tmp.Close()
		return 0, err
	}
	if err := tmp.Sync(); err != nil { // durability: flush to disk before rename
		tmp.Close()
		return 0, err
	}
	if err := tmp.Close(); err != nil {
		return 0, err
	}

	got := hex.EncodeToString(h.Sum(nil))
	if !strings.EqualFold(got, hash) {
		return 0, fmt.Errorf("%w: got %s want %s", ErrHashMismatch, got, hash)
	}

	if err := os.Rename(tmpName, dst); err != nil {
		return 0, err
	}
	return n, nil
}

// Get opens the chunk for reading. The caller must Close the returned reader.
func (d *DiskStore) Get(hash string) (io.ReadCloser, error) {
	p, err := d.pathFor(hash)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	return f, err
}

// Delete removes a chunk. Deleting a missing chunk is not an error (idempotent),
// which matters because GC may retry deletes.
func (d *DiskStore) Delete(hash string) error {
	p, err := d.pathFor(hash)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// Exists reports whether the chunk is stored on this node.
func (d *DiskStore) Exists(hash string) bool {
	p, err := d.pathFor(hash)
	if err != nil {
		return false
	}
	_, err = os.Stat(p)
	return err == nil
}

// Verify re-reads the stored chunk, recomputes its SHA-256, and compares it to
// the expected hash. This is how the integrity checker detects bit rot. Returns
// ErrNotFound if absent, ErrHashMismatch if corrupt, nil if healthy.
func (d *DiskStore) Verify(hash string) error {
	rc, err := d.Get(hash)
	if err != nil {
		return err
	}
	defer rc.Close()

	h := sha256.New()
	if _, err := io.Copy(h, rc); err != nil {
		return err
	}
	if !strings.EqualFold(hex.EncodeToString(h.Sum(nil)), hash) {
		return ErrHashMismatch
	}
	return nil
}

// List returns every chunk hash stored on this node. Used by the integrity
// checker and rebalancer. For a demo scale node this in-memory slice is
// fine; a production node would paginate.
func (d *DiskStore) List() ([]string, error) {
	base := filepath.Join(d.root, "chunks")
	var out []string
	err := filepath.WalkDir(base, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".chunk") {
			return nil
		}
		out = append(out, strings.TrimSuffix(entry.Name(), ".chunk"))
		return nil
	})
	return out, err
}
