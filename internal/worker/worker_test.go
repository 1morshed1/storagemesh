package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/1morshed1/storagemesh/internal/gateway"
	"github.com/1morshed1/storagemesh/internal/metadata"
	"github.com/1morshed1/storagemesh/internal/storage"
)

// node bundles a storage node's disk store, HTTP server, and ID for tests that
// need to manipulate the disk directly (e.g. simulate corruption).
type node struct {
	id  string
	dir string
	ds  *storage.DiskStore
	srv *httptest.Server
}

func newNodes(t *testing.T, n int) ([]node, *gateway.Cluster, int) {
	t.Helper()
	var nodes []node
	var cnodes []gateway.Node
	for i := 0; i < n; i++ {
		dir := t.TempDir()
		ds, err := storage.NewDiskStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewServer(storage.NewServer(ds, "n", nil).Handler())
		nodes = append(nodes, node{id: srv.URL, dir: dir, ds: ds, srv: srv})
		cnodes = append(cnodes, gateway.Node{ID: srv.URL, BaseURL: srv.URL})
	}
	repl := n
	return nodes, gateway.NewCluster(cnodes, repl, nil), repl
}

func closeNodes(nodes []node) {
	for _, n := range nodes {
		n.srv.Close()
	}
}

func hashOf(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func TestGarbageCollectorDeletesOrphans(t *testing.T) {
	nodes, cluster, _ := newNodes(t, 2)
	defer closeNodes(nodes)
	ctx := context.Background()

	data := []byte("orphan me")
	h := hashOf(data)
	// Place bytes on both nodes.
	for _, n := range nodes {
		if _, err := n.ds.Put(h, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
	}
	// Record the chunk as orphaned (ref_count 0) with its node locations.
	store := metadata.NewMemStore()
	store.IncrementChunkRef(ctx, metadata.ChunkRef{Hash: h, SizeBytes: len(data), Nodes: []string{nodes[0].id, nodes[1].id}})
	store.DecrementChunkRefs(ctx, []string{h}) // -> ref_count 0

	gc := NewGarbageCollector(store, cluster, 0, nil) // 0 grace: collect immediately
	collected, err := gc.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if collected != 1 {
		t.Fatalf("collected = %d, want 1", collected)
	}
	// Bytes gone from both nodes, metadata row gone.
	for _, n := range nodes {
		if n.ds.Exists(h) {
			t.Fatalf("chunk still on node %s after GC", n.id)
		}
	}
	if _, err := store.GetChunk(ctx, h); err == nil {
		t.Fatal("chunk ref should be deleted")
	}
}

func TestGarbageCollectorSparesReferenced(t *testing.T) {
	nodes, cluster, _ := newNodes(t, 1)
	defer closeNodes(nodes)
	ctx := context.Background()

	data := []byte("keep me")
	h := hashOf(data)
	nodes[0].ds.Put(h, bytes.NewReader(data))
	store := metadata.NewMemStore()
	store.IncrementChunkRef(ctx, metadata.ChunkRef{Hash: h, Nodes: []string{nodes[0].id}}) // ref_count 1

	gc := NewGarbageCollector(store, cluster, 0, nil)
	collected, _ := gc.RunOnce(ctx)
	if collected != 0 {
		t.Fatalf("collected = %d, want 0 (chunk is referenced)", collected)
	}
	if !nodes[0].ds.Exists(h) {
		t.Fatal("referenced chunk must not be deleted")
	}
}

func TestIntegrityCheckerRepairsCorruption(t *testing.T) {
	nodes, cluster, _ := newNodes(t, 3)
	defer closeNodes(nodes)
	ctx := context.Background()

	data := []byte("verify and heal me")
	h := hashOf(data)
	for _, n := range nodes {
		n.ds.Put(h, bytes.NewReader(data))
	}

	// Corrupt the copy on node 0 by overwriting the file on disk.
	corruptPath := filepath.Join(nodes[0].dir, "chunks", h[:2], h+".chunk")
	if err := os.WriteFile(corruptPath, []byte("bit rot!!!!!!!!!!"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Sanity: node 0 now reports corruption.
	if err := nodes[0].ds.Verify(h); err != storage.ErrHashMismatch {
		t.Fatalf("expected corruption on node0, got %v", err)
	}

	ic := NewIntegrityChecker(metadata.NewMemStore(), cluster, nil)
	_, repaired, err := ic.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if repaired < 1 {
		t.Fatalf("repaired = %d, want >= 1", repaired)
	}
	// Node 0's copy is healthy again.
	if err := nodes[0].ds.Verify(h); err != nil {
		t.Fatalf("node0 chunk still corrupt after repair: %v", err)
	}
}

func TestRebalancerFillsMissingReplicas(t *testing.T) {
	nodes, cluster, _ := newNodes(t, 3)
	defer closeNodes(nodes)
	ctx := context.Background()

	data := []byte("replicate me everywhere")
	h := hashOf(data)
	// Place the chunk on ONLY node 0 (simulating a write that happened while the
	// others were down). Replication factor is 3, so it is under-replicated.
	nodes[0].ds.Put(h, bytes.NewReader(data))

	rb := NewRebalancer(cluster, nil)
	moved, err := rb.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if moved != 2 {
		t.Fatalf("copies made = %d, want 2 (fill nodes 1 and 2)", moved)
	}
	for _, n := range nodes {
		if !n.ds.Exists(h) {
			t.Fatalf("chunk missing on node %s after rebalance", n.id)
		}
	}
}
