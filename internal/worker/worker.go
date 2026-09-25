// Package worker holds the background maintenance jobs that keep StorageMesh
// healthy over time. None of them are on the request path; they run on timers.
//
//   - GarbageCollector — deletes chunk bytes whose ref_count has dropped to zero
//     (no object references them any more) after a grace period.
//   - IntegrityChecker — periodically re-hashes stored chunks and repairs any
//     that are corrupt (bit rot) from a healthy replica.
//   - Rebalancer — ensures every chunk has a full set of replicas on the nodes
//     the hash ring currently assigns; copies missing replicas. This both
//     restores under-replication after a node failure and moves data when a new
//     node joins the ring.
//
// All three depend only on the metadata.Store interface and a small ClusterView
// interface, so they are testable with a MemStore and in-process nodes.
package worker

import (
	"context"

	"github.com/1morshed1/storagemesh/internal/storage"
)

// ClusterView is the slice of the gateway's Cluster the workers need. Defining
// it here (consumer-side interface) keeps the worker package decoupled from the
// gateway — *gateway.Cluster satisfies it without importing worker.
type ClusterView interface {
	// ClientsFor returns the storage clients for a chunk's assigned replica
	// nodes, in ring order.
	ClientsFor(hash string) []*storage.Client
	// AllClients returns a client for every configured node, keyed by node ID.
	AllClients() map[string]*storage.Client
}

// copyChunk streams a chunk from src to dst. Used by both the integrity checker
// (repair a corrupt replica) and the rebalancer (fill a missing replica).
func copyChunk(ctx context.Context, hash string, src, dst *storage.Client) error {
	rc, err := src.GetChunk(ctx, hash)
	if err != nil {
		return err
	}
	defer rc.Close()
	// dst re-verifies the hash on write, so a corrupt source can't poison dst.
	return dst.PutChunk(ctx, hash, rc)
}
