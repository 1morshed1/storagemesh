package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/1morshed1/storagemesh/internal/metadata"
)

// GarbageCollector deletes orphaned chunks — those with ref_count <= 0 that have
// stayed unreferenced past a grace period.
//
// WHY THE GRACE PERIOD: an upload increments a chunk's ref_count, but there is a
// window between "chunk bytes stored / row created" and "object committed". A
// delete of one object can momentarily drive a shared chunk's count to zero even
// while another upload is about to reference it. Waiting (default 24h) before
// physically deleting avoids racing those in-flight references and makes GC safe
// to run concurrently with writes.
//
// WHY IT IS SAFE AGAINST RE-REFERENCE: DeleteChunkRef only removes the row if
// ref_count is still <= 0, so if an upload re-referenced the chunk between the
// scan and the delete, the row (and bytes) are spared.
type GarbageCollector struct {
	store metadata.Store
	view  ClusterView
	grace time.Duration
	log   *slog.Logger
}

// NewGarbageCollector builds a GC. grace is how long a chunk must sit orphaned
// before deletion (production: 24h; tests pass 0).
func NewGarbageCollector(store metadata.Store, view ClusterView, grace time.Duration, log *slog.Logger) *GarbageCollector {
	if log == nil {
		log = slog.Default()
	}
	return &GarbageCollector{store: store, view: view, grace: grace, log: log}
}

// Run executes GC on `every` interval until ctx is cancelled.
func (g *GarbageCollector) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := g.RunOnce(ctx); err != nil {
				g.log.Error("gc pass failed", "err", err)
			} else if n > 0 {
				g.log.Info("gc pass complete", "collected", n)
			}
		}
	}
}

// RunOnce performs a single GC sweep and returns how many chunks it collected.
// Exported so tests (and an ops "gc now" endpoint) can trigger it directly.
func (g *GarbageCollector) RunOnce(ctx context.Context) (int, error) {
	orphans, err := g.store.ListOrphanChunks(ctx, g.grace)
	if err != nil {
		return 0, err
	}
	clients := g.view.AllClients()
	collected := 0
	for _, c := range orphans {
		// 1. Delete the bytes from every node that holds them. We use the node
		//    list recorded on the chunk row. Deletes are idempotent, so a node
		//    that already lost the chunk is fine.
		for _, nodeID := range c.Nodes {
			cl, ok := clients[nodeID]
			if !ok {
				continue // node no longer configured
			}
			if err := cl.DeleteChunk(ctx, c.Hash); err != nil {
				// Log and continue: leaving bytes on one node is a leak, not a
				// correctness bug. Next pass retries.
				g.log.Warn("gc delete chunk on node failed", "hash", short(c.Hash), "node", nodeID, "err", err)
			}
		}
		// 2. Remove the metadata row — but only if still orphaned (guards against
		//    a concurrent upload re-referencing it).
		if err := g.store.DeleteChunkRef(ctx, c.Hash); err != nil {
			g.log.Warn("gc delete chunk ref failed", "hash", short(c.Hash), "err", err)
			continue
		}
		collected++
	}
	return collected, nil
}

func short(h string) string {
	if len(h) > 8 {
		return h[:8]
	}
	return h
}
