package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/1morshed1/storagemesh/internal/metadata"
	"github.com/1morshed1/storagemesh/internal/storage"
)

// IntegrityChecker defends against silent data corruption ("bit rot"): a stored
// chunk's bytes flip on disk over time, so they no longer match their content
// hash. It asks each node to re-hash its chunks; when a node reports a chunk
// corrupt, it repairs that replica by copying a healthy copy from another node.
//
// This is the answer to *"what if a chunk is corrupted?"* — periodic SHA-256
// verification detects it, and a healthy replica heals it.
type IntegrityChecker struct {
	store metadata.Store
	view  ClusterView
	log   *slog.Logger
}

func NewIntegrityChecker(store metadata.Store, view ClusterView, log *slog.Logger) *IntegrityChecker {
	if log == nil {
		log = slog.Default()
	}
	return &IntegrityChecker{store: store, view: view, log: log}
}

func (c *IntegrityChecker) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			checked, repaired, err := c.RunOnce(ctx)
			if err != nil {
				c.log.Error("integrity pass failed", "err", err)
			} else {
				c.log.Info("integrity pass complete", "checked", checked, "repaired", repaired)
			}
		}
	}
}

// RunOnce verifies every chunk on every node once. Returns (checked, repaired).
func (c *IntegrityChecker) RunOnce(ctx context.Context) (checked, repaired int, err error) {
	clients := c.view.AllClients()
	for nodeID, cl := range clients {
		hashes, lerr := cl.ListChunks(ctx)
		if lerr != nil {
			c.log.Warn("integrity: list chunks failed", "node", nodeID, "err", lerr)
			continue
		}
		for _, h := range hashes {
			checked++
			healthy, verr := cl.VerifyChunk(ctx, h)
			if verr != nil {
				c.log.Warn("integrity: verify failed", "node", nodeID, "hash", short(h), "err", verr)
				continue
			}
			if healthy {
				_ = c.store.MarkChunkVerified(ctx, h, time.Now().UTC())
				continue
			}
			// Corrupt on this node: repair from a healthy replica.
			c.log.Warn("integrity: corrupt chunk detected", "node", nodeID, "hash", short(h))
			if c.repair(ctx, h, cl) {
				repaired++
			}
		}
	}
	return checked, repaired, nil
}

// repair finds a replica that verifies healthy and copies its bytes over the
// corrupt node. Returns true on success.
func (c *IntegrityChecker) repair(ctx context.Context, hash string, corrupt *storage.Client) bool {
	for _, src := range c.view.ClientsFor(hash) {
		if src == corrupt {
			continue
		}
		ok, err := src.VerifyChunk(ctx, hash)
		if err != nil || !ok {
			continue // this replica is missing or also corrupt; try the next
		}
		// Delete the corrupt copy first: a storage node treats an existing file
		// as already-correct (content addressing assumes hash==bytes) and would
		// skip the overwrite. Removing it forces a fresh, verified write.
		if err := corrupt.DeleteChunk(ctx, hash); err != nil {
			c.log.Warn("integrity: could not remove corrupt copy", "hash", short(hash), "err", err)
			continue
		}
		// The destination re-verifies on write, so it cannot store bad bytes.
		if err := copyChunk(ctx, hash, src, corrupt); err != nil {
			c.log.Warn("integrity: repair copy failed", "hash", short(hash), "err", err)
			continue
		}
		c.log.Info("integrity: repaired chunk from healthy replica", "hash", short(hash))
		return true
	}
	c.log.Error("integrity: NO healthy replica to repair from", "hash", short(hash))
	return false
}
