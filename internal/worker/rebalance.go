package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/1morshed1/storagemesh/internal/storage"
)

// Rebalancer ensures every chunk physically lives on the nodes the hash ring
// currently assigns to it. It handles two situations with one mechanism:
//
//   - Node joins: the ring reassigns some chunks to the new node; the rebalancer
//     copies those chunks onto it.
//   - Under-replication: a node was down during a write (quorum let the write
//     succeed with fewer than R copies), so some assigned replicas are missing;
//     the rebalancer fills them from a node that has the bytes.
//
// It copies missing replicas but does NOT delete now-misplaced copies from nodes
// the ring no longer assigns — that cleanup is deferred (a stale extra copy is a
// space cost, never a correctness bug). See the limitation note in doc 06.
type Rebalancer struct {
	view ClusterView
	log  *slog.Logger
}

func NewRebalancer(view ClusterView, log *slog.Logger) *Rebalancer {
	if log == nil {
		log = slog.Default()
	}
	return &Rebalancer{view: view, log: log}
}

func (rb *Rebalancer) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			moved, err := rb.RunOnce(ctx)
			if err != nil {
				rb.log.Error("rebalance pass failed", "err", err)
			} else if moved > 0 {
				rb.log.Info("rebalance pass complete", "copies", moved)
			}
		}
	}
}

// RunOnce makes one rebalancing pass and returns how many replica copies it made.
func (rb *Rebalancer) RunOnce(ctx context.Context) (int, error) {
	clients := rb.view.AllClients()

	// Discover every chunk hash known anywhere in the cluster, and remember one
	// node that currently holds each (a copy source).
	holder := make(map[string]*storage.Client)
	for _, cl := range clients {
		hashes, err := cl.ListChunks(ctx)
		if err != nil {
			rb.log.Warn("rebalance: list chunks failed", "err", err)
			continue
		}
		for _, h := range hashes {
			if _, seen := holder[h]; !seen {
				holder[h] = cl
			}
		}
	}

	moved := 0
	for hash, src := range holder {
		// Where SHOULD this chunk live now?
		for _, target := range rb.view.ClientsFor(hash) {
			has, err := target.HasChunk(ctx, hash)
			if err != nil {
				rb.log.Warn("rebalance: HasChunk failed", "hash", short(hash), "err", err)
				continue
			}
			if has {
				continue // already placed correctly
			}
			// Missing on an assigned node: copy it there.
			if err := copyChunk(ctx, hash, src, target); err != nil {
				rb.log.Warn("rebalance: copy failed", "hash", short(hash), "err", err)
				continue
			}
			moved++
		}
	}
	return moved, nil
}
