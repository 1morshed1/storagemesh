package gateway

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/1morshed1/storagemesh/internal/storage"
)

// quorum returns the number of acknowledgements a write needs. With replication
// factor N, W = floor(N/2)+1 (a strict majority): 2 of 3, 3 of 5. A majority
// write quorum combined with a majority read quorum guarantees any read overlaps
// any successful write on at least one node, so a reader can never miss the
// latest data. Here reads are simpler (chunks are immutable, addressed by
// content hash), but we keep the majority write rule for durability: a chunk is
// "stored" only once a majority of its replicas hold it, so losing any single
// node cannot lose the chunk.
func quorum(replication int) int {
	return replication/2 + 1
}

// replicateChunk pushes chunk bytes to all target nodes in parallel and waits
// for a write quorum of acknowledgements. It returns the node IDs that
// successfully stored the chunk.
//
// Why parallel + quorum instead of "write to all, fail if any fails": a single
// slow or dead node must not fail or stall the upload. We fan out to all R
// replicas at once and succeed as soon as W of them ack. The stragglers keep
// going in the background best-effort; if they fail, the rebalancer/integrity
// checker will restore the missing replica later. This is how the system stays
// available for writes even with a node down.
func replicateChunk(ctx context.Context, hash string, data []byte, targets []replicaTarget, replication int) ([]string, error) {
	if len(targets) == 0 {
		return nil, fmt.Errorf("replicate %s: no storage nodes available", short(hash))
	}
	need := quorum(replication)
	if need > len(targets) {
		// Fewer live replicas than a full quorum wants; require all we have.
		need = len(targets)
	}

	type ack struct {
		nodeID string
		err    error
	}
	acks := make(chan ack, len(targets))
	for _, t := range targets {
		go func(t replicaTarget) {
			// Each replica gets its own reader over the shared byte slice.
			// bytes.NewReader is read-only, so sharing the underlying array
			// across goroutines is safe.
			err := t.client.PutChunk(ctx, hash, bytes.NewReader(data))
			acks <- ack{nodeID: t.nodeID, err: err}
		}(t)
	}

	stored := make([]string, 0, len(targets))
	var firstErr error
	for i := 0; i < len(targets); i++ {
		a := <-acks
		if a.err != nil {
			if firstErr == nil {
				firstErr = a.err
			}
			continue
		}
		stored = append(stored, a.nodeID)
		if len(stored) >= need {
			// Quorum reached — return success immediately. The remaining sender
			// goroutines are NOT leaked: `acks` is buffered to len(targets), so
			// their sends complete without a receiver, and they store their
			// replica best-effort in the background.
			return stored, nil
		}
	}

	if len(stored) < need {
		return stored, fmt.Errorf("replicate %s: only %d/%d replicas acked (quorum %d): %w",
			short(hash), len(stored), len(targets), need, firstErr)
	}
	return stored, nil
}

// fetchChunk streams a chunk back, trying replicas in ring order until one
// serves it. This is the read-side failover: if the primary is down or has lost
// the chunk (404), we transparently fall back to the next replica. Returns the
// first replica's reader that succeeds; caller must Close it.
func fetchChunk(ctx context.Context, hash string, targets []replicaTarget) (io.ReadCloser, error) {
	var lastErr error
	for _, t := range targets {
		rc, err := t.client.GetChunk(ctx, hash)
		if err == nil {
			return rc, nil
		}
		lastErr = err
		if err != storage.ErrNotFound {
			// A transport error on one node: try the next replica.
			continue
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no replicas for chunk %s", short(hash))
	}
	return nil, fmt.Errorf("fetch chunk %s: all replicas failed: %w", short(hash), lastErr)
}

func short(hash string) string {
	if len(hash) > 8 {
		return hash[:8]
	}
	return hash
}
