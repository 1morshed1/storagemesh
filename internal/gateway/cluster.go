package gateway

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/1morshed1/storagemesh/internal/hashring"
	"github.com/1morshed1/storagemesh/internal/storage"
)

// Node describes one storage node the gateway can talk to.
type Node struct {
	ID      string
	BaseURL string
}

// Cluster is the gateway's view of the storage ring. It owns:
//   - the consistent hash ring (which nodes should hold a chunk),
//   - a pooled storage.Client per node,
//   - a background health loop that keeps the ring's membership in sync with
//     which nodes are actually reachable.
//
// Keeping the ring in sync with liveness is what lets writes route around a
// dead node: if node-2 stops answering /healthz, the health loop removes it from
// the ring, so GetReplicas stops selecting it. When it recovers, it is added
// back and the rebalancer (Phase 4) re-replicates any chunks it missed.
type Cluster struct {
	ring    *hashring.HashRing
	repl    int
	mu      sync.RWMutex
	clients map[string]*storage.Client // nodeID -> client
	all     map[string]Node            // every configured node, live or not
	log     *slog.Logger
}

// NewCluster builds a cluster from a static node list with replication factor
// repl. All nodes start assumed-live; the health loop corrects that.
func NewCluster(nodes []Node, repl int, log *slog.Logger) *Cluster {
	if log == nil {
		log = slog.Default()
	}
	c := &Cluster{
		ring:    hashring.New(hashring.DefaultVNodes, repl),
		repl:    repl,
		clients: make(map[string]*storage.Client),
		all:     make(map[string]Node),
		log:     log,
	}
	live := make([]string, 0, len(nodes))
	for _, n := range nodes {
		c.all[n.ID] = n
		c.clients[n.ID] = storage.NewClient(n.BaseURL)
		live = append(live, n.ID)
	}
	c.ring.SetNodes(live)
	return c
}

// ReplicationFactor returns the configured number of copies per chunk.
func (c *Cluster) ReplicationFactor() int { return c.repl }

// clientFor returns the storage client for a node ID (nil if unknown).
func (c *Cluster) clientFor(nodeID string) *storage.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.clients[nodeID]
}

// replicaTarget pairs a node ID with its client for a chunk placement.
type replicaTarget struct {
	nodeID string
	client *storage.Client
}

// GetReplicas returns the ordered replica targets for a chunk hash: the R nodes
// the ring assigns, each with its client. Primary first.
func (c *Cluster) GetReplicas(hash string) []replicaTarget {
	nodeIDs := c.ring.GetNodes(hash)
	out := make([]replicaTarget, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		if cl := c.clientFor(id); cl != nil {
			out = append(out, replicaTarget{nodeID: id, client: cl})
		}
	}
	return out
}

// ClientsFor returns the storage clients for a chunk's current replica nodes, in
// ring order. Exported for the background workers (GC / integrity / rebalancer),
// which reason about a chunk's replica set without touching the unexported
// replicaTarget type.
func (c *Cluster) ClientsFor(hash string) []*storage.Client {
	targets := c.GetReplicas(hash)
	out := make([]*storage.Client, 0, len(targets))
	for _, t := range targets {
		out = append(out, t.client)
	}
	return out
}

// StartHealthChecks launches the background liveness loop. It probes every
// configured node every `interval` and updates ring membership. It stops when
// ctx is cancelled.
func (c *Cluster) StartHealthChecks(ctx context.Context, interval time.Duration) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		c.probeOnce(ctx) // probe immediately so startup reflects reality fast
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				c.probeOnce(ctx)
			}
		}
	}()
}

// probeOnce checks every configured node and rebuilds ring membership from the
// set that answered. Probes run concurrently so one slow node doesn't stall the
// whole sweep.
func (c *Cluster) probeOnce(ctx context.Context) {
	c.mu.RLock()
	nodes := make([]Node, 0, len(c.all))
	for _, n := range c.all {
		nodes = append(nodes, n)
	}
	c.mu.RUnlock()

	type result struct {
		id   string
		live bool
	}
	results := make(chan result, len(nodes))
	for _, n := range nodes {
		go func(n Node) {
			pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			err := c.clientFor(n.ID).Health(pctx)
			results <- result{id: n.ID, live: err == nil}
		}(n)
	}

	live := make([]string, 0, len(nodes))
	for i := 0; i < len(nodes); i++ {
		r := <-results
		if r.live {
			live = append(live, r.id)
		} else {
			c.log.Warn("storage node unhealthy, removed from ring", "node", r.id)
		}
	}
	c.ring.SetNodes(live)
}

// LiveNodes returns the node IDs currently in the ring (diagnostics).
func (c *Cluster) LiveNodes() []string { return c.ring.Nodes() }

// AllClients returns a client for every configured node. Workers (GC, integrity)
// need to reach every node, not just ring members.
func (c *Cluster) AllClients() map[string]*storage.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]*storage.Client, len(c.clients))
	for id, cl := range c.clients {
		out[id] = cl
	}
	return out
}
