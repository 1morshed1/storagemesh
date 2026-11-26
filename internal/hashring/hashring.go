// Package hashring implements consistent hashing with virtual nodes to decide
// which storage nodes hold a given chunk.
//
// # THE PROBLEM IT SOLVES
//
// Say we have N storage nodes and want to place a chunk. The naive answer is
// node = hash(chunk) % N. It works until N changes. Add or remove one node and
// the modulus changes for *almost every* key, so nearly all data must move.
// That is catastrophic for a storage system.
//
// Consistent hashing fixes this. We map both nodes and chunks onto the same
// circular keyspace [0, 2^32). A chunk is owned by the first node found walking
// clockwise from the chunk's position. When a node is added, it only steals the
// arc between itself and its clockwise-previous node — on average 1/N of the
// keys move, and only from its neighbours. Removing a node hands its arc to the
// next node clockwise. Everything else stays put.
//
// # WHY VIRTUAL NODES
//
// With one point per physical node, the arcs are wildly uneven — one node might
// own 40% of the ring by luck, creating hotspots. We instead place many points
// ("virtual nodes" / vnodes) per physical node, e.g. 150. The law of large
// numbers then smooths each physical node's total arc toward a fair 1/N share,
// and when a node leaves, its load is spread across many neighbours instead of
// dumped entirely on one.
//
// # REPLICATION
//
// GetNodes walks clockwise past the first owner to collect R *distinct physical*
// nodes. Those are the R replicas for the chunk. Skipping vnodes that map back
// to an already-chosen physical node is what makes replicas land on different
// machines (otherwise all three "replicas" could be the same box).
package hashring

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// DefaultVNodes is the number of virtual nodes per physical node. 150 gives a
// good balance: enough points for even distribution, not so many that ring
// rebuilds get expensive.
const DefaultVNodes = 150

// HashRing maps chunk hashes to an ordered set of storage-node IDs.
//
// It is safe for concurrent use. Reads (GetNodes) take a read lock; membership
// changes (Add/Remove) take a write lock and rebuild the sorted ring.
type HashRing struct {
	mu          sync.RWMutex
	nodes       map[string]struct{} // set of physical node IDs
	sorted      []uint32            // sorted vnode hash positions
	positions   map[uint32]string   // vnode position -> physical node ID
	vnodes      int
	replication int
}

// New creates a ring. vnodes<=0 uses DefaultVNodes. replication<1 is clamped to
// 1 (you always need at least one copy).
func New(vnodes, replication int) *HashRing {
	if vnodes <= 0 {
		vnodes = DefaultVNodes
	}
	if replication < 1 {
		replication = 1
	}
	return &HashRing{
		nodes:       make(map[string]struct{}),
		positions:   make(map[uint32]string),
		vnodes:      vnodes,
		replication: replication,
	}
}

// hashKey maps an arbitrary string to a position on the ring. CRC32 is used
// (not SHA-256) because ring placement does not need cryptographic strength —
// only a fast, well-distributed 32-bit value. The chunk's *identity* is already
// a SHA-256; here we just need to scatter it around the circle.
func hashKey(s string) uint32 {
	return crc32.ChecksumIEEE([]byte(s))
}

// vnodeKey is the string hashed to place one virtual node. Including the vnode
// index means a node's vnodes scatter independently around the ring.
func vnodeKey(nodeID string, i int) string {
	return nodeID + "#" + strconv.Itoa(i)
}

// Add inserts a physical node (idempotent) and rebuilds the ring.
func (r *HashRing) Add(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[nodeID]; ok {
		return
	}
	r.nodes[nodeID] = struct{}{}
	r.rebuild()
}

// Remove deletes a physical node (idempotent) and rebuilds the ring.
func (r *HashRing) Remove(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[nodeID]; !ok {
		return
	}
	delete(r.nodes, nodeID)
	r.rebuild()
}

// SetNodes replaces the entire membership set in one rebuild. Handy when a
// gossip/health layer hands us a fresh live-node list.
func (r *HashRing) SetNodes(nodeIDs []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodes = make(map[string]struct{}, len(nodeIDs))
	for _, id := range nodeIDs {
		r.nodes[id] = struct{}{}
	}
	r.rebuild()
}

// rebuild regenerates positions and the sorted slice from the node set. Called
// under the write lock. O(nodes * vnodes) plus a sort; cheap for realistic
// cluster sizes and only runs on membership change, not per request.
func (r *HashRing) rebuild() {
	r.positions = make(map[uint32]string, len(r.nodes)*r.vnodes)
	r.sorted = r.sorted[:0]
	for nodeID := range r.nodes {
		for i := 0; i < r.vnodes; i++ {
			pos := hashKey(vnodeKey(nodeID, i))
			// On the astronomically rare CRC32 collision between two vnodes,
			// last writer wins; the distribution impact is negligible.
			r.positions[pos] = nodeID
			r.sorted = append(r.sorted, pos)
		}
	}
	sort.Slice(r.sorted, func(i, j int) bool { return r.sorted[i] < r.sorted[j] })
}

// GetNodes returns up to `replication` distinct physical node IDs responsible
// for chunkHash, ordered by ring proximity (primary first). If the cluster has
// fewer than `replication` nodes, it returns all of them — you cannot make 3
// copies on 2 machines.
func (r *HashRing) GetNodes(chunkHash string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.sorted) == 0 {
		return nil
	}

	h := hashKey(chunkHash)
	// Binary search for the first vnode position >= h (clockwise owner).
	idx := sort.Search(len(r.sorted), func(i int) bool { return r.sorted[i] >= h })
	if idx == len(r.sorted) {
		idx = 0 // wrap around the top of the circle
	}

	want := r.replication
	if want > len(r.nodes) {
		want = len(r.nodes)
	}

	out := make([]string, 0, want)
	seen := make(map[string]struct{}, want)
	// Walk clockwise collecting distinct physical nodes. At most len(sorted)
	// steps, which terminates even if the cluster is tiny.
	for i := 0; i < len(r.sorted) && len(out) < want; i++ {
		pos := r.sorted[(idx+i)%len(r.sorted)]
		nodeID := r.positions[pos]
		if _, dup := seen[nodeID]; dup {
			continue
		}
		seen[nodeID] = struct{}{}
		out = append(out, nodeID)
	}
	return out
}

// Nodes returns the current physical node IDs (unordered). For diagnostics.
func (r *HashRing) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.nodes))
	for id := range r.nodes {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
