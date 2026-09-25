package hashring

import (
	"fmt"
	"math"
	"testing"
)

func TestReplicationReturnsDistinctNodes(t *testing.T) {
	r := New(50, 3)
	for _, n := range []string{"node-1", "node-2", "node-3", "node-4"} {
		r.Add(n)
	}
	nodes := r.GetNodes("some-chunk-hash")
	if len(nodes) != 3 {
		t.Fatalf("got %d replicas, want 3", len(nodes))
	}
	seen := map[string]bool{}
	for _, n := range nodes {
		if seen[n] {
			t.Fatalf("duplicate replica node %q in %v", n, nodes)
		}
		seen[n] = true
	}
}

func TestFewerNodesThanReplication(t *testing.T) {
	r := New(50, 3)
	r.Add("only-node")
	nodes := r.GetNodes("x")
	if len(nodes) != 1 || nodes[0] != "only-node" {
		t.Fatalf("want single node, got %v", nodes)
	}
}

func TestEmptyRing(t *testing.T) {
	r := New(50, 3)
	if got := r.GetNodes("x"); got != nil {
		t.Fatalf("empty ring should return nil, got %v", got)
	}
}

func TestDeterministicPlacement(t *testing.T) {
	build := func() *HashRing {
		r := New(100, 3)
		for i := 0; i < 5; i++ {
			r.Add(fmt.Sprintf("node-%d", i))
		}
		return r
	}
	a, b := build(), build()
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("chunk-%d", i)
		na, nb := a.GetNodes(key), b.GetNodes(key)
		if fmt.Sprint(na) != fmt.Sprint(nb) {
			t.Fatalf("placement not deterministic for %s: %v vs %v", key, na, nb)
		}
	}
}

// TestMinimalReshuffleOnAdd is the whole point of consistent hashing: adding a
// node should move only a small fraction of keys, not remap everything.
func TestMinimalReshuffleOnAdd(t *testing.T) {
	r := New(150, 1) // replication 1 => track primary owner only
	initial := 4
	for i := 0; i < initial; i++ {
		r.Add(fmt.Sprintf("node-%d", i))
	}

	const keys = 10000
	before := make([]string, keys)
	for i := 0; i < keys; i++ {
		before[i] = r.GetNodes(fmt.Sprintf("k-%d", i))[0]
	}

	r.Add(fmt.Sprintf("node-%d", initial)) // 5th node

	moved := 0
	for i := 0; i < keys; i++ {
		if r.GetNodes(fmt.Sprintf("k-%d", i))[0] != before[i] {
			moved++
		}
	}

	// Theory says ~1/(N+1) = 1/5 = 20% move. Allow generous slack for vnode
	// variance; the point is it is nowhere near 100% (what modular hashing gives).
	frac := float64(moved) / float64(keys)
	if frac > 0.35 {
		t.Fatalf("moved %.1f%% of keys on add; expected ~20%%", frac*100)
	}
	if frac == 0 {
		t.Fatal("no keys moved; new node received no load")
	}
	t.Logf("moved %.1f%% of keys (theory ~20%%)", frac*100)
}

// TestDistributionIsBalanced checks vnodes actually spread load across nodes.
func TestDistributionIsBalanced(t *testing.T) {
	r := New(200, 1)
	n := 5
	for i := 0; i < n; i++ {
		r.Add(fmt.Sprintf("node-%d", i))
	}
	counts := map[string]int{}
	const keys = 50000
	for i := 0; i < keys; i++ {
		counts[r.GetNodes(fmt.Sprintf("k-%d", i))[0]]++
	}
	ideal := float64(keys) / float64(n)
	for node, c := range counts {
		dev := math.Abs(float64(c)-ideal) / ideal
		if dev > 0.20 {
			t.Errorf("node %s owns %d keys, %.1f%% off ideal %.0f", node, c, dev*100, ideal)
		}
	}
}
