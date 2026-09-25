# 02 — Consistent Hashing: Where Does a Chunk Live?

**Code:** `internal/hashring/`

## The problem

We have N storage nodes and a chunk. Which node stores it?

**Naive answer:** `node = hash(chunk) % N`. This works — until N changes. Add a
node (N → N+1) and the modulus changes for almost *every* chunk, so almost all
data has to move between nodes. For a storage system that is a disaster: adding
one disk triggers a full-cluster reshuffle.

## The fix: a ring

Map both nodes and chunks onto a circle of positions `[0, 2^32)`:

```
            0 / 2^32
              ┌───────┐
      nodeC ● │       │ ● nodeA
              │       │
              │   ●───┼──── chunk X  (walk clockwise → owned by nodeA)
      nodeB ● │       │
              └───────┘
```

A chunk is owned by the **first node clockwise** from the chunk's position.

Now add `nodeD`. It lands somewhere on the circle and only steals the arc
between itself and the previous node clockwise. **Only the chunks in that one arc
move — on average 1/N of the data, and only from one neighbour.** Everything
else stays exactly where it was. That is the whole point.

## Virtual nodes (vnodes)

With one point per node the arcs are uneven — by luck one node could own 40% of
the circle. And when a node dies, its entire arc dumps onto a single neighbour.

Fix: give each physical node **many** points (we use 150). Node "node-1" is
placed at `hash("node-1#0")`, `hash("node-1#1")`, … 150 times. With many small
arcs per node:

- each node's *total* arc converges to a fair 1/N (law of large numbers),
- when a node leaves, its many arcs spread across many neighbours, not one.

`internal/hashring/hashring_test.go::TestDistributionIsBalanced` checks every
node ends up within 20% of the ideal share across 50k keys.

## Selecting R replicas

We want 3 copies of each chunk on 3 *different machines*. `GetNodes` finds the
clockwise owner, then keeps walking, collecting distinct physical nodes until it
has R of them:

```go
idx := sort.Search(len(r.sorted), func(i int) bool { return r.sorted[i] >= h })
// walk clockwise, skip vnodes whose physical node we already picked
for ... len(out) < want ... {
    nodeID := r.positions[pos]
    if seen[nodeID] { continue }   // <- this line is what puts replicas on
    out = append(out, nodeID)      //    different machines
}
```

Skipping already-seen physical nodes matters: without it, the next two vnodes
clockwise might belong to the *same* box, and your "3 replicas" would be 3 copies
on one machine — useless for durability.

Edge case: if the cluster has fewer nodes than the replication factor, we return
all of them. You can't make 3 copies on 2 machines.

## Why CRC32 here and SHA-256 for the chunk

The chunk's *identity* needs to be collision-resistant → SHA-256. Its *position
on the ring* only needs to be fast and uniformly scattered → CRC32. Two jobs,
two tools. See decision **D-4**.

## What the tests prove

- `TestMinimalReshuffleOnAdd`: adding a 5th node to a 4-node ring moves ~20% of
  keys (theory: 1/5), never ~100%. This is the headline guarantee.
- `TestReplicationReturnsDistinctNodes`: 3 replicas are always distinct nodes.
- `TestDeterministicPlacement`: same nodes → same placement, every time.
- `TestDistributionIsBalanced`: load is even across nodes.
