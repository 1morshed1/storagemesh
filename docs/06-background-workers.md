# 06 — Background Workers: GC, Integrity, Rebalance

**Code:** `internal/worker/`, `cmd/worker/`

These jobs run on timers, never on the request path, and keep the cluster healthy
over time. They depend only on `metadata.Store` and a small `ClusterView`
interface (`ClientsFor`, `AllClients`), so they test against a `MemStore` plus
in-process nodes. `ClusterView` is defined in the *worker* package (consumer-side
interface) so workers don't import the gateway — `*gateway.Cluster` just happens
to satisfy it.

## Garbage Collector (`gc.go`)

Chunks are shared (dedup) and reference-counted. When the last object referencing
a chunk is deleted, its `ref_count` hits 0 and the bytes become garbage. GC
reclaims them:

1. `ListOrphanChunks(grace)` — chunks with `ref_count <= 0` untouched for longer
   than the grace period.
2. For each, delete the bytes from every node in the chunk's recorded node list
   (deletes are idempotent, so a node that already lost it is fine).
3. `DeleteChunkRef` — removes the metadata row, **but only if still orphaned**
   (`WHERE ref_count <= 0`), sparing a chunk an upload re-referenced in between.

**Why the grace period (default 24h):** there is a window between "chunk stored"
and "object committed." A delete could momentarily drop a shared chunk to 0 while
another upload is about to reference it. Waiting avoids racing in-flight
references. `TestGarbageCollectorSparesReferenced` confirms a referenced chunk is
never touched; `TestGarbageCollectorDeletesOrphans` confirms orphans are removed
from disk *and* metadata.

## Integrity Checker (`integrity.go`)

Defends against **bit rot** — bytes silently flipping on disk so they no longer
match their content hash.

1. For every node, `ListChunks` then `VerifyChunk` each (the node re-hashes its
   stored bytes and compares to the expected hash).
2. Healthy → record `verified_at`.
3. Corrupt (node returns `422`) → **repair**: find a replica that verifies
   healthy, delete the corrupt copy, and copy the good bytes over.

The subtle bug found while building this: a storage node treats an *existing*
file as already-correct (content addressing assumes hash == bytes) and skips the
overwrite. So repair must **delete the corrupt copy first**, then write fresh.
The destination re-verifies on write, so a bad source can never poison it.
`TestIntegrityCheckerRepairsCorruption` corrupts a file on disk and asserts it is
healed.

This answers *"what if a chunk is corrupted?"*

## Rebalancer (`rebalance.go`)

Ensures every chunk physically lives on the nodes the ring currently assigns.
One mechanism covers two situations:

- **Node joins** → the ring reassigns some chunks to it → copy them onto it.
- **Under-replication** → a node was down during a quorum write (the write
  succeeded with < R copies) → fill the missing replicas.

Algorithm: discover every chunk hash across all nodes (remembering one holder as
a copy source), then for each, ensure every ring-assigned node has it; copy from
the holder to any that don't. `TestRebalancerFillsMissingReplicas` places a chunk
on one node with replication 3 and asserts the rebalancer makes exactly 2 copies.

**Known limitation (documented on purpose):** it copies missing replicas but does
*not* delete now-misplaced copies from nodes the ring no longer assigns. A stale
extra copy is a space cost, never a correctness bug; cleaning it up is deferred
work. This is the honest engineering trade-off — see decision **D-11**.

## Deployment

`cmd/worker` runs all three on independent tickers (GC hourly, integrity every
6h, rebalance every 15m by default, all env-tunable). It is a **separate process
from the gateway** so maintenance load never competes with request serving and
the two scale independently.
