# 09 — Interview Guide: Explaining StorageMesh

This ties the whole system together the way you'd explain it in an interview.
Each talking point links to where it lives in code and the test that proves it.

## The 60-second pitch

> StorageMesh is an S3-compatible object store. On upload it streams the file
> through a chunker that splits it into 8 MiB pieces, hashes each piece with
> SHA-256, and stores each piece by its hash — content addressing. A consistent
> hash ring decides which 3 nodes hold each chunk; writes use a majority quorum
> so a node can be down without failing the write. Chunks are reference-counted,
> so identical data across objects is stored once, and a garbage collector
> reclaims chunks when their last reference is deleted. Background workers verify
> integrity (re-hashing to catch bit rot) and rebalance replicas when nodes join
> or recover. Metadata lives in Postgres; the gateway is stateless and
> horizontally scalable.

## The five questions from the spec, answered

**Q: How do you handle a storage-node failure during a write?**
Quorum writes. We fan the chunk out to all R=3 replicas in parallel and succeed
once a majority (2) acknowledge, so one dead/slow node neither fails nor stalls
the write. The lagging replica is filled later by the rebalancer.
→ `internal/gateway/replicate.go` (`replicateChunk`, `quorum`) · proven by
`TestReadFailoverWhenReplicaDown` and the live compose failover in doc 08.

**Q: What if a chunk is corrupted?**
The integrity checker periodically asks each node to re-hash its chunks. A node
whose bytes no longer match the hash returns `422`; the checker deletes that bad
copy and restores it from a replica that verifies healthy.
→ `internal/worker/integrity.go` · proven by `TestIntegrityCheckerRepairsCorruption`.

**Q: How does deduplication work across objects?**
Content addressing + reference counting. A chunk's identity *is* its SHA-256, so
identical bytes get the same address and are stored once. The atomic ref-count
upsert (`INSERT … ON CONFLICT`) both counts references and tells the uploader
whether the bytes are new (upload) or already present (skip).
→ `internal/metadata/postgres.go` (`IncrementChunkRef`), `internal/gateway/handlers.go`
(`storeChunk`) · proven by `TestDeduplicationAcrossObjects` (ref_count == 2).

**Q: What happens when you add a fourth storage node?**
Consistent hashing means only ~1/N of chunks change ownership (not everything,
as modular hashing would). The rebalancer copies the reassigned chunks onto the
new node.
→ `internal/hashring/hashring.go`, `internal/worker/rebalance.go` · proven by
`TestMinimalReshuffleOnAdd` (~20% moved, not ~100%) and `TestRebalancerFillsMissingReplicas`.

**Q: How do pre-signed URLs work?**
The server HMACs `(method, bucket, key, expiry)` with a secret. The signature
travels in the URL; on use, the server recomputes it and checks it matches
(constant-time) and hasn't expired. No credentials needed to *use* the URL;
credentials are needed to *mint* one.
→ `internal/presign/presign.go` · proven by `TestPresignedURLFlow`.

## The design principles worth naming

1. **Separate the data plane from the control plane.** Dumb storage nodes move
   bytes; the smart gateway/workers decide placement, replication, dedup, GC.
   That split is why nodes are interchangeable and the gateway scales.
2. **Push I/O behind interfaces (dependency inversion).** `metadata.Store` and
   `worker.ClusterView` let the entire system be tested in-process with no
   Postgres, no Kafka, no containers — see `go test ./...`.
3. **Let the database do concurrency control.** The ref-count race is solved by
   one atomic upsert, not application locks.
4. **Immutability + content addressing make hard problems easy.** Dedup,
   integrity, and multipart assembly all fall out of "address bytes by their hash."
5. **Match the workload to the runtime.** Stateful nodes → StatefulSet; stateless
   gateway → Deployment + HPA.

## Honest limitations (know these; they show maturity)

- Fixed-size chunking dedups poorly under byte *insertion* (content-defined
  chunking would fix it) — D-2.
- A small dedup race on simultaneous first-upload of identical chunks — D-9a.
- Auth is a shared key + pre-signed URLs, not full AWS SigV4 — D-10.
- The rebalancer fills missing replicas but doesn't garbage-collect misplaced
  ones — D-11.
- Demo Postgres is single-node; production wants replication/backups — D-14.

Full reasoning for every decision is in [`decisions.md`](decisions.md).
