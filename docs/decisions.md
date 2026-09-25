# Decision Log

A running log of the engineering decisions made while building StorageMesh, with
the reasoning behind each. This is the "why" companion to the code comments.
Newest decisions at the bottom of each phase.

Format: **D-N — Title** · *what we chose* · *why* · *the trade-off / alternative*.

---

## Phase 1 — Foundations

### D-1 — Content-addressed storage as the core primitive
**Chose:** address every chunk by the SHA-256 of its own bytes; the hash is the
filename and the database key.
**Why:** it gives deduplication, integrity checking, and immutability for free.
If the bytes are identical the address is identical, so identical chunks are
stored once and any corruption is detectable by re-hashing.
**Trade-off:** you cannot mutate a chunk in place — a change produces a new hash
and a new chunk. That is fine for object storage (objects are versioned, not
edited byte-by-byte) but would be wrong for, say, a database page store.

### D-2 — 8 MiB fixed-size chunks (not content-defined chunking)
**Chose:** split on a fixed 8 MiB boundary.
**Why:** simple, predictable, and matches the plan. Fixed chunks are trivial to
reason about and to test.
**Trade-off:** fixed chunking dedups poorly when bytes are *inserted* (every
boundary after the insert shifts, changing all downstream hashes). Real systems
(restic, borg) use content-defined chunking (a rolling hash picks boundaries) to
survive insertions. We note this as a known limitation; upgrading only touches
the `chunk` package because the rest of the system just consumes a manifest.

### D-3 — Streaming chunker via `io.ReadFull`
**Chose:** read the upload as a stream, one reused buffer, emit chunks lazily.
**Why:** uploads can be gigabytes; buffering the whole file would blow up memory
and cap concurrency. A single naive `Read()` can legally return a short read
mid-stream, which would create mis-sized chunks and silently break dedup, so we
use `io.ReadFull` to guarantee full-size chunk boundaries.
**Trade-off:** none meaningful; this is the correct pattern. The subtlety is
copying bytes out of the reused buffer before returning them (aliasing bug
otherwise) — see the comment in `chunk.go`.

### D-4 — CRC32 for ring placement, SHA-256 for chunk identity
**Chose:** two different hash functions for two different jobs.
**Why:** the chunk's *identity* must be collision-resistant → SHA-256. Its
*position on the hash ring* just needs fast, uniform scattering → CRC32. Using
SHA-256 for ring math would waste cycles for no benefit.
**Trade-off:** none; this mirrors how production systems separate the concerns.

### D-5 — `Store` interface with in-memory + Postgres implementations
**Chose:** define metadata persistence as a Go interface; ship a memory impl for
tests and a Postgres impl for real use.
**Why:** dependency inversion. Handlers depend on the interface, not on Postgres,
so we can unit-test the entire upload/download/GC logic with an in-memory store
and no database container. This is the idiomatic Go way to keep I/O at the edges.
**Trade-off:** the interface must stay honest — the memory impl has to reproduce
Postgres semantics (e.g. ref-count atomicity) or tests pass while production
breaks. We keep the interface small to limit that risk.

---

## Phase 2 — Storage nodes

### D-6 — HTTP instead of gRPC between gateway and nodes
**Chose:** plain HTTP (net/http) for chunk transfer, not the gRPC service the
plan sketches.
**Why:** (a) no protoc toolchain required, so the project builds with only the
Go compiler; (b) chunk transfer is a simple request/response over a byte stream
— `PUT /chunks/{hash}` with the body streamed — which HTTP does natively without
gRPC's code generation and streaming-RPC machinery; (c) it keeps the node
debuggable with `curl`.
**Trade-off:** we give up gRPC's typed contracts, bidirectional streaming, and
slightly lower per-call overhead. If this grew, the swap is localized: only
`internal/storage/{client,server}.go` change; `DiskStore` and every caller stay
put because they depend on the `Client`'s Go methods, not on HTTP directly.

### D-7 — Verify-on-write + atomic rename in the disk store
**Chose:** hash bytes while writing and reject mismatches; write to a temp file,
fsync, then rename into place.
**Why:** content addressing is only trustworthy if the stored bytes truly match
their name. Verifying on write catches transport corruption immediately. The
temp-then-rename dance makes writes atomic so a crash can never publish a
half-written chunk under a valid hash.
**Trade-off:** an extra fsync per write costs latency. Correct durability is
worth it; a throughput-tuned system might batch fsyncs.

### D-8a — Atomic ref counting via upsert (INSERT ... ON CONFLICT)
**Chose:** one `INSERT ... ON CONFLICT (hash) DO UPDATE ... RETURNING ref_count`
statement for `IncrementChunkRef`, using the returned count to decide new-vs-dedup.
**Why:** two concurrent uploads of the same new chunk would race a naive
select-then-insert (lost update or duplicate-key error). The upsert is atomic and
serialized by Postgres, and `RETURNING ref_count` tells the caller if it was
first (ref_count==1 → upload bytes) or a dedup hit (skip). Let the database do
the concurrency control it is built for.
**Trade-off:** ties this logic to a DB that supports upsert; every mainstream one
does. The `MemStore` reproduces the same semantics under a mutex.

### D-8b — Colocate schema.sql in the package, embed with go:embed
**Chose:** keep the SQL at `internal/metadata/schema.sql` and embed it; the store
applies it on connect.
**Why:** `go:embed` cannot reach a repo-root `migrations/` dir from inside the
package, and a single embedded source of truth avoids drift between "what the
binary applies" and "what operators apply." A single binary bootstraps its own DB.
**Trade-off:** not a real migration framework (no versioned up/down). Fine at this
scale; a production system would adopt golang-migrate or goose.

---

## Phase 3 — Upload/download flow

### D-9 — Quorum writes (W = floor(N/2)+1), fan-out in parallel
**Chose:** replicate each chunk to all R nodes concurrently, return success at a
majority of acks; stragglers finish in the background.
**Why:** availability. A single slow/dead node must not stall or fail an upload.
A majority-ack rule also guarantees durability — a chunk is "stored" only once a
majority hold it, so any single later node loss can't lose it. Reads are simpler
(chunks are immutable, content-addressed) so we fail over across replicas rather
than doing a read quorum.
**Trade-off:** a chunk may briefly have fewer than R live copies until the
background writers / rebalancer catch up. Acceptable given quorum durability; the
integrity checker repairs under-replication.

### D-9a — Ref-count upsert as the dedup gate; roll back on replication failure
**Chose:** call IncrementChunkRef before uploading; upload only when it reports
"new"; DecrementChunkRefs if replication then fails.
**Why:** makes dedup and ref counting one atomic operation and guarantees we
never record a reference to bytes that were never stored.
**Trade-off:** a small race exists for two simultaneous first-time uploads of the
identical chunk (one may skip upload before the other's bytes land). Documented;
a production fix would track an "uploading" state or verify node presence.

### D-10 — Shared access key + HMAC pre-signed URLs instead of AWS SigV4
**Chose:** a constant-time shared-key header check for normal requests, plus full
HMAC-SHA256 pre-signed URLs.
**Why:** demonstrates the signing/verification mechanics (canonical string, HMAC,
constant-time compare, expiry, method binding) that matter for the interview,
without the bulk of implementing SigV4's canonical-request algorithm, which would
distract from the storage system.
**Trade-off:** not wire-compatible with real S3 clients' auth. Isolated to
auth.go; SigV4 could replace it without touching handlers.

### D-8 — Idempotent deletes, existence-based dedup
**Chose:** deleting a missing chunk returns success; putting an existing chunk is
a no-op.
**Why:** the garbage collector and rebalancer retry operations, so every node
verb must be safe to repeat. Idempotency turns "did my retry double-delete?" into
a non-question.
**Trade-off:** none; idempotency is the right default for a distributed system's
data-plane operations.
