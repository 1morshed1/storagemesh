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
