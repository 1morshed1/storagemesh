# 01 — Content-Addressed Storage: Chunking, Hashing, Dedup

**Code:** `internal/chunk/`

## The idea in one sentence

Instead of storing a file as one blob under its name, cut it into pieces and
store each piece under the hash of its own bytes.

## Why hash-as-address is powerful

Consider two files:

```
report-v1.pdf   = [A][B][C][D]
report-v2.pdf   = [A][B][C][E]   (only the last chunk changed)
```

If chunks are addressed by content hash:

- `A`, `B`, `C` have the *same* hash in both files.
- We store `A`, `B`, `C` **once** and `E` once. `D` too. So five physical
  chunks back two logical files that are 4 chunks each.
- Storing v2 after v1 costs only the bytes of `E`.

This is **deduplication**, and it falls out automatically from the addressing
scheme — no special "dedup pass" is needed.

Three properties we get for free:

| Property | Because |
|----------|---------|
| Deduplication | identical bytes → identical address → stored once |
| Integrity | re-hash the stored bytes; if it ≠ the address, it is corrupt |
| Immutability | you can't edit a chunk; a change is a *new* chunk |

## The streaming chunker

Uploads can be huge, so we never hold the whole file. `Chunker` wraps an
`io.Reader` and yields one chunk at a time (`internal/chunk/chunk.go`).

The one non-obvious bug it avoids: a plain `io.Reader.Read` is allowed to return
fewer bytes than you asked for *even mid-stream*. If we treated every short read
as "end of a chunk," the same file could chunk differently depending on network
timing — and dedup would silently fail. We use `io.ReadFull`, which keeps reading
until the buffer is full or the stream genuinely ends:

```go
n, err := io.ReadFull(c.r, c.buf)
// err == io.EOF            -> clean end, no bytes
// err == io.ErrUnexpectedEOF -> short FINAL chunk, emit the n bytes
// err == nil               -> a full-size chunk
```

Second subtlety: the read buffer is **reused** across calls, so we `copy` the
bytes into a fresh slice before returning them. Returning a slice of the shared
buffer would let the next `Next()` call overwrite a chunk the caller still holds.

## The manifest

Chunks alone don't tell you how to rebuild the file. The **manifest** is the
ordered recipe (`internal/chunk/manifest.go`):

```json
{
  "chunk_size": 8388608,
  "chunks": [
    {"index": 0, "hash": "a3f2…", "size": 8388608},
    {"index": 1, "hash": "7d1e…", "size": 4194304}
  ]
}
```

To download an object: read the manifest, fetch each chunk hash in `index`
order from a storage node, concatenate. The manifest is stored as JSONB on the
object row in Postgres.

## Reference counting (preview)

Because chunks are shared between objects, we cannot delete a chunk just because
one object referenced it — another object might still need it. Each chunk row
carries a `ref_count`. Uploading increments it (or creates the row); deleting an
object decrements it. The garbage collector only removes a chunk when its count
hits zero. Details in `04-metadata-store.md` and `06-background-workers.md`.

## What the tests prove

`internal/chunk/chunk_test.go` deliberately targets the failure modes:

- streams ending exactly on a boundary vs. with a short final chunk,
- reassembly equals the original bytes,
- identical content hashes identically (the dedup guarantee),
- correct behaviour when the reader dribbles one byte per `Read` (the
  short-read case that `io.ReadFull` protects us from).
