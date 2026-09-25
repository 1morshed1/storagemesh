# 03 — Storage Nodes: The Dumb, Durable Layer

**Code:** `internal/storage/`, `cmd/storagenode/`

## Design principle: keep nodes dumb

A storage node knows **nothing** about buckets, objects, versions, manifests, or
replication. Its entire vocabulary is four verbs over a content hash:

> store these bytes · give me those bytes · delete them · are they still intact?

All intelligence — where a chunk goes, how many copies, dedup, GC — lives in the
gateway and workers. Why push all the smarts up and leave the node dumb?

- **Scale-out:** dumb nodes are interchangeable. Add a box, it just serves bytes.
- **Swappable backend:** today the backend is the local filesystem; replacing it
  with S3, a raw block device, or MinIO touches only `DiskStore`.
- **Testability:** the node has no business rules to get wrong.

## On-disk layout

Chunks are files named by their hash, sharded into 256 directories by the first
two hex characters:

```
<DATA_DIR>/chunks/
├── a3/a3f2b8c9….chunk
├── 7d/7d1e4f9a….chunk
└── …
```

Why shard? A single directory holding millions of files makes most filesystems
crawl (directory lookups go linear/expensive). Two hex chars = 256 buckets,
spreading files evenly. Real systems shard deeper (e.g. `a3/f2/…`); two levels is
plenty for a portfolio scale.

## Three correctness details in `DiskStore.Put`

`internal/storage/diskstore.go` does three things that a naive `os.WriteFile`
would get wrong:

1. **Verify-on-write.** It hashes the bytes *as it writes them* and rejects the
   write if the result doesn't match the claimed hash (`ErrHashMismatch`). A
   corrupt transfer never gets stored under a valid name.
2. **Atomic write.** Bytes go to a temp file, get `fsync`'d, then `rename`'d into
   place. `rename` is atomic on POSIX, so a crash mid-write leaves a stray temp
   file (cleaned up), never a half-written chunk masquerading as complete.
3. **Path-traversal defence.** The hash is validated as hex before being used in
   a path, so a request for `../../etc/passwd` is rejected, not served.

Dedup is implicit: if the file already exists, `Put` is a no-op (content
addressing guarantees identical hash ⇒ identical bytes).

## The node HTTP API

`internal/storage/server.go` exposes the store using Go 1.22's stdlib router
(method + wildcard patterns — no web framework needed on the node):

| Method & path | Purpose | Notable status codes |
|---|---|---|
| `PUT /chunks/{hash}` | store bytes | `201` ok · `400` hash mismatch |
| `GET /chunks/{hash}` | stream bytes | `200` · `404` |
| `HEAD /chunks/{hash}` | existence check | `200` · `404` |
| `DELETE /chunks/{hash}` | remove (idempotent) | `204` |
| `GET /chunks/{hash}/verify` | integrity check | `200` healthy · `422` corrupt · `404` |
| `GET /chunks` | list all hashes | `200` |
| `GET /healthz` | liveness | `200` |

The `422` on a corrupt chunk is deliberate: the integrity checker (Phase 4) keys
off that exact status to trigger a repair from a healthy replica.

## The client

`internal/storage/client.go` is the typed wrapper the gateway and workers use.
Two things worth internalising as Go lessons:

- **Reuse one `http.Client`.** It pools TCP connections. A fresh client per
  request would leak sockets and exhaust ephemeral ports under load.
- **Always drain + close response bodies** (`drain()` helper). If you don't fully
  read a body before closing, the connection can't be returned to the pool —
  a subtle, very common Go resource leak.

`GetChunk` returns `ErrNotFound` on a 404 and `PutChunk` returns
`ErrHashMismatch` on a 400, so the gateway can *fail over to another replica*
instead of aborting the whole request.

## Running a node

```bash
NODE_ID=node-1 DATA_DIR=./data-node1 LISTEN=:8081 go run ./cmd/storagenode
```

The daemon does graceful shutdown: on `SIGTERM` it stops accepting new requests
and lets in-flight chunk transfers finish (important so Kubernetes rolling
restarts don't sever uploads).
