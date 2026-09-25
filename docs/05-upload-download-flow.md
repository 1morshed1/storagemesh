# 05 — The Upload & Download Flow (Quorum, Streaming, Failover)

**Code:** `internal/gateway/` — `handlers.go`, `replicate.go`, `cluster.go`

This is where all the pieces meet. The gateway is **stateless**: it holds no
durable bytes, so you can run many replicas behind a load balancer.

## Upload: `PUT /bucket/key`

```
body ─▶ chunker ─▶ for each chunk:
                     ├─ IncrementChunkRef (atomic upsert)  ── dedup gate
                     │     └─ isNew?  yes ─▶ quorum-replicate bytes to R nodes
                     │                no  ─▶ skip upload (dedup hit)
                     └─ append to manifest + fold into whole-object hash
        ─▶ PutObject(metadata + manifest)
        ─▶ publish object.created  (best effort)
        ─▶ 200 OK, ETag = SHA-256 of whole object
```

Three ideas make this correct and fast:

### 1. The ref-count upsert is also the dedup gate

`storeChunk` calls `IncrementChunkRef` *first*. The atomic upsert (see doc 04)
returns whether the row was newly created:

- **new** → nobody had this chunk; replicate the bytes.
- **not new** → the bytes already exist on nodes; just skip the upload.

So deduplication and reference counting are the *same* operation. If replication
then fails, we `DecrementChunkRefs` to undo the reference, so we never record a
reference to bytes that were never stored. (Known small race: two simultaneous
first-time uploads of the same chunk — one wins the "new" flag, the other may
skip before the bytes land. Documented as a portfolio-scope limitation; a
production fix pins an "uploading" state or checks node presence.)

### 2. Quorum writes for availability + durability

`replicateChunk` fans the chunk out to all R replica nodes **in parallel** and
returns as soon as a **write quorum** `W = floor(N/2)+1` acknowledge (2 of 3):

- A single slow or dead node cannot stall or fail the upload — we only wait for
  the majority.
- A chunk counts as "stored" only once a majority hold it, so losing any single
  node afterwards cannot lose the chunk.
- Stragglers keep writing in the background (the ack channel is buffered so
  their goroutines never block); if they fail, the integrity checker /
  rebalancer restores the missing replica later.

This is the answer to the interview question *"how do you handle a storage node
failure during a write?"* — quorum writes.

### 3. Streaming, never buffering

The whole object is never held in memory. The chunker yields 8 MiB pieces; each
is replicated and released before the next is read. Peak gateway memory is ~one
chunk per in-flight request, independent of object size. The whole-object ETag is
computed incrementally by feeding each chunk into one running SHA-256.

## Download: `GET /bucket/key`

```
GetObject(metadata) ─▶ for each chunk hash in manifest order:
                          fetchChunk(replicas)  ── try replicas until one serves
                          io.Copy(chunk ─▶ client socket)
```

`fetchChunk` walks the chunk's replica list (in ring order) and returns the first
node that serves the bytes, transparently failing over past a node that is down
or returns 404. Bytes stream straight from the storage node through the gateway
to the client — flat memory again.

`TestReadFailoverWhenReplicaDown` proves this: it uploads with replication 3,
**closes a storage node**, and the GET still succeeds from a surviving replica.

## Health-driven ring membership

`Cluster.StartHealthChecks` probes every node's `/healthz` on an interval and
rebuilds the ring from the set that answered. A dead node is removed from the
ring, so new writes route around it automatically; when it recovers it rejoins.
Probes run concurrently so one slow node doesn't stall the sweep.

## What the tests prove (`gateway_test.go`)

The integration tests run real in-process storage nodes + a gateway on a
`MemStore` — the entire system in one test binary:

- `TestEndToEndPutGetDelete` — multi-chunk round trip, ETag = whole-object hash.
- `TestDeduplicationAcrossObjects` — same content under two keys ⇒ every chunk
  has `ref_count == 2` (dedup verified, not just assumed).
- `TestReadFailoverWhenReplicaDown` — kill a node, reads still succeed.
- `TestMultipartUpload` — parts stitched back in order.
- `TestPresignedURLFlow` — see doc 07.
