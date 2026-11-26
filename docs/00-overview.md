# StorageMesh — Learning Log & Architecture

> A simplified, S3-compatible distributed object storage service in Go.
> Every file in this `docs/` folder explains not just
> *what* the code does but *why* it is built that way and what trade-offs were
> made. Read the docs in order; each maps to a build phase.

## What we are building

A service where clients `PUT` and `GET` files over an HTTP API. Under the hood:

- Files are **chunked** into 8 MiB pieces.
- Each chunk is stored by its **SHA-256 content hash** (content-addressed).
- Chunks are **replicated** across multiple storage nodes.
- Which nodes hold a chunk is decided by **consistent hashing**.
- Object metadata (buckets, keys, versions, chunk manifests) lives in **PostgreSQL**.
- Background **workers** garbage-collect orphaned chunks, verify integrity, and rebalance.
- **Pre-signed URLs** allow credential-free, time-limited access.

## The mental model

```
             ┌─────────────┐
   client ──▶│  Gateway    │  HTTP S3-ish API, chunking, replication, auth
             │  (Go/Gin)   │
             └──────┬──────┘
        metadata    │    chunk bytes (HTTP)
        ┌───────────┤─────────────┐
        ▼           ▼             ▼
   ┌─────────┐  ┌────────┐  ┌────────┐  ┌────────┐
   │Postgres │  │ Node 1 │  │ Node 2 │  │ Node 3 │  ...  the storage ring
   └─────────┘  └────────┘  └────────┘  └────────┘
                     ▲
                ┌────┴─────┐
                │ Workers  │  GC / integrity / rebalance
                └──────────┘
```

The single most important idea: **the gateway holds no file bytes durably.** It
is a stateless router. Durability lives in the storage nodes (chunk bytes) and
Postgres (metadata). That separation is what lets the gateway scale horizontally.

## Reading order

| Doc | Topic | Build phase |
|-----|-------|-------------|
| `01-content-addressed-storage.md` | Chunking, hashing, dedup, manifests | Phase 1 |
| `02-consistent-hashing.md` | The hash ring, vnodes, replica selection | Phase 2 |
| `03-storage-nodes.md` | On-disk chunk store + node HTTP API | Phase 2 |
| `04-metadata-store.md` | Postgres schema, the Store interface, ref counting | Phase 1/3 |
| `05-upload-download-flow.md` | Quorum writes, streaming, the read path | Phase 3 |
| `06-background-workers.md` | GC, integrity checker, rebalancer | Phase 4 |
| `07-presigned-urls.md` | HMAC signing, constant-time verification | Phase 5 |
| `08-running-it.md` | docker-compose, trying it end to end | — |
| `09-system-walkthrough.md` | The whole system end to end; concept → code → test | — |
| `decisions.md` | Running log of engineering decisions & deviations from the spec | all |

## Key engineering decisions at a glance

See `decisions.md` for the full log with reasoning. The headline ones:

1. **HTTP (not gRPC) between gateway and storage nodes.** The spec specifies
   gRPC. We use plain HTTP with streaming bodies to keep the project buildable
   without a protoc toolchain, and because chunk transfer is a simple
   request/response that does not need gRPC's streaming multiplexing. The
   trade-off and how we'd switch is documented in `decisions.md`.
2. **A `Store` interface with in-memory and Postgres implementations.** This
   lets the whole system be unit-tested without a running database, and teaches
   the dependency-inversion pattern that makes Go services testable.
3. **Content-addressed dedup with reference counting**, exactly as the spec
   describes, because it is the intellectual core of the project.
