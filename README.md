# StorageMesh

A simplified, S3-compatible **distributed object storage service** written in Go.

Clients `PUT`/`GET` files over an HTTP API. StorageMesh chunks each file,
addresses every chunk by its SHA-256 content hash, replicates chunks across
storage nodes chosen by consistent hashing, and serves them back. It supports
versioning, multipart uploads, pre-signed URLs, deduplication, and background
garbage collection / integrity checking.

> This is a portfolio / learning project. The [`docs/`](docs/) folder is a full
> written walkthrough of the *why* behind every component — start with
> [`docs/00-overview.md`](docs/00-overview.md).

## Architecture

```
   client ──HTTP──▶ Gateway ──┬── metadata ──▶ PostgreSQL
                              └── chunk bytes ─▶ Storage Node ring (N nodes, R=3 replicas)
                                                     ▲
                                          Workers ───┘  (GC · integrity · rebalance)
```

- **Gateway** (`cmd/gateway`) — stateless HTTP API: auth, chunking, replication,
  multipart assembly, pre-signed URLs.
- **Storage nodes** (`cmd/storagenode`) — store raw chunk bytes on local disk,
  indexed by content hash.
- **PostgreSQL** — object/bucket/chunk metadata and manifests.
- **Consistent hash ring** — decides which nodes hold each chunk; adding a node
  moves only ~1/N of the data.

## Status

Built in phases; see [`docs/`](docs/) and [`docs/decisions.md`](docs/decisions.md).

- [x] Phase 1 — chunking, content addressing, consistent hash ring
- [x] Phase 2 — storage nodes (disk store + node API + client)
- [x] Phase 3 — upload/download flow with quorum replication + metadata
- [ ] Phase 4 — background workers (GC, integrity, rebalance)
- [x] Phase 5 — pre-signed URLs, multipart uploads
- [ ] Deploy — docker-compose + Kubernetes manifests

## Quick start (dev)

```bash
# Go 1.24+ required
go test ./...          # run the test suite
```

Full run instructions land in [`docs/08-running-it.md`](docs/08-running-it.md)
as the deployment layer is completed.

## Layout

```
cmd/gateway         HTTP API server
cmd/storagenode     chunk storage daemon
internal/chunk      streaming chunker + manifest
internal/hashring   consistent hashing with virtual nodes
internal/storage    on-disk chunk store + node client/server
internal/metadata   Store interface (Postgres + in-memory)
internal/gateway    HTTP handlers
internal/presign    HMAC pre-signed URLs
internal/worker     GC / integrity / rebalancer
internal/events     Kafka event publisher (+ no-op fallback)
migrations          SQL schema
deploy              docker-compose + k8s
docs                learning walkthrough
```
