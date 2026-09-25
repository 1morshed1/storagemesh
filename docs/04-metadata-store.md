# 04 — Metadata Store: Postgres, the Store Interface, Ref Counting

**Code:** `internal/metadata/`, schema in `internal/metadata/schema.sql`

## What the metadata plane stores

Chunk *bytes* live on storage nodes. Postgres stores everything *about* the data:

- **buckets** — namespaces owned by a user, versioning on/off.
- **objects** — one row per *version* of a key; the manifest (chunk recipe) lives here.
- **chunks** — the reference-counted index: which nodes hold each chunk, how many objects use it.
- **multipart_uploads** — in-progress multipart state.

## The `Store` interface (dependency inversion)

`types.go` defines a `Store` interface; nothing above this package imports
Postgres. Two implementations satisfy it:

- `PostgresStore` — the real thing (`postgres.go`).
- `MemStore` — in-memory, for tests and local demos (`memstore.go`).

This is the single most useful pattern for testable Go services: **push I/O
behind an interface at the edges.** The gateway's upload logic, the GC worker's
logic — all of it runs against `MemStore` in unit tests with no database
container. See decision **D-5**. The catch: `MemStore` must faithfully mimic
Postgres semantics, or green tests lie. The two places that matters most are
ref-count atomicity and delete-marker behaviour, so both stores implement them
the same way.

## Reference counting is the whole ballgame

Because chunks are shared between objects (dedup), we can't delete a chunk when
one object goes away — another might still use it. Each `chunks` row has a
`ref_count`:

- **Upload references a chunk** → `IncrementChunkRef`. If the chunk is new,
  ref_count starts at 1 and the gateway uploads the bytes. If it already exists,
  ref_count bumps and the gateway *skips the upload* (dedup hit).
- **Object deleted** → `DecrementChunkRefs` on every hash in its manifest.
- **GC** later removes any chunk whose ref_count hit 0 (after a grace period).

### The concurrency trap and how Postgres solves it

Two uploads reference the same brand-new chunk at the same moment. Naively:

```
upload A: SELECT ... -> not found -> INSERT ref_count=1
upload B: SELECT ... -> not found -> INSERT ref_count=1   -- lost update / dup key!
```

The fix is a single atomic **upsert** — `INSERT ... ON CONFLICT DO UPDATE`:

```sql
INSERT INTO chunks (hash, size_bytes, ref_count, nodes, updated_at)
VALUES ($1, $2, 1, $3, NOW())
ON CONFLICT (hash) DO UPDATE
  SET ref_count = chunks.ref_count + 1,
      nodes = (SELECT ARRAY(SELECT DISTINCT unnest(chunks.nodes || EXCLUDED.nodes))),
      updated_at = NOW()
RETURNING ref_count;
```

Postgres serializes concurrent executions of this statement. The `RETURNING
ref_count` tells the caller who was first: `ref_count == 1` means "you created
it, upload the bytes"; anything higher means "dedup hit, skip." No application
lock, no read-modify-write race. This is the key lesson: **let the database do
the concurrency control it is designed for.**

Two more safety details in `postgres.go`:

- `DecrementChunkRefs` uses `GREATEST(ref_count - 1, 0)` so a double-decrement
  can't drive the count negative.
- `DeleteChunkRef` deletes `WHERE ref_count <= 0`, so if an upload re-referenced
  the chunk between the GC scan and the delete, the chunk is spared.

## Versioning via delete markers

On a versioned bucket, deleting an object doesn't erase history — it writes a new
object row with `delete_marker = TRUE` that becomes the latest. `GetObject`
returns "not found" when the latest row is a marker, but old versions remain
addressable by `version_id`. `PutObject` demotes the previous latest and inserts
the new version inside one transaction, so a reader never sees two latest rows.

## Schema choices worth noting

- **Partial index** `idx_objects_bucket_key_latest ... WHERE is_latest AND NOT
  delete_marker`: the hot lookup is "current live object for (bucket, key)".
  Indexing only live-latest rows keeps the index tiny even with thousands of
  versions per key.
- **Partial index** on orphaned chunks (`WHERE ref_count <= 0`) makes the GC scan
  cheap — it never walks the millions of healthy chunks.
- **`chunk_manifest JSONB`**: the manifest is a variable-length ordered list;
  JSONB stores it inline with the object, so a read is one row fetch.

## Bootstrapping

`NewPostgresStore` embeds `schema.sql` with `go:embed` and applies it on connect,
so a single binary sets up its own tables — no separate migration tool needed to
try the project. The same file is what docker-compose / `psql` users apply.
