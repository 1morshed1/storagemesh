-- StorageMesh metadata schema.
--
-- This is the single source of truth for object metadata. Chunk *bytes* live on
-- the storage nodes; this database only records where things are and how they
-- relate. Every design choice below is explained in docs/04-metadata-store.md.

-- gen_random_uuid() lives in pgcrypto on older PGs; it is built in on PG13+.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS buckets (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        VARCHAR(63) UNIQUE NOT NULL,   -- S3 bucket naming: max 63 chars
    owner_id    UUID NOT NULL,
    versioning  BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS objects (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bucket_id       UUID NOT NULL REFERENCES buckets(id) ON DELETE CASCADE,
    key             TEXT NOT NULL,                     -- e.g. "photos/2024/img.jpg"
    version_id      UUID NOT NULL DEFAULT gen_random_uuid(),
    size_bytes      BIGINT NOT NULL,
    content_type    TEXT NOT NULL DEFAULT 'application/octet-stream',
    checksum_sha256 TEXT NOT NULL,                     -- ETag: SHA-256 of whole object
    chunk_manifest  JSONB NOT NULL,                    -- ordered chunk recipe
    is_latest       BOOLEAN NOT NULL DEFAULT TRUE,
    delete_marker   BOOLEAN NOT NULL DEFAULT FALSE,
    metadata        JSONB,                             -- user x-amz-meta-* headers
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (bucket_id, key, version_id)
);

-- Partial index: the hot path is "give me the current, non-deleted object for
-- (bucket, key)". Indexing only the latest live rows keeps the index tiny even
-- when a key has thousands of historical versions.
CREATE INDEX IF NOT EXISTS idx_objects_bucket_key_latest
    ON objects (bucket_id, key)
    WHERE is_latest = TRUE AND delete_marker = FALSE;

CREATE TABLE IF NOT EXISTS chunks (
    hash         TEXT PRIMARY KEY,          -- SHA-256 hex of chunk content
    size_bytes   INTEGER NOT NULL,
    ref_count    INTEGER NOT NULL DEFAULT 1,-- how many object versions use this chunk
    nodes        TEXT[] NOT NULL DEFAULT '{}', -- storage nodes holding the bytes
    verified_at  TIMESTAMPTZ,               -- last successful integrity check
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()  -- for GC grace-period math
);

-- GC scans for ref_count <= 0. A partial index makes that scan cheap.
CREATE INDEX IF NOT EXISTS idx_chunks_orphaned
    ON chunks (updated_at)
    WHERE ref_count <= 0;

CREATE TABLE IF NOT EXISTS multipart_uploads (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bucket_id   UUID NOT NULL REFERENCES buckets(id) ON DELETE CASCADE,
    key         TEXT NOT NULL,
    status      VARCHAR(20) NOT NULL DEFAULT 'in_progress', -- in_progress|completed|aborted
    parts       JSONB NOT NULL DEFAULT '[]',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '7 days'
);
