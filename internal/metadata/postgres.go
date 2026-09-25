package metadata

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/1morshed1/storagemesh/internal/chunk"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// schemaSQL embeds the SQL schema so a single binary can bootstrap its own
// database — no external migration tool needed to try the project. It is the
// single source of truth; docker-compose and psql users apply this same file.
//
//go:embed schema.sql
var schemaSQL string

// PostgresStore is the production Store, backed by a pgx connection pool.
//
// Two Postgres features do the heavy lifting here and are worth understanding:
//
//   - Atomic ref counting via INSERT ... ON CONFLICT DO UPDATE (upsert). The
//     whole dedup/ref-count race — "two uploads reference the same new chunk at
//     once" — collapses into one atomic statement the database serializes for us.
//   - Delete markers as rows, not deletions, so versioning keeps history.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore connects to dsn (e.g. "postgres://user:pass@host:5432/db")
// and runs the embedded migrations. It pings first so a bad DSN fails fast at
// startup rather than on the first request.
func NewPostgresStore(ctx context.Context, dsn string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("metadata: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("metadata: ping: %w", err)
	}
	s := &PostgresStore{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *PostgresStore) migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("metadata: apply schema: %w", err)
	}
	return nil
}

func (s *PostgresStore) Close() error { s.pool.Close(); return nil }

// ---- Buckets ----

func (s *PostgresStore) CreateBucket(ctx context.Context, b *Bucket) error {
	err := s.pool.QueryRow(ctx,
		`INSERT INTO buckets (name, owner_id, versioning)
		 VALUES ($1, $2, $3) RETURNING id, created_at`,
		b.Name, b.OwnerID, b.Versioning,
	).Scan(&b.ID, &b.CreatedAt)
	if isUniqueViolation(err) {
		return ErrBucketExists
	}
	return err
}

func (s *PostgresStore) GetBucket(ctx context.Context, name string) (*Bucket, error) {
	b := &Bucket{}
	err := s.pool.QueryRow(ctx,
		`SELECT id, name, owner_id, versioning, created_at FROM buckets WHERE name=$1`, name,
	).Scan(&b.ID, &b.Name, &b.OwnerID, &b.Versioning, &b.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrBucketNotFound
	}
	return b, err
}

func (s *PostgresStore) ListBuckets(ctx context.Context, ownerID string) ([]*Bucket, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, name, owner_id, versioning, created_at FROM buckets
		 WHERE ($1 = '' OR owner_id = $1::uuid) ORDER BY created_at`, ownerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Bucket
	for rows.Next() {
		b := &Bucket{}
		if err := rows.Scan(&b.ID, &b.Name, &b.OwnerID, &b.Versioning, &b.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *PostgresStore) DeleteBucket(ctx context.Context, name string) error {
	b, err := s.GetBucket(ctx, name)
	if err != nil {
		return err
	}
	var live int
	if err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM objects WHERE bucket_id=$1 AND is_latest AND NOT delete_marker`,
		b.ID).Scan(&live); err != nil {
		return err
	}
	if live > 0 {
		return ErrBucketNotEmpty
	}
	_, err = s.pool.Exec(ctx, `DELETE FROM buckets WHERE id=$1`, b.ID)
	return err
}

// ---- Objects ----

func (s *PostgresStore) PutObject(ctx context.Context, o *Object) error {
	manifestJSON, err := o.Manifest.JSON()
	if err != nil {
		return err
	}
	var metaJSON []byte
	if o.Metadata != nil {
		metaJSON, _ = json.Marshal(o.Metadata)
	}

	// A new version supersedes the old. Do both writes in one transaction so a
	// reader never sees two "latest" rows (or zero) for the same key.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint: no-op after commit

	if _, err := tx.Exec(ctx,
		`UPDATE objects SET is_latest=FALSE WHERE bucket_id=$1 AND key=$2 AND is_latest`,
		o.BucketID, o.Key); err != nil {
		return err
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO objects
		   (bucket_id, key, size_bytes, content_type, checksum_sha256, chunk_manifest, is_latest, delete_marker, metadata)
		 VALUES ($1,$2,$3,$4,$5,$6,TRUE,$7,$8)
		 RETURNING id, version_id, created_at`,
		o.BucketID, o.Key, o.SizeBytes, o.ContentType, o.ChecksumSHA, manifestJSON, o.DeleteMarker, metaJSON,
	).Scan(&o.ID, &o.VersionID, &o.CreatedAt)
	if err != nil {
		return err
	}
	o.IsLatest = true
	return tx.Commit(ctx)
}

func (s *PostgresStore) GetObject(ctx context.Context, bucketID, key string) (*Object, error) {
	return s.scanObject(s.pool.QueryRow(ctx,
		`SELECT id, bucket_id, key, version_id, size_bytes, content_type, checksum_sha256,
		        chunk_manifest, is_latest, delete_marker, metadata, created_at
		 FROM objects
		 WHERE bucket_id=$1 AND key=$2 AND is_latest AND NOT delete_marker`,
		bucketID, key))
}

func (s *PostgresStore) GetObjectVersion(ctx context.Context, bucketID, key, versionID string) (*Object, error) {
	return s.scanObject(s.pool.QueryRow(ctx,
		`SELECT id, bucket_id, key, version_id, size_bytes, content_type, checksum_sha256,
		        chunk_manifest, is_latest, delete_marker, metadata, created_at
		 FROM objects WHERE bucket_id=$1 AND key=$2 AND version_id=$3`,
		bucketID, key, versionID))
}

func (s *PostgresStore) ListObjects(ctx context.Context, bucketID, prefix string, limit int) ([]*Object, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, bucket_id, key, version_id, size_bytes, content_type, checksum_sha256,
		        chunk_manifest, is_latest, delete_marker, metadata, created_at
		 FROM objects
		 WHERE bucket_id=$1 AND is_latest AND NOT delete_marker AND key LIKE $2
		 ORDER BY key LIMIT $3`,
		bucketID, prefix+"%", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Object
	for rows.Next() {
		o, err := s.scanObjectRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

func (s *PostgresStore) DeleteObject(ctx context.Context, bucketID, key string) error {
	// Non-versioned semantics: physically drop all versions of the key. The
	// caller decrements chunk refs first (it has read the manifest).
	ct, err := s.pool.Exec(ctx, `DELETE FROM objects WHERE bucket_id=$1 AND key=$2`, bucketID, key)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return ErrObjectNotFound
	}
	return nil
}

// ---- Chunk ref counting ----

// IncrementChunkRef is the atomic upsert at the heart of dedup. If the chunk is
// new, we INSERT it with ref_count=1 and report isNew=true (caller uploads
// bytes). If it exists, ON CONFLICT bumps ref_count and merges node lists,
// reporting isNew=false (dedup hit, skip upload). Postgres serializes concurrent
// executions of this statement, so two simultaneous uploads of the same new
// chunk can't both think they are first.
func (s *PostgresStore) IncrementChunkRef(ctx context.Context, ref ChunkRef) (bool, error) {
	var refCount int
	err := s.pool.QueryRow(ctx,
		`INSERT INTO chunks (hash, size_bytes, ref_count, nodes, updated_at)
		 VALUES ($1, $2, 1, $3, NOW())
		 ON CONFLICT (hash) DO UPDATE
		   SET ref_count = chunks.ref_count + 1,
		       nodes = (SELECT ARRAY(SELECT DISTINCT unnest(chunks.nodes || EXCLUDED.nodes))),
		       updated_at = NOW()
		 RETURNING ref_count`,
		ref.Hash, ref.SizeBytes, ref.Nodes,
	).Scan(&refCount)
	if err != nil {
		return false, err
	}
	return refCount == 1, nil // ref_count==1 means we just created it
}

func (s *PostgresStore) DecrementChunkRefs(ctx context.Context, hashes []string) error {
	if len(hashes) == 0 {
		return nil
	}
	// GREATEST clamps at 0 so a double-decrement can't drive the count negative.
	_, err := s.pool.Exec(ctx,
		`UPDATE chunks SET ref_count = GREATEST(ref_count - 1, 0), updated_at = NOW()
		 WHERE hash = ANY($1)`, hashes)
	return err
}

func (s *PostgresStore) GetChunk(ctx context.Context, hash string) (*ChunkRef, error) {
	c := &ChunkRef{}
	err := s.pool.QueryRow(ctx,
		`SELECT hash, size_bytes, ref_count, nodes, verified_at FROM chunks WHERE hash=$1`, hash,
	).Scan(&c.Hash, &c.SizeBytes, &c.RefCount, &c.Nodes, &c.VerifiedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrObjectNotFound
	}
	return c, err
}

func (s *PostgresStore) ListOrphanChunks(ctx context.Context, olderThan time.Duration) ([]*ChunkRef, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT hash, size_bytes, ref_count, nodes, verified_at FROM chunks
		 WHERE ref_count <= 0 AND updated_at < NOW() - $1::interval`,
		fmt.Sprintf("%d seconds", int(olderThan.Seconds())))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*ChunkRef
	for rows.Next() {
		c := &ChunkRef{}
		if err := rows.Scan(&c.Hash, &c.SizeBytes, &c.RefCount, &c.Nodes, &c.VerifiedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *PostgresStore) DeleteChunkRef(ctx context.Context, hash string) error {
	// Guard against a race: only delete if it is still an orphan. An upload that
	// re-referenced the chunk between the GC scan and this delete bumps
	// ref_count above 0, and this WHERE clause spares it.
	_, err := s.pool.Exec(ctx, `DELETE FROM chunks WHERE hash=$1 AND ref_count <= 0`, hash)
	return err
}

func (s *PostgresStore) MarkChunkVerified(ctx context.Context, hash string, at time.Time) error {
	_, err := s.pool.Exec(ctx, `UPDATE chunks SET verified_at=$2 WHERE hash=$1`, hash, at)
	return err
}

// ---- Multipart ----

func (s *PostgresStore) CreateMultipartUpload(ctx context.Context, u *MultipartUpload) error {
	return s.pool.QueryRow(ctx,
		`INSERT INTO multipart_uploads (bucket_id, key) VALUES ($1,$2)
		 RETURNING id, status, created_at, expires_at`,
		u.BucketID, u.Key,
	).Scan(&u.ID, &u.Status, &u.CreatedAt, &u.ExpiresAt)
}

func (s *PostgresStore) GetMultipartUpload(ctx context.Context, id string) (*MultipartUpload, error) {
	u := &MultipartUpload{}
	var partsJSON []byte
	err := s.pool.QueryRow(ctx,
		`SELECT id, bucket_id, key, status, parts, created_at, expires_at FROM multipart_uploads WHERE id=$1`, id,
	).Scan(&u.ID, &u.BucketID, &u.Key, &u.Status, &partsJSON, &u.CreatedAt, &u.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrUploadNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(partsJSON, &u.Parts); err != nil {
		return nil, err
	}
	return u, nil
}

func (s *PostgresStore) AddPart(ctx context.Context, uploadID string, part Part) error {
	u, err := s.GetMultipartUpload(ctx, uploadID)
	if err != nil {
		return err
	}
	replaced := false
	for i, p := range u.Parts {
		if p.PartNumber == part.PartNumber {
			u.Parts[i] = part
			replaced = true
			break
		}
	}
	if !replaced {
		u.Parts = append(u.Parts, part)
	}
	partsJSON, err := json.Marshal(u.Parts)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `UPDATE multipart_uploads SET parts=$2 WHERE id=$1`, uploadID, partsJSON)
	return err
}

func (s *PostgresStore) CompleteMultipartUpload(ctx context.Context, id string) (*MultipartUpload, error) {
	u, err := s.GetMultipartUpload(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, err := s.pool.Exec(ctx, `UPDATE multipart_uploads SET status='completed' WHERE id=$1`, id); err != nil {
		return nil, err
	}
	u.Status = "completed"
	return u, nil
}

func (s *PostgresStore) AbortMultipartUpload(ctx context.Context, id string) error {
	ct, err := s.pool.Exec(ctx, `UPDATE multipart_uploads SET status='aborted' WHERE id=$1`, id)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return ErrUploadNotFound
	}
	return nil
}

// ---- helpers ----

type rowScanner interface {
	Scan(dest ...any) error
}

func (s *PostgresStore) scanObject(row rowScanner) (*Object, error) {
	o, err := s.scanObjectRow(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrObjectNotFound
	}
	return o, err
}

func (s *PostgresStore) scanObjectRow(row rowScanner) (*Object, error) {
	o := &Object{}
	var manifestJSON, metaJSON []byte
	if err := row.Scan(&o.ID, &o.BucketID, &o.Key, &o.VersionID, &o.SizeBytes, &o.ContentType,
		&o.ChecksumSHA, &manifestJSON, &o.IsLatest, &o.DeleteMarker, &metaJSON, &o.CreatedAt); err != nil {
		return nil, err
	}
	m, err := chunk.ParseManifest(manifestJSON)
	if err != nil {
		return nil, err
	}
	o.Manifest = m
	if len(metaJSON) > 0 {
		_ = json.Unmarshal(metaJSON, &o.Metadata)
	}
	return o, nil
}

func isUniqueViolation(err error) bool {
	// pgx surfaces SQLSTATE 23505 for unique violations. We check the message to
	// avoid importing pgconn just for the code; a production system would type-
	// assert *pgconn.PgError and compare .Code == "23505".
	return err != nil && (contains(err.Error(), "23505") || contains(err.Error(), "duplicate key"))
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
