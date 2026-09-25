## Project 6: Distributed Object Storage Service (Mini-S3)

### Concept & Why It Matters

You are building a simplified S3-compatible object storage service. Users upload files via an HTTP API, and the system chunks the files, replicates the chunks across multiple storage nodes, and serves them back. It supports versioning, pre-signed URLs, multipart uploads, and lifecycle policies.

This is one of the most technically demanding projects on this list. It forces you to solve problems that most application developers never encounter: content-addressed storage, consistent hashing for data distribution, replication consistency, and garbage collection of orphaned chunks. If you can explain this system in an interview, you are demonstrating systems-level thinking that is extremely rare in junior-to-mid-level candidates.

### Architecture Diagram

```
  Clients                                                                     
   │                                                                          
   │  S3-compatible REST API                                                  
   │  PUT /bucket/key                                                         
   │  GET /bucket/key                                                         
   ▼                                                                          
┌──────────────────────────────────────────────────────────────────────────────┐
│                        KUBERNETES CLUSTER                                    │
│                                                                              │
│  ┌───────────────────────────────────────────┐                               │
│  │  Go API Gateway                           │                              │
│  │                                           │                              │
│  │  - Authenticates requests (HMAC-SHA256)   │                              │
│  │  - Routes to correct storage nodes        │                              │
│  │  - Handles multipart upload assembly     │                              │
│  │  - Generates pre-signed URLs             │                              │
│  │  - HPA scaled                            │                              │
│  └──────────┬────────────────────────────────┘                               │
│             │                                                                │
│             ▼                                                                │
│  ┌───────────────────────────────────────────┐                               │
│  │  PostgreSQL (Metadata Store)              │                              │
│  │                                           │                              │
│  │  buckets (id, name, owner, created_at)    │                              │
│  │  objects (bucket_id, key, version_id,     │                              │
│  │           size, content_type, checksum,   │                              │
│  │           chunk_manifest, created_at,     │                              │
│  │           delete_marker)                  │                              │
│  │  multipart_uploads (id, bucket_id, key,   │                             │
│  │           parts[], status)                │                              │
│  └───────────────────────────────────────────┘                               │
│             │                                                                │
│             │  Chunk placement via consistent hashing                       │
│             ▼                                                                │
│  ┌──────────────────────────────────────────────────────────┐                │
│  │  Storage Node Ring (Go)                                  │               │
│  │                                                          │               │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐               │               │
│  │  │  Node 1  │  │  Node 2  │  │  Node 3  │  ...          │               │
│  │  │  /data/  │  │  /data/  │  │  /data/  │               │               │
│  │  │  chunks/ │  │  chunks/ │  │  chunks/ │               │               │
│  │  └──────────┘  └──────────┘  └──────────┘               │               │
│  │                                                          │               │
│  │  - Each chunk stored by content hash (SHA-256)          │               │
│  │  - Replication factor: 3 (each chunk on 3 nodes)        │               │
│  │  - Consistent hashing determines primary node           │               │
│  │  - Nodes gossip health status to each other             │               │
│  └──────────────────────────────────────────────────────────┘                │
│                                                                              │
│  ┌───────────────────────────────────────────┐                               │
│  │  Background Workers (Go)                  │                              │
│  │                                           │                              │
│  │  - Garbage Collector: deletes orphaned    │                              │
│  │    chunks with no object reference        │                              │
│  │  - Rebalancer: redistributes chunks       │                              │
│  │    when nodes join/leave the ring         │                              │
│  │  - Integrity Checker: periodic SHA-256    │                              │
│  │    verification of stored chunks          │                              │
│  │  - Lifecycle Manager: applies expiration  │                              │
│  │    and transition rules                   │                              │
│  └───────────────────────────────────────────┘                               │
│                                                                              │
│  Kafka: object-events topic (upload, delete, lifecycle transitions)          │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Tech Stack Breakdown

| Layer | Technology | Why This Choice |
|---|---|---|
| API Gateway | Go + Gin | Needs to handle large file uploads efficiently. Go's `io.Reader` streaming lets you process multi-gigabyte uploads without loading the entire file into memory. |
| Metadata Store | PostgreSQL | Object metadata (keys, versions, chunk manifests) is relational. Postgres handles it well with proper indexing. |
| Storage Nodes | Go + local filesystem | Each node is a Go service that stores raw byte chunks on its local disk, indexed by content hash. In production you would use raw block devices; for a portfolio project, the local filesystem is fine. |
| Data Distribution | Consistent Hashing (custom) | Determines which nodes store which chunks. When a node is added or removed, only 1/N of the data needs to move (unlike modular hashing where everything reshuffles). |
| Replication | Quorum writes (W=2, R=2 of N=3) | Write succeeds when 2 of 3 replicas acknowledge. Read succeeds when 2 of 3 replicas agree. This guarantees consistency even if one node is down. |
| Health | Gossip Protocol (custom or SWIM) | Nodes detect each other's liveness without a central coordinator. If Node 2 stops responding, Nodes 1 and 3 detect it within seconds and stop routing traffic to it. |
| Event Streaming | Apache Kafka | Object lifecycle events (created, deleted, transitioned to cold storage) are published for downstream consumers (analytics, billing, CDN cache invalidation). |

### Phase-by-Phase Build Plan

#### Phase 1: Metadata Service and Basic CRUD

**Database Schema:**

```sql
CREATE TABLE buckets (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        VARCHAR(63) UNIQUE NOT NULL,     -- S3 bucket naming rules
    owner_id    UUID NOT NULL,
    versioning  BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE objects (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bucket_id       UUID NOT NULL REFERENCES buckets(id),
    key             TEXT NOT NULL,                -- the object path (e.g., "photos/vacation/img001.jpg")
    version_id      UUID NOT NULL DEFAULT gen_random_uuid(),
    size_bytes      BIGINT NOT NULL,
    content_type    TEXT NOT NULL DEFAULT 'application/octet-stream',
    checksum_sha256 TEXT NOT NULL,                -- integrity check
    chunk_manifest  JSONB NOT NULL,               -- ordered list of chunk hashes
    is_latest       BOOLEAN NOT NULL DEFAULT TRUE,
    delete_marker   BOOLEAN NOT NULL DEFAULT FALSE,
    metadata        JSONB,                        -- user-defined metadata headers
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE(bucket_id, key, version_id)
);

CREATE INDEX idx_objects_bucket_key ON objects (bucket_id, key, is_latest)
    WHERE is_latest = TRUE AND delete_marker = FALSE;

CREATE TABLE chunks (
    hash            TEXT PRIMARY KEY,             -- SHA-256 of chunk content
    size_bytes      INTEGER NOT NULL,
    ref_count       INTEGER NOT NULL DEFAULT 1,   -- number of objects referencing this chunk
    nodes           TEXT[] NOT NULL,               -- which storage nodes hold this chunk
    verified_at     TIMESTAMPTZ                    -- last integrity check
);

CREATE TABLE multipart_uploads (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bucket_id       UUID NOT NULL REFERENCES buckets(id),
    key             TEXT NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'in_progress',  -- in_progress, completed, aborted
    parts           JSONB NOT NULL DEFAULT '[]',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at      TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '7 days'
);
```

**The Chunk Manifest:**

When a 100MB file is uploaded, it is split into fixed-size chunks (default: 8MB). Each chunk is independently hashed, stored, and replicated. The object's `chunk_manifest` records the ordered list of chunk hashes:

```json
{
  "chunk_size": 8388608,
  "chunks": [
    {"index": 0, "hash": "a3f2b8c...", "size": 8388608},
    {"index": 1, "hash": "7d1e4f9...", "size": 8388608},
    {"index": 2, "hash": "b9c0a2d...", "size": 8388608},
    {"index": 3, "hash": "e5f6g7h...", "size": 4194304}
  ]
}
```

Content-addressed storage means that if two different objects contain the same chunk (e.g., two slightly different versions of a large file), the identical chunk is stored only once. The `ref_count` on the `chunks` table tracks how many objects reference each chunk. Garbage collection only deletes a chunk when its `ref_count` drops to zero.

#### Phase 2: Consistent Hashing and Storage Nodes

**Consistent Hashing Implementation:**

```go
type HashRing struct {
    mu           sync.RWMutex
    nodes        map[string]*StorageNode   // node_id -> node
    ring         []uint32                   // sorted hash positions
    nodeMap      map[uint32]string          // hash position -> node_id
    vnodes       int                        // virtual nodes per physical node (150 is a good default)
    replication  int                        // replication factor (3)
}

func (r *HashRing) GetNodes(chunkHash string) []string {
    r.mu.RLock()
    defer r.mu.RUnlock()

    hash := crc32.ChecksumIEEE([]byte(chunkHash))
    idx := sort.Search(len(r.ring), func(i int) bool {
        return r.ring[i] >= hash
    })
    if idx >= len(r.ring) {
        idx = 0
    }

    // Walk the ring to find `replication` distinct physical nodes
    nodes := make([]string, 0, r.replication)
    seen := make(map[string]bool)
    for len(nodes) < r.replication && len(seen) < len(r.nodes) {
        nodeID := r.nodeMap[r.ring[idx%len(r.ring)]]
        if !seen[nodeID] {
            nodes = append(nodes, nodeID)
            seen[nodeID] = true
        }
        idx++
    }
    return nodes
}
```

**Storage Node gRPC Service:**

```protobuf
service StorageNode {
    rpc PutChunk(stream ChunkData) returns (PutChunkResponse);
    rpc GetChunk(GetChunkRequest) returns (stream ChunkData);
    rpc DeleteChunk(DeleteChunkRequest) returns (DeleteChunkResponse);
    rpc VerifyChunk(VerifyChunkRequest) returns (VerifyChunkResponse);
    rpc ListChunks(ListChunksRequest) returns (ListChunksResponse);
}
```

Each storage node stores chunks on its local filesystem using the content hash as the filename, organized into a two-level directory structure to avoid having millions of files in a single directory:

```
/data/chunks/
├── a3/
│   └── f2b8c9d1e4f5a6b7c8d9e0f1a2b3c4d5.chunk
├── 7d/
│   └── 1e4f9a2b3c4d5e6f7a8b9c0d1e2f3a4b.chunk
└── ...
```

#### Phase 3: Upload Flow (Putting It Together)

The full upload flow for a 100MB file:

1. Client sends `PUT /bucket/key` with the file body and a `Content-SHA256` header.
2. The API Gateway streams the request body through a chunker that splits it into 8MB pieces.
3. For each chunk, the Gateway computes the SHA-256 hash.
4. The Gateway consults the `chunks` table. If the hash already exists (deduplication), it increments `ref_count` and skips storage. If not, it continues.
5. The Gateway queries the consistent hash ring to determine which 3 nodes should store this chunk.
6. The Gateway streams the chunk data to all 3 nodes in parallel. It waits for at least 2 acknowledgments (quorum write) before considering the chunk stored.
7. After all chunks are stored, the Gateway writes the object metadata (including the chunk manifest) to Postgres.
8. The Gateway publishes an `object.created` event to Kafka.
9. The Gateway returns `200 OK` with the object's `ETag` (the SHA-256 of the complete file).

**Critical: Streaming, Not Buffering:**

The Gateway must never load the entire file into memory. It processes the upload as a stream:

```go
func (h *Handler) PutObject(c *gin.Context) {
    chunker := NewChunker(c.Request.Body, 8*1024*1024) // 8MB chunks
    manifest := ChunkManifest{}

    for {
        chunk, err := chunker.Next()
        if err == io.EOF {
            break
        }
        hash := sha256.Sum256(chunk)
        hashHex := hex.EncodeToString(hash[:])

        // Check for dedup
        if h.chunkStore.Exists(ctx, hashHex) {
            h.chunkStore.IncrementRefCount(ctx, hashHex)
        } else {
            nodes := h.ring.GetNodes(hashHex)
            if err := h.replicateChunk(ctx, chunk, hashHex, nodes); err != nil {
                c.JSON(500, gin.H{"error": "chunk replication failed"})
                return
            }
        }

        manifest.Append(hashHex, len(chunk))
    }

    // Save object metadata
    h.objectStore.Create(ctx, bucketID, key, manifest)
}
```

#### Phase 4: Background Workers

**Garbage Collector:**

Runs every hour. Finds chunks with `ref_count = 0` that have been unreferenced for more than 24 hours (grace period to avoid race conditions with in-progress uploads). Sends `DeleteChunk` RPCs to all storage nodes holding the chunk, then deletes the metadata row.

**Rebalancer:**

When a new storage node joins the ring, some chunks need to move. The rebalancer identifies chunks whose hash ring assignment has changed (the new node is now in their top-3) and copies them to the new node. After copying, it removes the chunk from the node that is no longer responsible.

**Integrity Checker:**

Runs continuously in the background. For each chunk, sends a `VerifyChunk` RPC to each storage node. The node re-computes the SHA-256 of the stored bytes and compares it to the expected hash. If corruption is detected (bit rot, disk error), the checker copies a healthy replica from another node.

#### Phase 5: Pre-Signed URLs

Pre-signed URLs let users upload or download objects without authentication credentials. The URL contains a cryptographic signature that expires after a configured duration:

```go
func (s *Signer) GeneratePresignedURL(method, bucket, key string, expiry time.Duration) string {
    expires := time.Now().Add(expiry).Unix()
    stringToSign := fmt.Sprintf("%s\n%s\n%s\n%d", method, bucket, key, expires)
    
    mac := hmac.New(sha256.New, []byte(s.secretKey))
    mac.Write([]byte(stringToSign))
    signature := hex.EncodeToString(mac.Sum(nil))
    
    return fmt.Sprintf("https://storage.yourdomain.com/%s/%s?expires=%d&signature=%s",
        bucket, key, expires, signature)
}
```

### Interview Talking Points Specific to This Project

- "How do you handle a storage node failure during a write?" — Quorum writes. If 2 of 3 nodes acknowledge, the write succeeds. The failed node receives the chunk later via the rebalancer.
- "What if a chunk is corrupted?" — Integrity checker detects it via periodic SHA-256 verification. The corrupted copy is replaced from a healthy replica.
- "How does deduplication work across objects?" — Content-addressed storage. Chunks are identified by their SHA-256 hash. Two objects containing the same 8MB chunk share the same physical storage. Reference counting tracks usage.
- "What happens when you add a fourth storage node?" — Consistent hashing ensures only about 25% of chunks need to move (1/N). The rebalancer handles this automatically.

---
---