package chunk

import "encoding/json"

// Manifest is the ordered recipe for reassembling an object from chunks. It is
// stored as JSONB in the objects table (column chunk_manifest) and is the only
// thing we need to stream an object back: read each chunk hash in order, fetch
// the bytes from a storage node, concatenate.
//
// It is deliberately a plain serializable struct with no behaviour tied to a
// database, so the same type flows from the chunker, into Postgres, and back
// out to the read path unchanged.
type Manifest struct {
	ChunkSize int             `json:"chunk_size"`
	Chunks    []ManifestEntry `json:"chunks"`
}

// ManifestEntry records one chunk's position, content hash, and byte size.
type ManifestEntry struct {
	Index int    `json:"index"`
	Hash  string `json:"hash"`
	Size  int    `json:"size"`
}

// NewManifest returns an empty manifest for the given chunk size.
func NewManifest(chunkSize int) *Manifest {
	return &Manifest{ChunkSize: chunkSize, Chunks: []ManifestEntry{}}
}

// Append records a chunk in the manifest, preserving order.
func (m *Manifest) Append(c Chunk) {
	m.Chunks = append(m.Chunks, ManifestEntry{
		Index: c.Index,
		Hash:  c.Hash,
		Size:  c.Size(),
	})
}

// TotalSize returns the reassembled object's size in bytes.
func (m *Manifest) TotalSize() int64 {
	var total int64
	for _, e := range m.Chunks {
		total += int64(e.Size)
	}
	return total
}

// Hashes returns the chunk hashes in order. Useful for ref-count bookkeeping.
func (m *Manifest) Hashes() []string {
	out := make([]string, len(m.Chunks))
	for i, e := range m.Chunks {
		out[i] = e.Hash
	}
	return out
}

// JSON serializes the manifest for storage.
func (m *Manifest) JSON() ([]byte, error) { return json.Marshal(m) }

// ParseManifest deserializes a manifest read back from storage.
func ParseManifest(b []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
