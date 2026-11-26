// Package chunk splits a byte stream into fixed-size pieces ("chunks") and
// computes each chunk's SHA-256 content hash.
//
// # WHY THIS EXISTS
//
// StorageMesh is a content-addressed object store. Instead of storing a whole
// file as one blob, we cut it into chunks and address each chunk by the hash of
// its own bytes. That single idea buys us three things:
//
//  1. Deduplication. If two objects contain an identical 8 MiB region, both
//     end up pointing at the same chunk hash, so we store those bytes once.
//  2. Parallel replication. Each chunk is independent, so we can push chunks to
//     storage nodes concurrently instead of streaming one giant file serially.
//  3. Integrity. The hash IS the address, so a corrupted chunk can be detected
//     by re-hashing it and noticing the bytes no longer match the name.
//
// # STREAMING, NOT BUFFERING
//
// The gateway may receive multi-gigabyte uploads. We must never hold a whole
// file in memory. The Chunker reads from an io.Reader and yields one chunk at a
// time, so peak memory is roughly one chunk (plus buffers), regardless of file
// size. This is the reason Next() returns chunks lazily instead of returning a
// []Chunk of the entire file.
package chunk

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
)

// DefaultChunkSize is 8 MiB, matching the spec. It is a trade-off:
//   - Smaller chunks => better dedup granularity, more metadata rows, more RPCs.
//   - Larger chunks  => less overhead, worse dedup, bigger memory per chunk.
//
// 8 MiB is the industry-ish sweet spot (S3 multipart minimum part size is 5 MiB).
const DefaultChunkSize = 8 * 1024 * 1024

// Chunk is one piece of an object's byte stream.
type Chunk struct {
	Index int    // 0-based position of this chunk within the object
	Hash  string // hex-encoded SHA-256 of Data
	Data  []byte // the raw bytes; length <= chunk size (last chunk may be short)
}

// Size returns the byte length of the chunk's data.
func (c Chunk) Size() int { return len(c.Data) }

// Chunker reads an underlying stream and produces fixed-size chunks on demand.
//
// It is NOT safe for concurrent use: call Next() from a single goroutine. The
// concurrency in StorageMesh happens *after* chunking, when we fan out each
// returned chunk to multiple storage nodes.
type Chunker struct {
	r         io.Reader
	chunkSize int
	index     int
	buf       []byte
}

// NewChunker wraps r. If chunkSize <= 0, DefaultChunkSize is used.
func NewChunker(r io.Reader, chunkSize int) *Chunker {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	return &Chunker{
		r:         r,
		chunkSize: chunkSize,
		buf:       make([]byte, chunkSize),
	}
}

// Next returns the next chunk. When the stream is exhausted it returns
// (zero Chunk, io.EOF). Any other error from the underlying reader is returned
// as-is.
//
// IMPLEMENTATION NOTE: we use io.ReadFull with a reused buffer. A single Read()
// call is allowed to return fewer bytes than requested even when the stream is
// not finished (that is legal per the io.Reader contract), which would produce
// wrongly-sized chunks and break dedup. io.ReadFull keeps reading until the
// buffer is full or the stream truly ends, giving us stable chunk boundaries.
func (c *Chunker) Next() (Chunk, error) {
	n, err := io.ReadFull(c.r, c.buf)

	// io.ReadFull reports a short final read as ErrUnexpectedEOF, and a stream
	// that ended exactly on a boundary (n == 0) as EOF. Both mean "done", but
	// ErrUnexpectedEOF still carries a valid partial chunk we must emit.
	switch {
	case err == io.EOF:
		// Clean end, nothing left to emit.
		return Chunk{}, io.EOF
	case err == io.ErrUnexpectedEOF:
		// Final short chunk: fall through and emit the n bytes we got.
	case err != nil:
		return Chunk{}, fmt.Errorf("chunk: read: %w", err)
	}

	// Copy out only the bytes we filled. We must copy because c.buf is reused on
	// the next call; handing the caller a slice of c.buf would alias mutable
	// memory and corrupt an already-returned chunk.
	data := make([]byte, n)
	copy(data, c.buf[:n])

	sum := sha256.Sum256(data)
	ch := Chunk{
		Index: c.index,
		Hash:  hex.EncodeToString(sum[:]),
		Data:  data,
	}
	c.index++
	return ch, nil
}

// HashBytes is a small helper returning the hex SHA-256 of b. Used wherever we
// need a content hash outside the streaming path (e.g. verifying a stored chunk).
func HashBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
