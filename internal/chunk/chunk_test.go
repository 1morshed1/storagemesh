package chunk

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"
)

// TestChunkerBoundaries verifies the two tricky cases that break naive
// chunkers: a stream that ends exactly on a chunk boundary, and one that ends
// with a short final chunk.
func TestChunkerBoundaries(t *testing.T) {
	cases := map[string]struct {
		size       int
		chunkSize  int
		wantChunks int
		lastSize   int
	}{
		"empty stream":            {size: 0, chunkSize: 4, wantChunks: 0, lastSize: 0},
		"exact single boundary":   {size: 4, chunkSize: 4, wantChunks: 1, lastSize: 4},
		"exact multiple boundary": {size: 8, chunkSize: 4, wantChunks: 2, lastSize: 4},
		"short final chunk":       {size: 10, chunkSize: 4, wantChunks: 3, lastSize: 2},
		"smaller than one chunk":  {size: 3, chunkSize: 4, wantChunks: 1, lastSize: 3},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			data := bytes.Repeat([]byte{0xAB}, tc.size)
			ch := NewChunker(bytes.NewReader(data), tc.chunkSize)

			var got []Chunk
			for {
				c, err := ch.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				got = append(got, c)
			}

			if len(got) != tc.wantChunks {
				t.Fatalf("chunk count = %d, want %d", len(got), tc.wantChunks)
			}
			for i, c := range got {
				if c.Index != i {
					t.Errorf("chunk %d has Index %d", i, c.Index)
				}
			}
			if tc.wantChunks > 0 && got[len(got)-1].Size() != tc.lastSize {
				t.Errorf("last chunk size = %d, want %d", got[len(got)-1].Size(), tc.lastSize)
			}
		})
	}
}

// TestChunkerReassembly proves chunks concatenate back into the original bytes
// and that identical content produces identical hashes (the dedup guarantee).
func TestChunkerReassembly(t *testing.T) {
	orig := make([]byte, 5000)
	if _, err := rand.Read(orig); err != nil {
		t.Fatal(err)
	}

	collect := func() ([]Chunk, []byte) {
		ch := NewChunker(bytes.NewReader(orig), 1024)
		var chunks []Chunk
		var reassembled []byte
		for {
			c, err := ch.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			chunks = append(chunks, c)
			reassembled = append(reassembled, c.Data...)
		}
		return chunks, reassembled
	}

	a, ra := collect()
	b, _ := collect()

	if !bytes.Equal(ra, orig) {
		t.Fatal("reassembled bytes differ from original")
	}
	if len(a) != len(b) {
		t.Fatalf("chunk counts differ across runs: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].Hash != b[i].Hash {
			t.Errorf("chunk %d hash not deterministic", i)
		}
	}
}

// aliasingReader hands out one byte per Read to force io.ReadFull to loop,
// exercising the short-read handling.
type oneByteReader struct{ data []byte }

func (r *oneByteReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	p[0] = r.data[0]
	r.data = r.data[1:]
	return 1, nil
}

// TestChunkerHandlesPartialReads ensures we assemble full-size chunks even when
// the underlying reader dribbles bytes out one at a time.
func TestChunkerHandlesPartialReads(t *testing.T) {
	data := bytes.Repeat([]byte{1}, 10)
	ch := NewChunker(&oneByteReader{data: data}, 4)

	c1, err := ch.Next()
	if err != nil || c1.Size() != 4 {
		t.Fatalf("chunk 1: size=%d err=%v", c1.Size(), err)
	}
	c2, _ := ch.Next()
	if c2.Size() != 4 {
		t.Fatalf("chunk 2 size = %d, want 4", c2.Size())
	}
	c3, _ := ch.Next()
	if c3.Size() != 2 {
		t.Fatalf("chunk 3 size = %d, want 2", c3.Size())
	}
}

func TestManifestRoundTrip(t *testing.T) {
	m := NewManifest(4)
	m.Append(Chunk{Index: 0, Hash: "aa", Data: []byte{1, 2, 3, 4}})
	m.Append(Chunk{Index: 1, Hash: "bb", Data: []byte{5, 6}})

	if m.TotalSize() != 6 {
		t.Fatalf("total size = %d, want 6", m.TotalSize())
	}
	b, err := m.JSON()
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseManifest(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Chunks) != 2 || got.Chunks[1].Hash != "bb" {
		t.Fatalf("round trip lost data: %+v", got)
	}
}
