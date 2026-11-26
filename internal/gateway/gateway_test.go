package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/1morshed1/storagemesh/internal/events"
	"github.com/1morshed1/storagemesh/internal/metadata"
	"github.com/1morshed1/storagemesh/internal/presign"
	"github.com/1morshed1/storagemesh/internal/storage"
)

// testHarness spins up N in-process storage nodes plus a gateway wired to a
// MemStore. This exercises the real replication and read paths over HTTP without
// any external service — the whole system in one test binary.
type testHarness struct {
	gw      http.Handler
	nodes   []*httptest.Server
	store   *metadata.MemStore
	cleanup func()
}

func newHarness(t *testing.T, nodeCount, repl, chunkSize int) *testHarness {
	t.Helper()
	var nodeServers []*httptest.Server
	var clusterNodes []Node
	for i := 0; i < nodeCount; i++ {
		ds, err := storage.NewDiskStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewServer(storage.NewServer(ds, "node-"+string(rune('a'+i)), nil).Handler())
		nodeServers = append(nodeServers, srv)
		clusterNodes = append(clusterNodes, Node{ID: srv.URL, BaseURL: srv.URL})
	}

	store := metadata.NewMemStore()
	cluster := NewCluster(clusterNodes, repl, nil)
	signer := presign.New("test-secret")
	gw := NewServer(store, cluster, signer, events.NoopPublisher{},
		Config{ChunkSize: chunkSize}, nil).Handler()

	return &testHarness{
		gw:    gw,
		nodes: nodeServers,
		store: store,
		cleanup: func() {
			for _, n := range nodeServers {
				n.Close()
			}
		},
	}
}

func (h *testHarness) do(t *testing.T, method, path string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, body)
	rec := httptest.NewRecorder()
	h.gw.ServeHTTP(rec, req)
	return rec
}

func TestEndToEndPutGetDelete(t *testing.T) {
	h := newHarness(t, 3, 3, 64) // tiny 64-byte chunks force multi-chunk objects
	defer h.cleanup()

	// Create bucket.
	if rec := h.do(t, "PUT", "/photos", nil); rec.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", rec.Code, rec.Body)
	}

	// Put an object spanning several chunks.
	payload := bytes.Repeat([]byte("StorageMesh! "), 50) // ~650 bytes -> ~11 chunks
	rec := h.do(t, "PUT", "/photos/dir/file.txt", bytes.NewReader(payload))
	if rec.Code != http.StatusOK {
		t.Fatalf("put object: %d %s", rec.Code, rec.Body)
	}
	wantETag := sha256Hex(payload)
	var putResp struct {
		ETag string `json:"etag"`
		Size int64  `json:"size"`
	}
	json.Unmarshal(rec.Body.Bytes(), &putResp)
	if putResp.ETag != wantETag {
		t.Fatalf("etag = %s, want %s", putResp.ETag, wantETag)
	}
	if putResp.Size != int64(len(payload)) {
		t.Fatalf("size = %d, want %d", putResp.Size, len(payload))
	}

	// Get it back and verify bytes.
	rec = h.do(t, "GET", "/photos/dir/file.txt", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("get object: %d %s", rec.Code, rec.Body)
	}
	if !bytes.Equal(rec.Body.Bytes(), payload) {
		t.Fatal("downloaded bytes differ from uploaded")
	}

	// Delete it.
	if rec := h.do(t, "DELETE", "/photos/dir/file.txt", nil); rec.Code != http.StatusNoContent {
		t.Fatalf("delete: %d %s", rec.Code, rec.Body)
	}
	if rec := h.do(t, "GET", "/photos/dir/file.txt", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("get after delete: want 404, got %d", rec.Code)
	}
}

func TestDeduplicationAcrossObjects(t *testing.T) {
	h := newHarness(t, 3, 2, 64)
	defer h.cleanup()
	h.do(t, "PUT", "/b", nil)

	payload := bytes.Repeat([]byte("identical bytes! "), 40)

	// Upload the same content under two different keys.
	h.do(t, "PUT", "/b/first", bytes.NewReader(payload))
	h.do(t, "PUT", "/b/second", bytes.NewReader(payload))

	// Every chunk should have ref_count == 2 (shared by both objects), proving
	// the second upload deduped instead of storing new bytes.
	obj, err := h.store.GetObject(context.Background(), bucketID(t, h, "b"), "first")
	if err != nil {
		t.Fatal(err)
	}
	for _, hs := range obj.Manifest.Hashes() {
		c, err := h.store.GetChunk(context.Background(), hs)
		if err != nil {
			t.Fatal(err)
		}
		if c.RefCount != 2 {
			t.Fatalf("chunk %s ref_count = %d, want 2 (dedup failed)", hs[:8], c.RefCount)
		}
	}
}

func TestReadFailoverWhenReplicaDown(t *testing.T) {
	h := newHarness(t, 3, 3, 64)
	defer h.cleanup()
	h.do(t, "PUT", "/b", nil)

	payload := bytes.Repeat([]byte("survive a node failure "), 30)
	h.do(t, "PUT", "/b/k", bytes.NewReader(payload))

	// Kill one storage node. With replication 3 the chunks live on all 3 nodes,
	// so reads must fail over to a surviving replica.
	h.nodes[0].Close()

	rec := h.do(t, "GET", "/b/k", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("get after node down: %d %s", rec.Code, rec.Body)
	}
	if !bytes.Equal(rec.Body.Bytes(), payload) {
		t.Fatal("bytes differ after failover")
	}
}

func TestMultipartUpload(t *testing.T) {
	h := newHarness(t, 3, 2, 64)
	defer h.cleanup()
	h.do(t, "PUT", "/b", nil)

	// Initiate.
	rec := h.do(t, "POST", "/b/big.bin?uploads", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("initiate: %d %s", rec.Code, rec.Body)
	}
	var init struct {
		UploadID string `json:"uploadId"`
	}
	json.Unmarshal(rec.Body.Bytes(), &init)
	if init.UploadID == "" {
		t.Fatal("no uploadId")
	}

	part1 := bytes.Repeat([]byte("A"), 200)
	part2 := bytes.Repeat([]byte("B"), 150)
	h.do(t, "PUT", "/b/big.bin?uploadId="+init.UploadID+"&partNumber=1", bytes.NewReader(part1))
	h.do(t, "PUT", "/b/big.bin?uploadId="+init.UploadID+"&partNumber=2", bytes.NewReader(part2))

	// Complete.
	rec = h.do(t, "POST", "/b/big.bin?uploadId="+init.UploadID, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("complete: %d %s", rec.Code, rec.Body)
	}

	// Download and confirm parts stitched in order.
	rec = h.do(t, "GET", "/b/big.bin", nil)
	want := append(append([]byte{}, part1...), part2...)
	if !bytes.Equal(rec.Body.Bytes(), want) {
		t.Fatalf("assembled object wrong: got %d bytes want %d", rec.Body.Len(), len(want))
	}
}

func TestPresignedURLFlow(t *testing.T) {
	// Enable auth so we can prove a presigned URL bypasses the header check.
	var nodeServers []Node
	var closers []*httptest.Server
	for i := 0; i < 3; i++ {
		ds, _ := storage.NewDiskStore(t.TempDir())
		srv := httptest.NewServer(storage.NewServer(ds, "n", nil).Handler())
		closers = append(closers, srv)
		nodeServers = append(nodeServers, Node{ID: srv.URL, BaseURL: srv.URL})
	}
	defer func() {
		for _, s := range closers {
			s.Close()
		}
	}()
	store := metadata.NewMemStore()
	signer := presign.New("shh")
	gw := NewServer(store, NewCluster(nodeServers, 2, nil), signer, events.NoopPublisher{},
		Config{ChunkSize: 64, AccessKey: "secret-key"}, nil).Handler()

	req := func(method, path string, body io.Reader, key string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, body)
		if key != "" {
			r.Header.Set("X-Access-Key", key)
		}
		rec := httptest.NewRecorder()
		gw.ServeHTTP(rec, r)
		return rec
	}

	req("PUT", "/b", nil, "secret-key")
	req("PUT", "/b/k", bytes.NewReader([]byte("hello presign")), "secret-key")

	// Without credentials, GET is forbidden.
	if rec := req("GET", "/b/k", nil, ""); rec.Code != http.StatusForbidden {
		t.Fatalf("unauth GET should be 403, got %d", rec.Code)
	}

	// Mint a presigned URL (requires the access key).
	rec := req("GET", "/b/k?presign=GET&expiry=300", nil, "secret-key")
	if rec.Code != http.StatusOK {
		t.Fatalf("mint presign: %d %s", rec.Code, rec.Body)
	}
	var pres struct {
		URL string `json:"url"`
	}
	json.Unmarshal(rec.Body.Bytes(), &pres)

	// The presigned URL works with NO credentials.
	rec = req("GET", pres.URL, nil, "")
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), []byte("hello presign")) {
		t.Fatalf("presigned GET failed: %d %s", rec.Code, rec.Body)
	}
}

// ---- helpers ----

func sha256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func bucketID(t *testing.T, h *testHarness, name string) string {
	t.Helper()
	b, err := h.store.GetBucket(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	return b.ID
}
