// Package gateway is the stateless HTTP front door of StorageMesh. It speaks an
// S3-flavoured REST API, chunks uploads, replicates chunks across storage nodes
// with quorum writes, records metadata in Postgres, and streams objects back.
//
// It holds no durable state itself: bytes live on storage nodes, metadata in the
// Store. That is what lets many gateway replicas run behind a load balancer and
// scale horizontally (the "HPA scaled" box in the architecture diagram).
package gateway

import (
	"log/slog"
	"net/http"

	"github.com/1morshed1/storagemesh/internal/chunk"
	"github.com/1morshed1/storagemesh/internal/events"
	"github.com/1morshed1/storagemesh/internal/metadata"
	"github.com/1morshed1/storagemesh/internal/presign"
)

// Config tunes a Server.
type Config struct {
	ChunkSize int    // bytes per chunk (0 => chunk.DefaultChunkSize)
	AccessKey string // shared access key required on non-presigned requests
	OwnerID   string // owner recorded on created buckets (single-tenant demo)
}

// Server wires the metadata store, storage cluster, URL signer, and event
// publisher into an http.Handler.
type Server struct {
	store     metadata.Store
	cluster   *Cluster
	signer    *presign.Signer
	publisher events.Publisher
	chunkSize int
	accessKey string
	ownerID   string
	log       *slog.Logger
}

// NewServer constructs a gateway. publisher may be events.NoopPublisher{} when
// no broker is configured.
func NewServer(store metadata.Store, cluster *Cluster, signer *presign.Signer, publisher events.Publisher, cfg Config, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = chunk.DefaultChunkSize
	}
	if publisher == nil {
		publisher = events.NoopPublisher{}
	}
	if cfg.OwnerID == "" {
		cfg.OwnerID = "00000000-0000-0000-0000-000000000000"
	}
	return &Server{
		store:     store,
		cluster:   cluster,
		signer:    signer,
		publisher: publisher,
		chunkSize: cfg.ChunkSize,
		accessKey: cfg.AccessKey,
		ownerID:   cfg.OwnerID,
		log:       log,
	}
}

// Handler builds the router. Routes are ordered from most specific to least so
// the stdlib mux resolves them unambiguously.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", s.handleHealth)

	// Bucket-level.
	mux.HandleFunc("GET /{$}", s.auth(s.handleListBuckets)) // list buckets at root
	mux.HandleFunc("PUT /{bucket}", s.auth(s.handleCreateBucket))
	mux.HandleFunc("DELETE /{bucket}", s.auth(s.handleDeleteBucket))
	mux.HandleFunc("GET /{bucket}", s.auth(s.handleListObjects))

	// Object-level. {key...} captures the full remaining path (keys contain "/").
	mux.HandleFunc("POST /{bucket}/{key...}", s.auth(s.handleMultipart)) // initiate/complete/part
	mux.HandleFunc("PUT /{bucket}/{key...}", s.handlePutObject)          // auth handled inside (presign or key)
	mux.HandleFunc("GET /{bucket}/{key...}", s.handleGetObject)          // auth handled inside (presign or key)
	mux.HandleFunc("DELETE /{bucket}/{key...}", s.auth(s.handleDeleteObject))

	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":     "ok",
		"live_nodes": s.cluster.LiveNodes(),
	})
}
