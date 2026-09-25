package storage

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
)

// Server exposes a DiskStore over HTTP so the gateway and workers can move chunk
// bytes to/from this node. The API is deliberately tiny and REST-ish:
//
//	PUT    /chunks/{hash}          store bytes (body = chunk), verifies hash
//	GET    /chunks/{hash}          stream bytes back
//	HEAD   /chunks/{hash}          existence check (no body)
//	DELETE /chunks/{hash}          remove chunk (idempotent)
//	GET    /chunks/{hash}/verify   recompute + compare hash (integrity)
//	GET    /chunks                 list all stored hashes (JSON)
//	GET    /healthz                liveness probe
//
// We use net/http's stdlib router (Go 1.22+ pattern matching) — no framework
// needed on the node because there is no auth, no chunking, just byte movement.
type Server struct {
	store  *DiskStore
	nodeID string
	log    *slog.Logger
}

// NewServer builds a node HTTP server backed by store. nodeID is reported on
// /healthz so operators (and the gateway) can confirm which node answered.
func NewServer(store *DiskStore, nodeID string, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{store: store, nodeID: nodeID, log: log}
}

// Handler returns the http.Handler with all routes registered.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /chunks", s.handleList)
	// The {hash} wildcard + explicit methods give us clean routing without a
	// framework. Note the /verify route is registered before the bare {hash}
	// so it is matched first.
	mux.HandleFunc("GET /chunks/{hash}/verify", s.handleVerify)
	mux.HandleFunc("PUT /chunks/{hash}", s.handlePut)
	mux.HandleFunc("GET /chunks/{hash}", s.handleGet)
	mux.HandleFunc("HEAD /chunks/{hash}", s.handleHead)
	mux.HandleFunc("DELETE /chunks/{hash}", s.handleDelete)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "node": s.nodeID})
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	hash := r.PathValue("hash")
	defer r.Body.Close()

	n, err := s.store.Put(hash, r.Body)
	switch {
	case errors.Is(err, ErrHashMismatch):
		// The bytes did not match the claimed hash: client/transport corruption.
		http.Error(w, "content hash mismatch", http.StatusBadRequest)
		return
	case err != nil:
		s.log.Error("put chunk", "hash", hash, "err", err)
		http.Error(w, "store failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"hash": hash, "size": n})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	hash := r.PathValue("hash")
	rc, err := s.store.Get(hash)
	if errors.Is(err, ErrNotFound) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "read failed", http.StatusInternalServerError)
		return
	}
	defer rc.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	// io.Copy streams straight from disk to the socket — no full-chunk buffer.
	if _, err := io.Copy(w, rc); err != nil {
		s.log.Warn("get chunk stream interrupted", "hash", hash, "err", err)
	}
}

func (s *Server) handleHead(w http.ResponseWriter, r *http.Request) {
	if s.store.Exists(r.PathValue("hash")) {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Delete(r.PathValue("hash")); err != nil {
		http.Error(w, "delete failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleVerify(w http.ResponseWriter, r *http.Request) {
	hash := r.PathValue("hash")
	err := s.store.Verify(hash)
	switch {
	case errors.Is(err, ErrNotFound):
		http.Error(w, "not found", http.StatusNotFound)
	case errors.Is(err, ErrHashMismatch):
		// 422: the chunk exists but its bytes are corrupt. The integrity checker
		// keys off this specific status to trigger a repair from a good replica.
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"hash": hash, "healthy": false})
	case err != nil:
		http.Error(w, "verify failed", http.StatusInternalServerError)
	default:
		writeJSON(w, http.StatusOK, map[string]any{"hash": hash, "healthy": true})
	}
}

func (s *Server) handleList(w http.ResponseWriter, _ *http.Request) {
	hashes, err := s.store.List()
	if err != nil {
		http.Error(w, "list failed", http.StatusInternalServerError)
		return
	}
	if hashes == nil {
		hashes = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"node": s.nodeID, "chunks": hashes})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
