// Command storagenode runs one storage node: an HTTP daemon that stores chunk
// bytes on local disk, addressed by content hash.
//
// It is configured entirely by environment variables so it drops cleanly into a
// container / Kubernetes pod:
//
//	NODE_ID    logical id reported on /healthz          (default: hostname)
//	DATA_DIR   root directory for chunk files           (default: ./data)
//	LISTEN     address to bind                           (default: :8081)
//
// Run several with different DATA_DIR and LISTEN to form the storage ring.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/1morshed1/storagemesh/internal/storage"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	nodeID := env("NODE_ID", hostname())
	dataDir := env("DATA_DIR", "./data")
	listen := env("LISTEN", ":8081")

	store, err := storage.NewDiskStore(dataDir)
	if err != nil {
		log.Error("init disk store", "err", err)
		os.Exit(1)
	}

	srv := &http.Server{
		Addr:    listen,
		Handler: storage.NewServer(store, nodeID, log).Handler(),
		// No ReadTimeout: chunk uploads stream and can legitimately take a while.
		// WriteTimeout likewise omitted so large GETs are not cut off. A real
		// deployment would set generous, not zero, values.
	}

	// Graceful shutdown: on SIGINT/SIGTERM, stop accepting new requests and let
	// in-flight chunk transfers finish before exiting.
	go func() {
		log.Info("storage node listening", "node", nodeID, "addr", listen, "data", dataDir)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("serve", "err", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Info("shutting down", "node", nodeID)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Error("graceful shutdown failed", "err", err)
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "storage-node"
	}
	return h
}
