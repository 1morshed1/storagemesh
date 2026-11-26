// Command gateway runs the StorageMesh API gateway: the stateless HTTP front
// door that chunks uploads, replicates chunks across storage nodes, records
// metadata, and serves objects back.
//
// Configuration (all via environment, container-friendly):
//
//	LISTEN          bind address                         (default :8080)
//	DATABASE_URL    Postgres DSN; empty => in-memory store (demo only)
//	STORAGE_NODES   comma-separated node base URLs        (required)
//	REPLICATION     replicas per chunk                    (default 3)
//	CHUNK_SIZE      bytes per chunk                       (default 8 MiB)
//	ACCESS_KEY      shared access key; empty => auth off
//	PRESIGN_SECRET  HMAC secret for pre-signed URLs       (default: ACCESS_KEY)
//	KAFKA_BROKERS   comma-separated brokers; empty => events disabled
//	KAFKA_TOPIC     event topic                           (default object-events)
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/1morshed1/storagemesh/internal/events"
	"github.com/1morshed1/storagemesh/internal/gateway"
	"github.com/1morshed1/storagemesh/internal/metadata"
	"github.com/1morshed1/storagemesh/internal/presign"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// --- Metadata store ---
	store, err := buildStore(ctx, log)
	if err != nil {
		log.Error("init metadata store", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	// --- Storage cluster ---
	nodeURLs := splitCSV(env("STORAGE_NODES", ""))
	if len(nodeURLs) == 0 {
		log.Error("STORAGE_NODES is required (comma-separated node base URLs)")
		os.Exit(1)
	}
	nodes := make([]gateway.Node, 0, len(nodeURLs))
	for _, u := range nodeURLs {
		nodes = append(nodes, gateway.Node{ID: u, BaseURL: u})
	}
	repl := envInt("REPLICATION", 3)
	cluster := gateway.NewCluster(nodes, repl, log)
	cluster.StartHealthChecks(ctx, 10*time.Second)

	// --- Event publisher ---
	publisher := buildPublisher(log)
	defer publisher.Close()

	// --- Signer ---
	presignSecret := env("PRESIGN_SECRET", env("ACCESS_KEY", "insecure-dev-secret"))
	signer := presign.New(presignSecret)

	// --- Gateway server ---
	srv := &http.Server{
		Addr: env("LISTEN", ":8080"),
		Handler: gateway.NewServer(store, cluster, signer, publisher, gateway.Config{
			ChunkSize: envInt("CHUNK_SIZE", 0),
			AccessKey: env("ACCESS_KEY", ""),
		}, log).Handler(),
	}

	go func() {
		log.Info("gateway listening", "addr", srv.Addr, "nodes", nodeURLs, "replication", repl)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("serve", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down gateway")
	sctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = srv.Shutdown(sctx)
}

// buildStore returns a Postgres store if DATABASE_URL is set, else an in-memory
// store (demo only — no durability).
func buildStore(ctx context.Context, log *slog.Logger) (metadata.Store, error) {
	dsn := env("DATABASE_URL", "")
	if dsn == "" {
		log.Warn("DATABASE_URL empty: using in-memory metadata store (NOT durable)")
		return metadata.NewMemStore(), nil
	}
	return metadata.NewPostgresStore(ctx, dsn)
}

// buildPublisher returns a Kafka publisher if brokers are configured, else a
// no-op publisher so the gateway runs fine without Kafka.
func buildPublisher(log *slog.Logger) events.Publisher {
	brokers := splitCSV(env("KAFKA_BROKERS", ""))
	if len(brokers) == 0 {
		log.Info("KAFKA_BROKERS empty: object events disabled")
		return events.NoopPublisher{}
	}
	return events.NewKafkaPublisher(brokers, env("KAFKA_TOPIC", "object-events"), log)
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func splitCSV(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
