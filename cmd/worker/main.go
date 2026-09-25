// Command worker runs the StorageMesh background maintenance daemon: the garbage
// collector, integrity checker, and rebalancer. It is deployed separately from
// the gateway so maintenance load never competes with request-serving capacity,
// and so you can scale / restart it independently.
//
// Configuration (environment):
//
//	DATABASE_URL         Postgres DSN; empty => in-memory (demo only, useless here
//	                     because it shares no state with the gateway process)
//	STORAGE_NODES        comma-separated node base URLs (required)
//	REPLICATION          replicas per chunk               (default 3)
//	GC_INTERVAL          garbage collector period         (default 1h)
//	GC_GRACE             orphan grace before deletion      (default 24h)
//	INTEGRITY_INTERVAL   integrity check period           (default 6h)
//	REBALANCE_INTERVAL   rebalance period                 (default 15m)
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/1morshed1/storagemesh/internal/gateway"
	"github.com/1morshed1/storagemesh/internal/metadata"
	"github.com/1morshed1/storagemesh/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dsn := env("DATABASE_URL", "")
	var store metadata.Store
	var err error
	if dsn == "" {
		log.Warn("DATABASE_URL empty: using in-memory store (GC will see no shared metadata)")
		store = metadata.NewMemStore()
	} else {
		store, err = metadata.NewPostgresStore(ctx, dsn)
		if err != nil {
			log.Error("connect metadata store", "err", err)
			os.Exit(1)
		}
	}
	defer store.Close()

	nodeURLs := splitCSV(env("STORAGE_NODES", ""))
	if len(nodeURLs) == 0 {
		log.Error("STORAGE_NODES is required")
		os.Exit(1)
	}
	nodes := make([]gateway.Node, 0, len(nodeURLs))
	for _, u := range nodeURLs {
		nodes = append(nodes, gateway.Node{ID: u, BaseURL: u})
	}
	cluster := gateway.NewCluster(nodes, envInt("REPLICATION", 3), log)
	cluster.StartHealthChecks(ctx, 10*time.Second)

	gc := worker.NewGarbageCollector(store, cluster, envDur("GC_GRACE", 24*time.Hour), log)
	ic := worker.NewIntegrityChecker(store, cluster, log)
	rb := worker.NewRebalancer(cluster, log)

	go gc.Run(ctx, envDur("GC_INTERVAL", time.Hour))
	go ic.Run(ctx, envDur("INTEGRITY_INTERVAL", 6*time.Hour))
	go rb.Run(ctx, envDur("REBALANCE_INTERVAL", 15*time.Minute))

	log.Info("worker daemon started", "nodes", nodeURLs)
	<-ctx.Done()
	log.Info("worker daemon shutting down")
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

func envDur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
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
