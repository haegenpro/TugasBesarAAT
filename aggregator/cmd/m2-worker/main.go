package main

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/ingest"
	"aggregator/internal/platform"
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	kind := os.Getenv("WORKER_KIND")
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", kind+"-processor"))
	flush := platform.Init(kind + "-processor")
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = flush(ctx)
	}()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	s, e := ingest.New(ctx, os.Getenv("DATABASE_URL"))
	if e != nil {
		slog.Error("worker configuration failed", "error", e)
		os.Exit(1)
	}
	defer s.Pool.Close()
	registry := avrowire.NewClient(os.Getenv("SCHEMA_REGISTRY_URL"))
	registry.SetCache(s)
	cfg := ingest.WorkerConfig{Kind: kind, NATSURL: os.Getenv("NATS_URL"), CanonicalURL: os.Getenv("AGGREGATOR_BASE_URL"), CanonicalToken: os.Getenv("CANONICAL_SERVICE_TOKEN"), Lateness: platform.Duration("SATELLITE_ALLOWED_LATENESS", 30*time.Minute)}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); s.RunWorker(ctx, cfg, registry) }()
	wg.Add(1)
	go func() { defer wg.Done(); s.RunMetrics(ctx) }()
	if kind != "field" {
		wg.Add(1)
		go func() { defer wg.Done(); s.RunCanonicalDelivery(ctx, cfg) }()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if e := s.Pool.Ping(r.Context()); e != nil {
			w.WriteHeader(503)
		} else {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ready"}`))
		}
	})
	server := &http.Server{Addr: ":8080", Handler: platform.Handler(kind+"-processor", mux), ReadHeaderTimeout: 5 * time.Second}
	_ = platform.Serve(ctx, server)
	cancel()
	wg.Wait()
}
