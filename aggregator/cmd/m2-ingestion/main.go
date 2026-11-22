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
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "ingestion"))
	flush := platform.Init("ingestion")
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = flush(ctx)
	}()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	s, e := ingest.New(ctx, os.Getenv("DATABASE_URL"))
	if e != nil {
		slog.Error("ingestion configuration failed", "error", e)
		os.Exit(1)
	}
	defer s.Pool.Close()
	registry := avrowire.NewClient(os.Getenv("SCHEMA_REGISTRY_URL"))
	registry.SetCache(s)
	var wg sync.WaitGroup
	launch := func(fn func()) { wg.Add(1); go func() { defer wg.Done(); fn() }() }
	launch(func() { s.RunMetrics(ctx, true) })
	launch(func() { s.RunPublisher(ctx, os.Getenv("NATS_URL"), registry) })
	if token := os.Getenv("SATELLITE_TOKEN"); token != "" {
		launch(func() {
			s.RunSatellite(ctx, ingest.SatelliteConfig{URL: os.Getenv("SATELLITE_URL"), Token: token, Namespace: os.Getenv("SATELLITE_NAMESPACE"), Initial: int64(platform.Int("SATELLITE_INITIAL_OFFSET", 0)), Exclusive: platform.Bool("SATELLITE_EXCLUSIVE_FROM", false)})
		})
	}
	if token := os.Getenv("PANTAI_AUTH_TOKEN"); token != "" {
		launch(func() {
			s.RunCoastal(ctx, ingest.CoastalConfig{URL: os.Getenv("PANTAI_BASE_URL"), Token: token, Namespace: os.Getenv("PANTAI_NAMESPACE"), Rate: float64(platform.Int("PANTAI_REQUESTS_PER_SECOND", 4))})
		})
	}
	server := &http.Server{Addr: ":8080", Handler: platform.Handler("ingestion", s.Handler(os.Getenv("FIELD_REPORT_SERVICE_TOKEN"), os.Getenv("COASTAL_SLO_TOKEN"))), ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	if e = platform.Serve(ctx, server); e != nil {
		slog.Error("ingestion HTTP server stopped", "error", e)
	}
	cancel()
	wg.Wait()
}
