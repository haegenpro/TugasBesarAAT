package main

import (
	"authservice/internal/platform"
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	flushTelemetry := platform.Init("auth-service")
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = flushTelemetry(c)
	}()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := loadConfig()
	if err != nil {
		log.Error("invalid configuration", "service", "auth-service", "error", err)
		os.Exit(1)
	}
	store, err := openStore(cfg.DatabaseURL, cfg.AccessTTL, cfg.RefreshTTL)
	if err != nil {
		log.Error("cannot open auth storage", "service", "auth-service", "error", err)
		os.Exit(1)
	}
	defer store.db.Close()
	server := &http.Server{Addr: ":" + cfg.Port, Handler: platform.Handler("auth-service", (&authServer{config: cfg, store: store, log: log}).router()), ReadHeaderTimeout: 5 * time.Second}
	log.Info("auth service listening", "service", "auth-service", "port", cfg.Port)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	metricsDone := make(chan struct{})
	go func() { defer close(metricsDone); store.collectMetrics(ctx) }()
	defer func() { stop(); <-metricsDone }()
	if err := platform.Serve(ctx, server); err != nil {
		log.Error("http server failed", "error", err)
	}

}
