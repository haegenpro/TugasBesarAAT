package main

import (
	"bmkg-mock/internal/platform"
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bmkg-mock/internal/avrowire"
	"bmkg-mock/internal/config"
	"bmkg-mock/internal/handlers"
	"bmkg-mock/internal/history"
	"bmkg-mock/internal/middleware"
	"bmkg-mock/internal/seed"
	"bmkg-mock/internal/server"
)

func main() {
	flushTelemetry := platform.Init("bmkg-mock")
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = flushTelemetry(c)
	}()

	logger := middleware.NewLogger()
	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", "service", "bmkg-mock", "error", err.Error())
		os.Exit(1)
	}

	events, warnings := seed.Build()
	h := handlers.New(events, warnings, cfg.DelayMs)
	archive, err := history.Open(cfg.ArchivePath)
	if err != nil {
		logger.Error("source archive unavailable", "error", err)
		os.Exit(1)
	}
	defer archive.DB.Close()
	if err := h.AttachArchive(archive); err != nil {
		logger.Error("source archive initialization failed", "error", err)
		os.Exit(1)
	}
	h.Registry = avrowire.NewClient(cfg.RegistryURL)
	h.Registry.ConfigureLogging(logger, "bmkg-mock")
	router := server.New(logger, cfg.APIKey, h)

	genCtx, stopGenerator := context.WithCancel(context.Background())
	defer stopGenerator()
	generatorDone := make(chan struct{})
	go func() {
		defer close(generatorDone)
		h.RunGenerator(genCtx, time.Duration(cfg.EventIntervalMs)*time.Millisecond, logger)
	}()
	defer func() { stopGenerator(); <-generatorDone }()

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: time.Minute,
		Handler: platform.Handler("bmkg-mock", router),
	}

	go func() {
		logger.Info("starting bmkg-mock", "service", "bmkg-mock", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server failed", "service", "bmkg-mock", "error", err.Error())
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), platform.Duration("SHUTDOWN_TIMEOUT", 30*time.Second))
	defer cancel()
	logger.Info("shutting down bmkg-mock", "service", "bmkg-mock")
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("graceful shutdown failed", "service", "bmkg-mock", "error", err.Error())
	}
}
