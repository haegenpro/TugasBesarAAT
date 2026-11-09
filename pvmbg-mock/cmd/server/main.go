package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"pvmbg-mock/internal/platform"
	"syscall"
	"time"

	"pvmbg-mock/internal/avrowire"
	"pvmbg-mock/internal/config"
	"pvmbg-mock/internal/handlers"
	"pvmbg-mock/internal/history"
	"pvmbg-mock/internal/middleware"
	"pvmbg-mock/internal/seed"
	"pvmbg-mock/internal/server"
	"pvmbg-mock/internal/state"
)

func main() {
	flushTelemetry := platform.Init("pvmbg-mock")
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = flushTelemetry(c)
	}()

	logger := middleware.NewLogger()
	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", "service", "pvmbg-mock", "error", err.Error())
		os.Exit(1)
	}

	reports := seed.Build()
	toggles := state.New()
	h := handlers.New(reports, toggles, cfg.MinDelayMs, cfg.MaxDelayMs)
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
	h.Registry.ConfigureLogging(logger, "pvmbg-mock")
	router := server.New(logger, cfg.Token, cfg.AdminToken, h, toggles)

	genCtx, stopGenerator := context.WithCancel(context.Background())
	defer stopGenerator()
	generatorDone := make(chan struct{})
	go func() {
		defer close(generatorDone)
		h.RunGenerator(genCtx, time.Duration(cfg.ReportIntervalMs)*time.Millisecond, logger)
	}()
	defer func() { stopGenerator(); <-generatorDone }()

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: time.Minute,
		Handler: platform.Handler("pvmbg-mock", router),
	}

	go func() {
		logger.Info("starting pvmbg-mock", "service", "pvmbg-mock", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server failed", "service", "pvmbg-mock", "error", err.Error())
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), platform.Duration("SHUTDOWN_TIMEOUT", 30*time.Second))
	defer cancel()
	logger.Info("shutting down pvmbg-mock", "service", "pvmbg-mock")
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("graceful shutdown failed", "service", "pvmbg-mock", "error", err.Error())
	}
}
