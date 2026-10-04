package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bmkg-mock/internal/avrowire"
	"bmkg-mock/internal/config"
	"bmkg-mock/internal/handlers"
	"bmkg-mock/internal/middleware"
	"bmkg-mock/internal/seed"
	"bmkg-mock/internal/server"
)

func main() {
	logger := middleware.NewLogger()
	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", "service", "bmkg-mock", "error", err.Error())
		os.Exit(1)
	}

	events, warnings := seed.Build()
	h := handlers.New(events, warnings, cfg.DelayMs)
	h.Registry = avrowire.NewClient(cfg.RegistryURL)
	h.Registry.ConfigureLogging(logger, "bmkg-mock")
	router := server.New(logger, cfg.APIKey, h)

	genCtx, stopGenerator := context.WithCancel(context.Background())
	defer stopGenerator()
	go h.RunGenerator(genCtx, time.Duration(cfg.EventIntervalMs)*time.Millisecond, logger)

	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: router,
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	logger.Info("shutting down bmkg-mock", "service", "bmkg-mock")
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("graceful shutdown failed", "service", "bmkg-mock", "error", err.Error())
	}
}
