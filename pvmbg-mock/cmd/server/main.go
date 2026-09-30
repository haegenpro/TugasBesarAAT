package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"pvmbg-mock/internal/config"
	"pvmbg-mock/internal/handlers"
	"pvmbg-mock/internal/middleware"
	"pvmbg-mock/internal/seed"
	"pvmbg-mock/internal/server"
	"pvmbg-mock/internal/state"
)

func main() {
	logger := middleware.NewLogger()
	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", "service", "pvmbg-mock", "error", err.Error())
		os.Exit(1)
	}

	reports := seed.Build()
	toggles := state.New()
	h := handlers.New(reports, toggles, cfg.MinDelayMs, cfg.MaxDelayMs)
	router := server.New(logger, cfg.Token, cfg.AdminToken, h, toggles)

	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: router,
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	logger.Info("shutting down pvmbg-mock", "service", "pvmbg-mock")
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("graceful shutdown failed", "service", "pvmbg-mock", "error", err.Error())
	}
}
