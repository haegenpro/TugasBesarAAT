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
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "situation-summarizer")
	cfg, err := loadConfig()
	if err != nil {
		log.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	source := &Aggregator{BaseURL: cfg.AggregatorURL, HTTP: &http.Client{Timeout: 10 * time.Second}}
	llm := &OpenRouter{
		BaseURL:         cfg.OpenRouterURL,
		APIKey:          cfg.APIKey,
		Model:           cfg.Model,
		Fallbacks:       cfg.FallbackModels,
		MaxTokens:       cfg.MaxTokens,
		ReasoningTokens: cfg.ReasoningTokens,
		HTTP:            &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}
	scheduler := NewScheduler(cfg, source, llm, log)
	chat := NewChat(cfg, scheduler.Latest, llm, log, scheduler.metrics)
	done := make(chan struct{})
	go func() { defer close(done); scheduler.Run(ctx) }()
	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           scheduler.Handler(chat),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      cfg.ChatTimeout + 10*time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	log.Info("situation summarizer listening", "port", cfg.Port, "model", cfg.Model, "fallbacks", cfg.FallbackModels)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("http server failed", "error", err)
		stop()
	}
	<-done
}
