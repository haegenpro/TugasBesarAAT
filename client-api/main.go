package main

import (
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"
)

type config struct {
	Port            string
	AuthURL         string
	AggregatorURL   string
	AuthInternalKey string
}

func loadConfig() (config, error) {
	key := os.Getenv("AUTH_INTERNAL_KEY")
	if key == "" {
		return config{}, errors.New("AUTH_INTERNAL_KEY is required")
	}
	port := os.Getenv("CLIENT_API_PORT")
	if port == "" {
		port = "8080"
	}
	authURL := os.Getenv("AUTH_BASE_URL")
	if authURL == "" {
		authURL = "http://auth-service:8083"
	}
	aggregatorURL := os.Getenv("AGGREGATOR_BASE_URL")
	if aggregatorURL == "" {
		aggregatorURL = "http://aggregator:8080"
	}
	return config{Port: port, AuthURL: authURL, AggregatorURL: aggregatorURL, AuthInternalKey: key}, nil
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := loadConfig()
	if err != nil {
		log.Error("invalid configuration", "service", "client-api", "error", err)
		os.Exit(1)
	}
	a := &api{cfg: cfg, log: log, client: &http.Client{Timeout: 5 * time.Second}}
	server := &http.Server{Addr: ":" + cfg.Port, Handler: a.router(), ReadHeaderTimeout: 5 * time.Second}
	log.Info("client API listening", "service", "client-api", "port", cfg.Port)
	if err := server.ListenAndServe(); err != nil {
		log.Error("client API stopped", "service", "client-api", "error", err)
		os.Exit(1)
	}
}
