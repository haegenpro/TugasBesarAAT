package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"
)

type config struct {
	Port            string
	AuthURL         string
	AggregatorURL   string
	AuthInternalKey string
	MaxInFlight     int
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
	maxInFlight := 128
	if raw := os.Getenv("CLIENT_API_MAX_INFLIGHT"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return config{}, fmt.Errorf("CLIENT_API_MAX_INFLIGHT must be an integer: %w", err)
		}
		maxInFlight = parsed
	}
	return config{Port: port, AuthURL: authURL, AggregatorURL: aggregatorURL, AuthInternalKey: key, MaxInFlight: maxInFlight}, nil
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := loadConfig()
	if err != nil {
		log.Error("invalid configuration", "service", "client-api", "error", err)
		os.Exit(1)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 64 // default of 2 forces connection churn to Auth/Aggregator under load
	a := &api{cfg: cfg, log: log, client: &http.Client{Timeout: 5 * time.Second, Transport: transport}}
	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           a.router(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second, // above the 5s upstream timeout (two sequential upstream calls)
		IdleTimeout:       60 * time.Second,
	}
	log.Info("client API listening", "service", "client-api", "port", cfg.Port)
	if err := server.ListenAndServe(); err != nil {
		log.Error("client API stopped", "service", "client-api", "error", err)
		os.Exit(1)
	}
}
