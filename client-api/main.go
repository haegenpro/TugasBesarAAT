package main

import (
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
	MaxInFlight     int
}

func loadConfig() (config, error) {
	v, err := requiredEnv("CLIENT_API_PORT", "AUTH_BASE_URL", "AGGREGATOR_BASE_URL", "AUTH_INTERNAL_KEY", "CLIENT_API_MAX_INFLIGHT")
	if err != nil {
		return config{}, err
	}
	if _, err = envInt(v, "CLIENT_API_PORT", 1, 65535); err != nil {
		return config{}, err
	}
	for _, key := range []string{"AUTH_BASE_URL", "AGGREGATOR_BASE_URL"} {
		if err = envURL(v, key, "http", "https"); err != nil {
			return config{}, err
		}
	}
	maxInFlight, err := envInt(v, "CLIENT_API_MAX_INFLIGHT", 0, 0)
	if err != nil {
		return config{}, err
	}
	return config{Port: v["CLIENT_API_PORT"], AuthURL: v["AUTH_BASE_URL"], AggregatorURL: v["AGGREGATOR_BASE_URL"], AuthInternalKey: v["AUTH_INTERNAL_KEY"], MaxInFlight: maxInFlight}, nil
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
