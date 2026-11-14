package main

import (
	"clientapi/internal/platform"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type config struct {
	AnalyticsEnabled     bool
	AnalyticsToken       string
	Port                 string
	AuthURL              string
	AggregatorURL        string
	AuthInternalKey      string
	MaxInFlight          int
	MaxInFlightPerClient int
	PriorityEnabled      bool
	FieldReserved        int
	AuthMaxInFlight      int
}

func loadConfig() (config, error) {
	v, err := requiredEnv("CLIENT_API_PORT", "AUTH_BASE_URL", "AGGREGATOR_BASE_URL", "AUTH_INTERNAL_KEY", "CLIENT_API_MAX_INFLIGHT", "CLIENT_API_MAX_INFLIGHT_PER_CLIENT")
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
	perClient, err := envInt(v, "CLIENT_API_MAX_INFLIGHT_PER_CLIENT", 0, 0)
	if err != nil {
		return config{}, err
	}
	cfg := config{Port: v["CLIENT_API_PORT"], AuthURL: v["AUTH_BASE_URL"], AggregatorURL: v["AGGREGATOR_BASE_URL"], AuthInternalKey: v["AUTH_INTERNAL_KEY"], MaxInFlight: maxInFlight, MaxInFlightPerClient: perClient}
	if raw := os.Getenv("ANALYTICS_ENABLED"); raw != "" && raw != "true" && raw != "false" {
		return config{}, fmt.Errorf("invalid ANALYTICS_ENABLED")
	}
	cfg.AnalyticsEnabled = os.Getenv("ANALYTICS_ENABLED") == "true"
	cfg.AnalyticsToken = os.Getenv("ANALYTICS_SERVICE_TOKEN")
	if cfg.AnalyticsEnabled && (cfg.AnalyticsToken == "" || cfg.AnalyticsToken == cfg.AuthInternalKey) {
		return config{}, fmt.Errorf("dedicated ANALYTICS_SERVICE_TOKEN required")
	}
	if raw := os.Getenv("CLIENT_API_PRIORITY_ENABLED"); raw != "" && raw != "false" && raw != "true" {
		return config{}, fmt.Errorf("CLIENT_API_PRIORITY_ENABLED must be true or false")
	}
	cfg.PriorityEnabled = os.Getenv("CLIENT_API_PRIORITY_ENABLED") == "true"
	if cfg.PriorityEnabled {
		settings, err := requiredEnv("CLIENT_API_FIELD_RESERVED", "CLIENT_API_AUTH_MAX_INFLIGHT")
		if err != nil {
			return config{}, err
		}
		if maxInFlight < 2 {
			return config{}, fmt.Errorf("CLIENT_API_MAX_INFLIGHT must be >=2 with priority enabled")
		}
		cfg.FieldReserved, err = envInt(settings, "CLIENT_API_FIELD_RESERVED", 1, maxInFlight-1)
		if err != nil {
			return config{}, err
		}
		cfg.AuthMaxInFlight, err = envInt(settings, "CLIENT_API_AUTH_MAX_INFLIGHT", 1, 65536)
		if err != nil {
			return config{}, err
		}
	}
	return cfg, nil
}

func main() {
	flushTelemetry := platform.Init("client-api")
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = flushTelemetry(c)
	}()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := loadConfig()
	if err != nil {
		log.Error("invalid configuration", "service", "client-api", "error", err)
		os.Exit(1)
	}

	a := &api{cfg: cfg, log: log, client: platform.Client(5 * time.Second)}
	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           platform.Handler("client-api", a.router()),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second, // above the 5s upstream timeout (two sequential upstream calls)
		IdleTimeout:       60 * time.Second,
	}
	log.Info("client API listening", "service", "client-api", "port", cfg.Port)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := platform.Serve(ctx, server); err != nil {
		log.Error("http server failed", "error", err)
	}

}
