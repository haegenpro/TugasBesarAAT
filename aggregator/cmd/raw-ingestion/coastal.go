package main

import (
	"aggregator/internal/acquisition"
	"aggregator/internal/avrowire"
	"aggregator/internal/canonical"
	"aggregator/internal/coastal"
	"aggregator/internal/ingestion"
	"aggregator/internal/ownership"
	"aggregator/internal/platform"
	"aggregator/internal/rawinput"
	"aggregator/internal/store"
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go/jetstream"
)

func startCoastal(ctx context.Context, r gin.IRoutes, coordinator *acquisition.Coordinator, db *store.Store, durability ingestion.Durability, authority *ownership.Authority, sender rawinput.Sender, registry *avrowire.Client, reader *avrowire.Reader, consumers map[string]jetstream.Consumer, replicas int, js jetstream.JetStream, token string) error {
	warningSchema, err := os.ReadFile(os.Getenv("COASTAL_WARNING_SCHEMA"))
	if err != nil {
		return err
	}
	check, stop := context.WithTimeout(ctx, canonical.Deadline)
	canonicalConsumer, err := rawinput.EnsureConsumer(check, js, rawinput.Coastal, replicas, rawinput.CoastalCanonical)
	stop()
	if err != nil {
		return err
	}
	observations := &coastal.Breaker{Name: "observations", Threshold: platform.Int("COASTAL_BREAKER_THRESHOLD", 3), Cooldown: platform.Duration("COASTAL_BREAKER_COOLDOWN", 2*time.Second), MaxCooldown: platform.Duration("COASTAL_BREAKER_MAX_COOLDOWN", 30*time.Second)}
	stations := &coastal.Breaker{Name: "stations", Threshold: observations.Threshold, Cooldown: observations.Cooldown, MaxCooldown: observations.MaxCooldown}
	rate := float64(platform.Int("COASTAL_MAX_REQUESTS_PER_SECOND", 8))
	client := &coastal.Client{BaseURL: os.Getenv("PANTAI_BASE_URL"), Token: os.Getenv("PANTAI_AUTH_TOKEN"), HTTP: platform.Client(0), Timeout: platform.Duration("COASTAL_REQUEST_TIMEOUT", time.Second), Limiter: &coastal.Limiter{Rate: rate, Burst: rate}, Stations: stations, Observations: observations}
	status := &coastal.Status{StaleAfter: platform.Duration("COASTAL_STALE_AFTER", time.Minute), Observations: observations, Stations: stations}
	progress := &ingestion.SourceStore{DB: db, Durability: durability, Fence: authority, Source: coastal.Source, Namespace: os.Getenv("COASTAL_NAMESPACE")}
	acquirer := &coastal.Acquirer{Client: client, Progress: progress, Sender: sender, Start: int64(platform.Int("COASTAL_INITIAL_POSITION", 0)), PageSize: platform.Int("COASTAL_PAGE_SIZE", 100), Interval: platform.Duration("COASTAL_POLL_INTERVAL", 500*time.Millisecond), Status: status, Logger: slog.Default()}
	cache := &coastal.StationCache{Client: client, TTL: platform.Duration("COASTAL_STATION_TTL", 5*time.Minute), MissDelay: 10 * time.Second}
	validator := &coastal.Validator{Stations: cache, Quarantine: progress, Warnings: coastal.Warnings{JS: js, Registry: registry, SchemaText: string(warningSchema)}, Registry: registry, Reader: reader, Skew: platform.Duration("COASTAL_CLOCK_SKEW", 2*time.Minute), Logger: slog.Default()}
	writer := &coastal.CanonicalWriter{Validator: validator, Store: canonical.Client{BaseURL: os.Getenv("AGGREGATOR_BASE_URL"), Token: os.Getenv("CANONICAL_SERVICE_TOKEN"), HTTP: platform.Client(canonical.Deadline + 500*time.Millisecond)}, Logger: slog.Default()}
	go coordinator.Run(ctx, coastal.Source, acquirer.Run)
	go validator.Run(ctx, consumers[rawinput.Coastal.Name])
	go writer.Run(ctx, canonicalConsumer)
	coastal.RegisterStatus(r, status, token)
	return nil
}
