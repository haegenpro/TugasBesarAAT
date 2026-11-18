package main

import (
	"aggregator/internal/acquisition"
	"aggregator/internal/ingestion"
	"aggregator/internal/ownership"
	"aggregator/internal/platform"
	"aggregator/internal/rawinput"
	"aggregator/internal/satellite"
	"aggregator/internal/store"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"
)

func startSatellite(ctx context.Context, coordinator *acquisition.Coordinator, db *store.Store, durability ingestion.Durability, authority *ownership.Authority, sender rawinput.Sender) error {
	start, err := strconv.ParseInt(os.Getenv("SATELLITE_INITIAL_OFFSET"), 10, 64)
	if err != nil || start < 0 {
		return fmt.Errorf("invalid SATELLITE_INITIAL_OFFSET")
	}
	progress := &ingestion.SourceStore{DB: db, Durability: durability, Fence: authority, Source: satellite.Source, Namespace: os.Getenv("SATELLITE_NAMESPACE")}
	sourceHTTP, err := acquisition.Client(authority, satellite.Source, os.Getenv("SATELLITE_FEED_URL"), 0, 64<<10)
	if err != nil {
		return err
	}
	connector := &satellite.Connector{
		MetadataKey:   func(ctx context.Context) (string, error) { return authority.DispatchWindow(ctx, 5*time.Second) },
		BaseURL:       os.Getenv("SATELLITE_FEED_URL"),
		Token:         os.Getenv("SATELLITE_FEED_TOKEN"),
		HTTP:          sourceHTTP,
		Progress:      progress,
		Sender:        sender,
		Start:         start,
		IdleTimeout:   platform.Duration("SATELLITE_IDLE_TIMEOUT", 30*time.Second),
		FlushEvery:    platform.Int("SATELLITE_FLUSH_EVERY", 256),
		FlushAfter:    platform.Duration("SATELLITE_FLUSH_AFTER", time.Second),
		MaxEventSize:  platform.Int("SATELLITE_MAX_EVENT_BYTES", 256<<10),
		ExclusiveFrom: platform.Bool("SATELLITE_FROM_EXCLUSIVE", false),
		Logger:        slog.Default(),
	}
	go coordinator.Run(ctx, satellite.Source, connector.Run)
	return nil
}
