package main

import (
	"aggregator/internal/canonical"
	"aggregator/internal/ownership"
	"aggregator/internal/store"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		slog.Error("offline input cutover failed", "error", err)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) != 4 || os.Args[1] != "--offline" || os.Args[2] != "BMKG" || os.Args[3] == "" {
		return fmt.Errorf("use input-cutover --offline BMKG NAMESPACE after stopping writers/publishers")
	}
	if os.Getenv("MIGRATION_DATABASE_URL") == "" || os.Getenv("CANONICAL_DURABILITY_CONFIG") == "" {
		return fmt.Errorf("administrator MIGRATION_DATABASE_URL and captured durability config required")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	s, err := store.New(ctx, os.Getenv("MIGRATION_DATABASE_URL"), slog.Default())
	if err != nil {
		return err
	}
	defer s.Close()
	if err = s.ConfigureReplication(ctx, os.Getenv("CANONICAL_DURABILITY_CONFIG")); err != nil {
		return err
	}
	processor, err := ownership.NewProcessor(s.Pool, 15*time.Second)
	if err != nil {
		return err
	}
	delivery, err := ownership.New(s.Pool, 15*time.Second, canonical.Deadline)
	if err != nil {
		return err
	}
	d := s.Canonical.Durability.(ownership.Durability)
	processor.Durability = d
	delivery.Durability = d
	s.Processor = processor
	s.Canonical.Fence = delivery
	done := make(chan error, 1)
	work, stop := context.WithCancel(ctx)
	defer stop()
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		ownership.Run(work, processor, "processor:BMKG", func(pctx context.Context, p canonical.Ownership) {
			ownership.Run(pctx, delivery, "canonical:BMKG", func(bctx context.Context, b canonical.Ownership) {
				err := s.CutoverBMKG(bctx, p, b, os.Args[3])
				select {
				case done <- err:
				default:
				}
				stop()
			})
		})
	}()
	select {
	case err = <-done:
		stop()
		<-closed
		return err
	case <-ctx.Done():
		stop()
		<-closed
		return ctx.Err()
	}
}
