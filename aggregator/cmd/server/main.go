package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"aggregator/internal/api"
	"aggregator/internal/clients"
	"aggregator/internal/config"
	"aggregator/internal/middleware"
	"aggregator/internal/poller"
	"aggregator/internal/publisher"
	"aggregator/internal/server"
	"aggregator/internal/store"
	"aggregator/internal/volcano"
)

func main() {
	cfg := config.Load()
	logger := middleware.NewLogger()

	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	dbCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	st, err := waitForStore(dbCtx, cfg.DatabaseURL, logger)
	cancel()
	if err != nil {
		logger.Error("failed to connect to canonical store", "service", "aggregator", "error", err.Error())
		os.Exit(1)
	}
	defer st.Close()

	pubCtx, pubCancel := context.WithTimeout(ctx, 30*time.Second)
	pub, err := waitForPublisher(pubCtx, cfg, logger)
	pubCancel()
	if err != nil {
		logger.Error("failed to connect to NATS JetStream", "service", "aggregator", "error", err.Error())
		os.Exit(1)
	}
	defer pub.Close()

	bmkgClient := clients.NewBMKGClient(cfg.BMKGBaseURL, cfg.BMKGAPIKey, logger)
	pvmbgClient := clients.NewPVMBGClient(cfg.PVMBGBaseURL, cfg.PVMBGToken, logger)
	lookup := volcano.NewLookup(st.Pool)
	status := poller.NewPVMBGStatus()

	p := &poller.Poller{
		BMKG:     bmkgClient,
		PVMBG:    pvmbgClient,
		Store:    st,
		Lookup:   lookup,
		Pub:      pub,
		Status:   status,
		Interval: cfg.PollInterval,
		Logger:   logger,
	}
	pollCtx, pollCancel := context.WithCancel(ctx)
	go p.Run(pollCtx)

	a := api.New(st, status)
	router := server.New(logger, a)
	srv := &http.Server{Addr: ":" + cfg.Port, Handler: router}

	go func() {
		logger.Info("starting aggregator", "service", "aggregator", "port", cfg.Port, "poll_interval_sec", cfg.PollInterval.Seconds())
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server failed", "service", "aggregator", "error", err.Error())
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down aggregator", "service", "aggregator")
	pollCancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "service", "aggregator", "error", err.Error())
	}
}

// waitForStore retries the initial connection/ping since compose's
// depends_on/healthcheck ordering is not a hard guarantee that Postgres is
// actually ready to accept connections yet.
func waitForStore(ctx context.Context, url string, logger *slog.Logger) (*store.Store, error) {
	var lastErr error
	for {
		select {
		case <-ctx.Done():
			return nil, lastErr
		default:
		}
		st, err := store.New(ctx, url)
		if err == nil {
			if pingErr := st.Ping(ctx); pingErr == nil {
				return st, nil
			} else {
				lastErr = pingErr
				st.Close()
			}
		} else {
			lastErr = err
		}
		logger.Warn("canonical store not ready, retrying", "service", "aggregator", "error", lastErr.Error())
		time.Sleep(1 * time.Second)
	}
}

// waitForPublisher retries the initial NATS JetStream connection for the
// same reason.
func waitForPublisher(ctx context.Context, cfg config.Config, logger *slog.Logger) (*publisher.Publisher, error) {
	var lastErr error
	for {
		select {
		case <-ctx.Done():
			return nil, lastErr
		default:
		}
		pub, err := publisher.New(ctx, cfg.NATSURL, cfg.NATSStream, cfg.NATSSubject, logger)
		if err == nil {
			return pub, nil
		}
		lastErr = err
		logger.Warn("nats jetstream not ready, retrying", "service", "aggregator", "error", lastErr.Error())
		time.Sleep(1 * time.Second)
	}
}
