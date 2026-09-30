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
	logger := middleware.NewLogger()
	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", "service", "aggregator", "error", err.Error())
		os.Exit(1)
	}

	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("invalid canonical store configuration", "service", "aggregator", "error", err.Error())
		os.Exit(1)
	}
	defer st.Close()

	deps := &api.Dependencies{}
	bmkgClient := clients.NewBMKGClient(cfg.BMKGBaseURL, cfg.BMKGAPIKey, logger)
	pvmbgClient := clients.NewPVMBGClient(cfg.PVMBGBaseURL, cfg.PVMBGToken, logger)
	lookup := volcano.NewLookup(st.Pool)
	status := poller.NewPVMBGStatus()

	p := &poller.Poller{
		BMKG:     bmkgClient,
		PVMBG:    pvmbgClient,
		Store:    st,
		Lookup:   lookup,
		Status:   status,
		Ready:    deps.StoreReady,
		Interval: cfg.PollInterval,
		Logger:   logger,
	}

	a := api.New(st, status, deps)
	router := server.New(logger, a)
	srv := &http.Server{Addr: ":" + cfg.Port, Handler: router}
	go monitorStore(ctx, st, deps, p, logger)
	go dispatchOutbox(ctx, st, cfg, deps, logger)

	go func() {
		logger.Info("starting aggregator", "service", "aggregator", "port", cfg.Port, "poll_interval_sec", cfg.PollInterval.Seconds())
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server failed", "service", "aggregator", "error", err.Error())
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down aggregator", "service", "aggregator")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "service", "aggregator", "error", err.Error())
	}
}

// monitorStore keeps the API alive while Postgres is unavailable and starts
// polling after the database and its initialized schema are ready.
func monitorStore(ctx context.Context, st *store.Store, deps *api.Dependencies, p *poller.Poller, logger *slog.Logger) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	pollerStarted := false
	schemaReady := false
	for {
		checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := st.Ping(checkCtx)
		if err == nil && !schemaReady {
			err = st.CheckSchema(checkCtx)
			if err == nil {
				schemaReady = true
			}
		}
		cancel()
		deps.SetStoreReady(err == nil)
		if err == nil {
			if !pollerStarted {
				go p.Run(ctx)
				pollerStarted = true
				logger.Info("canonical store ready; polling started", "service", "aggregator")
			}
		} else {
			schemaReady = false
			if ctx.Err() == nil {
				logger.Warn("canonical store unavailable; retrying", "service", "aggregator", "error", err.Error())
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// dispatchOutbox retries both broker connection and pending publication.
func dispatchOutbox(ctx context.Context, st *store.Store, cfg config.Config, deps *api.Dependencies, logger *slog.Logger) {
	for {
		if ctx.Err() != nil {
			return
		}
		if !deps.StoreReady() {
			if !sleepContext(ctx, 2*time.Second) {
				return
			}
			continue
		}
		connectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		pub, err := publisher.New(connectCtx, cfg.NATSURL, cfg.NATSStream, cfg.NATSSubject, logger)
		cancel()
		if err != nil {
			logger.Warn("nats jetstream unavailable; retrying", "service", "aggregator", "error", err.Error())
			if !sleepContext(ctx, 2*time.Second) {
				return
			}
			continue
		}
		deps.SetBrokerReady(true)
		for ctx.Err() == nil && deps.StoreReady() && pub.Connected() {
			attemptCtx, attemptCancel := context.WithTimeout(ctx, 10*time.Second)
			found, err := st.DeliverNext(attemptCtx, func(callCtx context.Context, event store.OutboxEvent) error {
				return pub.Publish(callCtx, event.Hazard, event.CorrelationID)
			})
			attemptCancel()
			if err != nil {
				if ctx.Err() == nil {
					logger.Warn("outbox delivery failed; retrying", "service", "aggregator", "error", err.Error())
				}
				break
			}
			if !found && !sleepContext(ctx, time.Second) {
				break
			}
		}
		deps.SetBrokerReady(false)
		pub.Close()
		if !sleepContext(ctx, 2*time.Second) {
			return
		}
	}
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
