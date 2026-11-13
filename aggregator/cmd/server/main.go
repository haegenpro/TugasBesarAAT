package main

import (
	"aggregator/internal/platform"
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"aggregator/internal/acquisition"
	"aggregator/internal/api"
	"aggregator/internal/avrowire"
	"aggregator/internal/canonical"
	"aggregator/internal/clients"
	"aggregator/internal/config"
	"aggregator/internal/middleware"
	"aggregator/internal/ownership"
	"aggregator/internal/poller"
	"aggregator/internal/publisher"
	"aggregator/internal/server"
	"aggregator/internal/store"
)

func main() {
	flushTelemetry := platform.Init("aggregator")
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = flushTelemetry(c)
	}()

	logger := middleware.NewLogger()
	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", "service", "aggregator", "error", err.Error())
		os.Exit(1)
	}

	seismicReader, err := avrowire.LoadSourceReader(cfg.BMKGSeismicReaderSchema, "SeismicHistoryPage")
	if err != nil {
		logger.Error("invalid seismic reader schema", "error", err)
		os.Exit(1)
	}
	tsunamiReader, err := avrowire.LoadSourceReader(cfg.BMKGTsunamiReaderSchema, "WarningHistoryPage")
	if err != nil {
		logger.Error("invalid tsunami reader schema", "error", err)
		os.Exit(1)
	}
	volcanicReader, err := avrowire.LoadSourceReader(cfg.PVMBGReaderSchema, "VolcanicHistoryPage")
	if err != nil {
		logger.Error("invalid volcanic reader schema", "error", err)
		os.Exit(1)
	}
	volcanicReaders, err := avrowire.NewReaderSwitch(volcanicReader, "VolcanicHistoryPage")
	if err != nil {
		logger.Error("invalid volcanic reader selection", "error", err)
		os.Exit(1)
	}

	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	st, err := store.New(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		logger.Error("invalid canonical store configuration", "service", "aggregator", "error", err.Error())
		os.Exit(1)
	}
	defer st.Close()
	if path := os.Getenv("CANONICAL_DURABILITY_CONFIG"); path != "" {
		if err := st.ConfigureReplication(ctx, path); err != nil {
			logger.Error("invalid replication configuration", "error", err)
			os.Exit(1)
		}
	}

	deps := &api.Dependencies{}
	bmkgClient := clients.NewBMKGClient(cfg.BMKGBaseURL, cfg.BMKGAPIKey, logger)
	pvmbgClient := clients.NewPVMBGClient(cfg.PVMBGBaseURL, cfg.PVMBGToken, logger)
	bmkgClient.CaptureFailure = st.RecordFailure
	pvmbgClient.CaptureFailure = st.RecordFailure
	registry := avrowire.NewClient(cfg.SchemaRegistryURL)
	registry.ConfigureLogging(logger, "aggregator")
	bmkgClient.Registry = registry
	bmkgClient.Readers = map[string]*avrowire.SourceReader{"/seismic-events": seismicReader, "/tsunami-warnings": tsunamiReader}
	pvmbgClient.Registry = registry
	pvmbgClient.Reader = volcanicReader
	pvmbgClient.ReaderSwitch = volcanicReaders
	lookup := st
	status := poller.NewPVMBGStatus()

	p := &poller.Poller{
		DisablePoll: !platform.Bool("AGGREGATOR_POLL_ENABLED", true), DisableReplay: !platform.Bool("AGGREGATOR_REPLAY_ENABLED", true),
		BMKG:       bmkgClient,
		PVMBG:      pvmbgClient,
		Store:      st,
		Lookup:     lookup,
		Status:     status,
		Ready:      deps.StoreReady,
		Interval:   cfg.PollInterval,
		MaxBackoff: cfg.MaxBackoff,
		Logger:     logger,
	}
	if cfg.AcquisitionEnabled {
		authority, err := ownership.NewAcquisition(st.Pool, cfg.AcquisitionTTL, canonical.Deadline)
		if err != nil {
			logger.Error("invalid acquisition authority", "error", err)
			os.Exit(1)
		}
		durability, ok := st.Canonical.Durability.(ownership.Durability)
		if !ok {
			logger.Error("acquisition requires the replicated durability adapter")
			os.Exit(1)
		}
		authority.Durability = durability
		coordinator, err := acquisition.New(authority, logger)
		if err != nil {
			logger.Error("invalid acquisition coordinator", "error", err)
			os.Exit(1)
		}
		coordinator.Ready = deps.StoreReady
		st.Acquisition = authority
		p.Acquisition = coordinator
		bmkgClient.BeforeFetch = acquisition.RequireRequest
		pvmbgClient.BeforeFetch = acquisition.RequireRequest
	}

	a := api.New(st, status, deps, cfg.BMKGFreshness, cfg.PVMBGFreshness)
	router := server.New(logger, a)
	if cfg.CanonicalEnabled {
		adapter := canonical.HTTP{Repository: st, Token: cfg.CanonicalToken}
		if cfg.OwnershipEnabled {
			authority, err := ownership.New(st.Pool, cfg.OwnershipTTL, canonical.Deadline)
			if err != nil {
				logger.Error("invalid ownership configuration", "error", err)
				os.Exit(1)
			}
			st.Canonical.Fence = authority
			if durability, ok := st.Canonical.Durability.(ownership.Durability); ok {
				authority.Durability = durability
			}
			adapter.Ownership = authority
		}
		adapter.Register(router)
	}
	if platform.Bool("PLATFORM_COORDINATION_ENABLED", false) {
		token := os.Getenv("PROCESSOR_SERVICE_TOKEN")
		if token == "" || token == cfg.AdminToken || token == cfg.CanonicalToken || token == cfg.BMKGAPIKey || token == cfg.PVMBGToken || token == os.Getenv("AUDIT_TOKEN") {
			logger.Error("platform coordination requires a dedicated PROCESSOR_SERVICE_TOKEN")
			os.Exit(1)
		}
		authority, err := ownership.NewProcessor(st.Pool, 15*time.Second)
		if err != nil {
			logger.Error("invalid processor authority", "error", err)
			os.Exit(1)
		}
		durability, ok := st.Canonical.Durability.(ownership.Durability)
		if !ok {
			logger.Error("platform coordination requires replicated durability")
			os.Exit(1)
		}
		authority.Durability = durability
		st.Processor = authority
		processorAPI := canonical.HTTP{Ownership: authority, Token: token}
		processorAPI.RegisterOwnership(router, "/internal/processor/ownership")
	}
	server.RegisterSchemaAdmin(router, volcanicReaders, cfg.AdminToken)
	srv := &http.Server{Addr: ":" + cfg.Port, Handler: platform.Handler("aggregator", router)}
	var workers sync.WaitGroup
	launch := func(fn func()) { workers.Add(1); go func() { defer workers.Done(); fn() }() }
	launch(func() { <-ctx.Done(); deps.SetDraining(true); deps.SetStoreReady(false) })
	launch(func() { monitorStore(ctx, st, deps, p, logger) })
	launch(func() { st.CollectMetrics(ctx) })
	if platform.Bool("AGGREGATOR_PUBLISH_ENABLED", true) {
		launch(func() { dispatchOutbox(ctx, st, cfg, deps, registry, logger) })
		launch(func() { dispatchLifecycle(ctx, st, cfg, deps, registry, logger) })
	}

	logger.Info("starting aggregator", "service", "aggregator", "port", cfg.Port)
	if err := platform.Serve(ctx, srv); err != nil {
		logger.Error("http server failed", "error", err)
		stopSignals()
	}
	stopSignals()
	workers.Wait()

}

// monitorStore keeps the API alive while Postgres is unavailable and starts
// polling after the database and its initialized schema are ready.
func monitorStore(ctx context.Context, st *store.Store, deps *api.Dependencies, p *poller.Poller, logger *slog.Logger) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	pollDone := make(chan struct{})
	pollStarted := false
	defer func() {
		if pollStarted {
			<-pollDone
		}
	}()
	pollerStarted := false
	schemaReady := false
	for {
		checkCtx, cancel := context.WithTimeout(middleware.EnsureCorrelationID(ctx), 5*time.Second)
		err := st.Ping(checkCtx)
		if err == nil && schemaReady {
			err = st.RecoverAttempts(checkCtx)
		}
		if err == nil && !schemaReady {
			err = st.CheckSchema(checkCtx)
			if err == nil {
				err = st.RecoverAttempts(checkCtx)
			}
			if err == nil {
				schemaReady = true
			}
		}
		if err == nil {
			err = st.CheckAcquisition(checkCtx)
		}
		if err == nil {
			err = st.CheckDurability(checkCtx)
		}
		cancel()
		deps.SetStoreReady(err == nil)
		if err == nil {
			if !pollerStarted {
				pollStarted = true
				go func() { defer close(pollDone); p.Run(ctx) }()
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
func dispatchOutbox(ctx context.Context, st *store.Store, cfg config.Config, deps *api.Dependencies, registry *avrowire.Client, logger *slog.Logger) {
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
		pub, err := publisher.New(connectCtx, cfg.NATSURL, cfg.NATSStream, cfg.NATSSubject, registry, logger)
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

func dispatchLifecycle(ctx context.Context, st *store.Store, cfg config.Config, deps *api.Dependencies, registry *avrowire.Client, logger *slog.Logger) {
	for ctx.Err() == nil {
		if !deps.StoreReady() {
			if !sleepContext(ctx, time.Second) {
				return
			}
			continue
		}
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		pub, err := publisher.New(callCtx, cfg.NATSURL, cfg.LifecycleStream, cfg.LifecycleSubject, registry, logger)
		cancel()
		if err != nil {
			logger.Warn("lifecycle broker unavailable; retrying", "error", err)
			if !sleepContext(ctx, 2*time.Second) {
				return
			}
			continue
		}
		deps.SetLifecycleReady(true)
		for ctx.Err() == nil && deps.StoreReady() && pub.Connected() {
			callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			found, err := st.DeliverLifecycle(callCtx, pub.PublishLifecycle)
			cancel()
			if err != nil {
				logger.Warn("lifecycle delivery failed; retained for retry", "error", err)
				break
			}
			if !found && !sleepContext(ctx, time.Second) {
				break
			}
		}
		deps.SetLifecycleReady(false)
		pub.Close()
		if !sleepContext(ctx, 2*time.Second) {
			return
		}
	}
}
