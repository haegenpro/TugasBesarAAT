package main

import (
	"aggregator/internal/platform"
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"aggregator/internal/api"
	"aggregator/internal/avrowire"
	"aggregator/internal/clients"
	"aggregator/internal/config"
	"aggregator/internal/m2api"
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

	deps := &api.Dependencies{}
	bmkgClient := clients.NewBMKGClient(cfg.BMKGBaseURL, cfg.BMKGAPIKey, logger)
	pvmbgClient := clients.NewPVMBGClient(cfg.PVMBGBaseURL, cfg.PVMBGToken, logger)
	bmkgClient.CaptureFailure = st.RecordFailure
	pvmbgClient.CaptureFailure = st.RecordFailure
	registry := avrowire.NewClient(cfg.SchemaRegistryURL)
	registry.ConfigureLogging(logger, "aggregator")
	if st.Distributed {
		registry.SetCache(st.SchemaCache())
	}
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
	if st.Distributed {
		p.Leases = ownership.New(st.Pool)
		p.BeforeAttempt = func(c context.Context, endpoint string) error {
			if endpoint != "PVMBG" && !strings.Contains(endpoint, "volcanic") {
				return nil
			}
			if err := st.SyncPolicy(c, volcanicReaders, cfg.PVMBGReaderSchema); err != nil {
				return err
			}
			if endpoint != "replay:volcanic" {
				return st.BlockParkedVolcanic(c, "PVMBG")
			}
			return nil
		}
	}

	a := api.New(st, status, deps, cfg.BMKGFreshness, cfg.PVMBGFreshness)
	if st.Distributed {
		a.ExtraSources = &api.SourceCache{}
	}
	router := server.New(logger, a)
	if st.Distributed {
		fieldConnections := platform.Int("AGGREGATOR_FIELD_DB_CONNS", 3)
		if fieldConnections < 1 || fieldConnections > 2147483647 {
			panic("AGGREGATOR_FIELD_DB_CONNS must be a positive 32-bit integer")
		}
		fieldStore, err := st.ReadPool(ctx, int32(fieldConnections))
		if err != nil {
			logger.Error("Field read pool configuration failed")
			os.Exit(1)
		}
		defer fieldStore.Close()
		fieldAPI := api.New(fieldStore, status, deps, cfg.BMKGFreshness, cfg.PVMBGFreshness)
		fieldAPI.ExtraSources = a.ExtraSources
		server.RegisterFieldReads(router, fieldAPI, os.Getenv("FIELD_READ_TOKEN"))
	}
	if !st.Distributed {
		server.RegisterSchemaAdmin(router, volcanicReaders, cfg.AdminToken)
	}
	if st.Distributed {
		m2api.Register(router, st, m2api.Config{AuditToken: os.Getenv("AUDIT_API_TOKEN"), CanonicalToken: os.Getenv("CANONICAL_SERVICE_TOKEN"), AnalyticsToken: os.Getenv("ANALYTICS_SERVICE_TOKEN"), AdminToken: cfg.AdminToken, NodeA: os.Getenv("M2_NODE_A"), NodeB: os.Getenv("M2_NODE_B"), IngestionURL: os.Getenv("INGESTION_BASE_URL"), IngestionToken: os.Getenv("INGESTION_SERVICE_TOKEN")})
	}
	srv := &http.Server{Addr: ":" + cfg.Port, Handler: platform.Handler("aggregator", router)}
	var workers sync.WaitGroup
	launch := func(fn func()) { workers.Add(1); go func() { defer workers.Done(); fn() }() }
	launch(func() { monitorStore(ctx, st, deps, p, logger) })
	launch(func() { st.CollectMetrics(ctx) })
	if st.Distributed {
		launch(func() { st.RunVisibilityRecovery(ctx) })
		launch(func() { a.ExtraSources.Run(ctx, os.Getenv("INGESTION_BASE_URL"), os.Getenv("INGESTION_SERVICE_TOKEN")) })
	}
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
