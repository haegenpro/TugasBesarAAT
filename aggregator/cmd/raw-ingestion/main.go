// raw-ingestion has its own database and credential. Running this binary never
// migrates schemas or opens DATABASE_URL (the canonical database).
package main

import (
	"aggregator/internal/acquisition"
	"aggregator/internal/avrowire"
	"aggregator/internal/canonical"
	"aggregator/internal/fieldreport"
	"aggregator/internal/ingestion"
	"aggregator/internal/ownership"
	"aggregator/internal/pagecursor"
	"aggregator/internal/platform"
	"aggregator/internal/rawinput"
	"aggregator/internal/store"
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		slog.Error("raw ingestion stopped", "error", err)
		os.Exit(1)
	}
}

var knownComponents = []string{"bmkg", "field-reports", "satellite", "coastal"}

func components() (map[string]bool, error) {
	raw := os.Getenv("INGESTION_COMPONENTS")
	if raw == "" {
		raw = "bmkg"
	}
	enabled := map[string]bool{}
	for _, name := range strings.Split(raw, ",") {
		name = strings.TrimSpace(name)
		if !slices.Contains(knownComponents, name) {
			return nil, fmt.Errorf("unknown ingestion component %q", name)
		}
		enabled[name] = true
	}
	return enabled, nil
}

func require(keys ...string) error {
	for _, key := range keys {
		if os.Getenv(key) == "" {
			return fmt.Errorf("missing %s", key)
		}
	}
	return nil
}

func run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		if len(os.Args) != 5 || os.Args[2] != "--offline" {
			return fmt.Errorf("use migrate --offline REPOSITORY_ROOT RUNTIME_ROLE")
		}
		dsn := os.Getenv("INGESTION_MIGRATION_DATABASE_URL")
		if dsn == "" {
			return fmt.Errorf("administrator INGESTION_MIGRATION_DATABASE_URL required")
		}
		c, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return fmt.Errorf("ingestion migration connection unavailable")
		}
		defer c.Close(context.Background())
		return ingestion.Migrate(ctx, c, os.Args[3], os.Args[4])
	}
	if len(os.Args) != 1 {
		return fmt.Errorf("use raw-ingestion, or migrate --offline ROOT ROLE")
	}
	enabled, err := components()
	if err != nil {
		return err
	}
	if err = require("INGESTION_DATABASE_URL", "INGESTION_NAMESPACE", "INGESTION_INITIAL_OFFSET", "INGESTION_PAYLOAD_QUOTA_BYTES", "INGESTION_DURABILITY_CONFIG", "INGESTION_SERVICE_TOKEN", "INGESTION_CURSOR_KEY", "INGESTION_PORT", "SCHEMA_REGISTRY_URL", "RAW_INPUT_SCHEMA", "NATS_URL"); err != nil {
		return err
	}
	if enabled["bmkg"] {
		if err = require("BMKG_BASE_URL", "BMKG_API_KEY", "BMKG_SEISMIC_READER_SCHEMA", "BMKG_TSUNAMI_READER_SCHEMA"); err != nil {
			return err
		}
	}
	if enabled["field-reports"] {
		if err = require("FIELD_REPORT_SERVICE_TOKEN", "FIELD_REPORT_NAMESPACE"); err != nil {
			return err
		}
	}
	if enabled["coastal"] {
		if err = require("PANTAI_BASE_URL", "PANTAI_AUTH_TOKEN", "COASTAL_NAMESPACE", "COASTAL_WARNING_SCHEMA", "AGGREGATOR_BASE_URL", "CANONICAL_SERVICE_TOKEN"); err != nil {
			return err
		}
	}
	if os.Getenv("INGESTION_DATABASE_URL") == os.Getenv("DATABASE_URL") {
		return fmt.Errorf("ingestion requires its own database")
	}
	start, err := strconv.ParseInt(os.Getenv("INGESTION_INITIAL_OFFSET"), 10, 64)
	if err != nil || start < 0 {
		return fmt.Errorf("invalid initial offset")
	}
	quota, err := strconv.ParseInt(os.Getenv("INGESTION_PAYLOAD_QUOTA_BYTES"), 10, 64)
	if err != nil || quota < 1 {
		return fmt.Errorf("explicit positive payload quota required")
	}
	replicas := platform.Int("RAW_STREAM_REPLICAS", 3)
	signer := pagecursor.Signer{Key: []byte(os.Getenv("INGESTION_CURSOR_KEY"))}
	if !signer.Valid() {
		return fmt.Errorf("cursor signing key requires at least 32 bytes")
	}
	token := os.Getenv("INGESTION_SERVICE_TOKEN")
	reportToken := os.Getenv("FIELD_REPORT_SERVICE_TOKEN")
	if reportToken == token {
		return fmt.Errorf("dedicated field report token required")
	}
	for _, key := range []string{"BMKG_API_KEY", "PVMBG_TOKEN", "CANONICAL_SERVICE_TOKEN", "PROCESSOR_SERVICE_TOKEN", "AUDIT_TOKEN", "AGGREGATOR_ADMIN_TOKEN", "FIELD_REPORT_SERVICE_TOKEN", "SATELLITE_FEED_TOKEN", "PANTAI_AUTH_TOKEN"} {
		if token == os.Getenv(key) {
			return fmt.Errorf("dedicated ingestion token required")
		}
		if reportToken != "" && key != "FIELD_REPORT_SERVICE_TOKEN" && reportToken == os.Getenv(key) {
			return fmt.Errorf("dedicated field report token required")
		}
	}
	flush := platform.Init("ingestion")
	defer func() {
		c, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		_ = flush(c)
	}()
	db, err := store.New(ctx, os.Getenv("INGESTION_DATABASE_URL"), slog.Default())
	if err != nil {
		return err
	}
	defer db.Close()
	if err = db.ConfigureReplication(ctx, os.Getenv("INGESTION_DURABILITY_CONFIG")); err != nil {
		return err
	}
	durability, ok := db.Canonical.Durability.(ingestion.Durability)
	if !ok {
		return canonical.Unavailable()
	}
	authority, err := ownership.NewAcquisition(db.Pool, 15*time.Second, canonical.Deadline)
	if err != nil {
		return err
	}
	authority.Durability = db.Canonical.Durability.(ownership.Durability)
	owned := &ingestion.Store{DB: db, Durability: durability, Fence: authority, Namespace: os.Getenv("INGESTION_NAMESPACE"), Start: start, Quota: quota}
	registry := avrowire.NewClient(os.Getenv("SCHEMA_REGISTRY_URL"))
	registry.SetCache(store.SchemaCache{Pool: db.Pool})
	schema, err := os.ReadFile(os.Getenv("RAW_INPUT_SCHEMA"))
	if err != nil {
		return err
	}
	rawReader, err := avrowire.NewReader(string(schema))
	if err != nil {
		return err
	}
	nc, err := nats.Connect(os.Getenv("NATS_URL"), nats.Timeout(2*time.Second))
	if err != nil {
		return fmt.Errorf("broker unavailable")
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	streams := []rawinput.Stream{}
	if enabled["bmkg"] {
		streams = append(streams, rawinput.BMKG)
	}
	if enabled["field-reports"] {
		streams = append(streams, rawinput.Reports)
	}
	if enabled["coastal"] {
		streams = append(streams, rawinput.Coastal, rawinput.Critical)
	}
	ensure := func(ctx context.Context) (map[string]jetstream.Consumer, error) {
		consumers := map[string]jetstream.Consumer{}
		for _, s := range streams {
			check, stop := context.WithTimeout(ctx, canonical.Deadline)
			c, err := rawinput.Ensure(check, js, s, replicas)
			stop()
			if err != nil {
				return nil, err
			}
			consumers[s.Name] = c
		}
		return consumers, nil
	}
	consumers, err := ensure(ctx)
	if err != nil {
		return err
	}
	coordinator, err := acquisition.New(authority, slog.Default())
	if err != nil {
		return err
	}
	coordinator.Ready = func() bool {
		c, stop := context.WithTimeout(ctx, canonical.Deadline)
		defer stop()
		return owned.Check(c) == nil && db.CheckDurability(c) == nil
	}
	sender := rawinput.Sender{JS: js, Registry: registry, SchemaText: string(schema)}
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	if enabled["bmkg"] {
		seismic, err := avrowire.LoadSourceReader(os.Getenv("BMKG_SEISMIC_READER_SCHEMA"), "SeismicHistoryPage")
		if err != nil {
			return err
		}
		warnings, err := avrowire.LoadSourceReader(os.Getenv("BMKG_TSUNAMI_READER_SCHEMA"), "WarningHistoryPage")
		if err != nil {
			return err
		}
		worker := &ingestion.Worker{Store: owned, Registry: registry, SchemaText: string(schema), Readers: map[string]*avrowire.SourceReader{"seismic-events": seismic, "tsunami-warnings": warnings}, JS: js, HTTP: platform.Client(2 * time.Second), BaseURL: os.Getenv("BMKG_BASE_URL"), APIKey: os.Getenv("BMKG_API_KEY"), Interval: time.Second, Logger: slog.Default()}
		go coordinator.Run(ctx, "BMKG", worker.Run)
	}
	if enabled["field-reports"] {
		host, _ := os.Hostname()
		ledger := &fieldreport.Ledger{DB: db, Durability: durability, Sender: sender, Namespace: os.Getenv("FIELD_REPORT_NAMESPACE"), Holder: "ingestion-" + host}
		consumer := &fieldreport.Consumer{Ledger: ledger, Stream: consumers[rawinput.Reports.Name], Registry: registry, Reader: rawReader, Logger: slog.Default()}
		go ledger.RunOutbox(ctx)
		go consumer.Run(ctx)
		fieldreport.HTTP{Ledger: ledger, Token: reportToken}.Register(r)
	}
	if enabled["coastal"] {
		if err = startCoastal(ctx, r, coordinator, db, durability, authority, sender, registry, rawReader, consumers, replicas, js, token); err != nil {
			return err
		}
	}
	r.GET("/health", func(c *gin.Context) { c.JSON(200, gin.H{"status": "ok"}) })
	r.GET("/ready", func(c *gin.Context) {
		if _, err := ensure(c.Request.Context()); err != nil || !coordinator.Ready() {
			rawinput.Error(c, canonical.Unavailable())
			return
		}
		c.JSON(200, gin.H{"status": "ready"})
	})
	r.GET("/metrics", gin.WrapF(platform.Metrics))
	ingestion.HTTP{Store: owned, ServiceToken: token, Signer: signer, AuthURL: os.Getenv("AUTH_BASE_URL"), AuthKey: os.Getenv("AUTH_INTERNAL_KEY"), Client: platform.Client(2 * time.Second), LegacyURL: os.Getenv("AGGREGATOR_BASE_URL")}.Register(r)
	return platform.Serve(ctx, &http.Server{Addr: ":" + os.Getenv("INGESTION_PORT"), Handler: platform.Handler("ingestion", r), ReadHeaderTimeout: 2 * time.Second})
}
