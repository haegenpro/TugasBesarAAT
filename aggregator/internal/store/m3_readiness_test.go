package store

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"aggregator/internal/ownership"
	"aggregator/internal/timeutil"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestM3SchemaCacheSurvivesClientRestartAndOutage(t *testing.T) {
	dsn, c := m2Database(t, false)
	m2Schema(t, c, "postgres", false)
	s := m2Store(t, dsn)
	ctx := context.Background()
	text := `{"type":"record","name":"CacheFixture","fields":[{"name":"value","type":"string"}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"schema": text})
	}))
	first := avrowire.NewClient(server.URL)
	first.SetCache(SchemaCache{Pool: s.Pool})
	known, err := first.Lookup(ctx, 41)
	require.NoError(t, err)
	frame, err := avrowire.Encode(known, map[string]any{"value": "original|雪"})
	require.NoError(t, err)
	server.Close()
	// A new client has no process cache. Only the actual database cache is left.
	restarted := avrowire.NewClient(server.URL)
	restarted.SetCache(SchemaCache{Pool: s.Pool})
	value, err := restarted.Decode(ctx, frame)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"value": "original|雪"}, value)
	registered, err := restarted.Register(ctx, "cache-fixture-value", text)
	require.NoError(t, err)
	require.Equal(t, 41, registered.ID)
	_, err = restarted.Lookup(ctx, 42)
	require.ErrorIs(t, err, avrowire.ErrRegistryUnavailable)
	require.Error(t, (SchemaCache{Pool: s.Pool}).Put(ctx, 41, `"string"`))
	_, err = c.Exec(ctx, `UPDATE input_schema_cache SET sha256='corrupted' WHERE schema_id=41`)
	require.NoError(t, err)
	corrupted := avrowire.NewClient(server.URL)
	corrupted.SetCache(SchemaCache{Pool: s.Pool})
	_, err = corrupted.Lookup(ctx, 41)
	require.Error(t, err, "unverified cached schema must not be used")
}

func TestM3OfflineCutoverIsRestartableAndPreservesCanonicalWork(t *testing.T) {
	dsn, c := m2Database(t, false)
	m2Schema(t, c, "postgres", false)
	s := m2Store(t, dsn)
	ctx := context.Background()
	event := domain.HazardEvent{HazardID: "00000000-0000-4000-8000-000000000097", Source: domain.SourceBMKG, SourceRefID: "legacy-quake", HazardType: domain.HazardSeismic, Severity: domain.SeverityWaspada, AreaName: "Fixture", Latitude: -6, Longitude: 107, OccurredAt: timeutil.New(domain.Now()), IngestedAt: timeutil.New(domain.Now()), Attributes: map[string]any{"original": true}}
	_, err := s.SaveDedup(ctx, event, "cutover-fixture")
	require.NoError(t, err)
	var before, after string
	require.NoError(t, c.QueryRow(ctx, `SELECT row_to_json(e)::text FROM hazard_events e`).Scan(&before))
	var outboxBefore, outboxAfter int
	require.NoError(t, c.QueryRow(ctx, `SELECT count(*) FROM hazard_outbox`).Scan(&outboxBefore))
	processor, err := ownership.NewProcessor(s.Pool, 15*time.Second)
	require.NoError(t, err)
	s.Processor = processor
	s.Canonical.Durability = m3LabDurability{pool: s.Pool}
	lease, err := processor.Acquire(ctx, canonical.AcquireOwnership{Scope: "processor:BMKG", Holder: "cutover"})
	require.NoError(t, err)
	owner := canonical.Ownership{Scope: "fixture", Epoch: 1, Holder: "owner"}
	// m2Store's test fence holds this exact owner in m2_authority.
	for i := 0; i < 2; i++ {
		require.NoError(t, s.CutoverBMKG(ctx, lease.Ownership, owner, "cutover-fixture"))
	}
	require.NoError(t, c.QueryRow(ctx, `SELECT row_to_json(e)::text FROM hazard_events e`).Scan(&after))
	require.JSONEq(t, before, after)
	require.NoError(t, c.QueryRow(ctx, `SELECT count(*) FROM hazard_outbox`).Scan(&outboxAfter))
	require.Equal(t, outboxBefore, outboxAfter)
	var namespace, state string
	require.NoError(t, c.QueryRow(ctx, `SELECT source_namespace FROM canonical_entities`).Scan(&namespace))
	require.Equal(t, "raw:cutover-fixture", namespace)
	require.NoError(t, c.QueryRow(ctx, `SELECT state FROM input_cutovers`).Scan(&state))
	require.Equal(t, "ready", state)
	_, err = s.SaveDedup(ctx, event, "old-writer")
	require.Error(t, err, "legacy writer must not overwrite transitioned identity")
}
