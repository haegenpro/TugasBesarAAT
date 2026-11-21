package store_test

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/clients"
	"aggregator/internal/domain"
	"aggregator/internal/m2api"
	"aggregator/internal/ownership"
	"aggregator/internal/store"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	urlpkg "net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func m2Store(t *testing.T) *store.Store {
	t.Helper()
	url := os.Getenv("M2_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("M2_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	admin, e := pgx.Connect(ctx, url)
	require.NoError(t, e)
	t.Cleanup(func() { admin.Close(context.Background()) })
	schema := "canonical_test_" + uuid.NewString()[:8]
	_, e = admin.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize())
	require.NoError(t, e)
	t.Cleanup(func() {
		_, e := admin.Exec(context.Background(), `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
		if e != nil {
			t.Error(e)
		}
	})
	parsed, e := urlpkg.Parse(url)
	require.NoError(t, e)
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	addresses := map[string]string{}
	if raw := os.Getenv("M2_TEST_WORKER_ADDRESSES"); raw != "" {
		require.NoError(t, json.Unmarshal([]byte(raw), &addresses))
	}
	configure := func(cfg *pgxpool.Config) {
		dialer := net.Dialer{Timeout: time.Second}
		cfg.ConnConfig.LookupFunc = func(ctx context.Context, host string) ([]string, error) {
			if _, ok := addresses[net.JoinHostPort(host, "5432")]; ok {
				return []string{host}, nil
			}
			return net.DefaultResolver.LookupHost(ctx, host)
		}
		cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
			if mapped, ok := addresses[address]; ok {
				address = mapped
			}
			return dialer.DialContext(ctx, network, address)
		}
	}
	workerReads := os.Getenv("M2_TEST_WORKER_READS") == "true"
	t.Setenv("M2_WORKER_READS", strconv.FormatBool(workerReads))
	s, e := store.New(ctx, parsed.String(), slog.New(slog.NewJSONHandler(io.Discard, nil)), configure)
	require.NoError(t, e)
	t.Cleanup(s.Close)
	for _, name := range []string{"init.sql", "m2.sql"} {
		raw, e := os.ReadFile(filepath.Join("..", "..", "..", "store", name))
		require.NoError(t, e)
		_, e = s.Pool.Exec(ctx, string(raw))
		require.NoError(t, e)
	}
	s.Distributed = true
	s.WorkerReads = workerReads

	return s
}
func TestM2CitusFencedDedup(t *testing.T) {
	s := m2Store(t)
	ctx := context.Background()
	e := sampleEvent(domain.SourceBMKG, "m2-fence-"+uuid.NewString())
	_, err := s.SaveDedup(ctx, e, "fenced")
	require.ErrorIs(t, err, ownership.ErrFenced)
	a := ownership.New(s.Pool)
	require.NoError(t, a.Run(ctx, "test:"+e.SourceRefID, func(c context.Context) error {
		ok, err := s.SaveDedup(c, e, "owned")
		require.True(t, ok)
		if err != nil {
			return err
		}
		ok, err = s.SaveDedup(c, e, "duplicate")
		require.False(t, ok)
		return err
	}))
	got, err := s.GetByID(ctx, e.HazardID)
	require.NoError(t, err)
	require.Equal(t, e.SourceRefID, got.SourceRefID)
	var workers int
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(DISTINCT n.groupid) FROM pg_dist_placement p JOIN pg_dist_node n USING(groupid) JOIN pg_dist_shard s USING(shardid) WHERE s.logicalrelid='hazard_events'::regclass AND n.noderole='primary'`).Scan(&workers))
	require.Equal(t, 2, workers)
}
func TestM2CommitExpiryAndCompetingHolder(t *testing.T) {
	s := m2Store(t)
	ctx := context.Background()
	scope := "test:" + uuid.NewString()
	a := ownership.New(s.Pool)
	other := ownership.New(s.Pool)
	require.NoError(t, a.Run(ctx, scope, func(c context.Context) error {
		require.ErrorIs(t, other.Run(ctx, scope, func(context.Context) error { return nil }), ownership.ErrBusy)
		tx, err := s.Pool.Begin(c)
		require.NoError(t, err)
		defer tx.Rollback(ctx)
		require.NoError(t, ownership.Guard(c, tx))
		_, err = tx.Exec(c, `UPDATE source_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE scope=$1`, scope)
		require.NoError(t, err)
		require.Error(t, tx.Commit(c), "deferred guard must reject an expired commit")
		return nil
	}))
	_, err := s.Pool.Exec(ctx, `UPDATE source_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE scope=$1`, scope)
	require.NoError(t, err)
	started := time.Now()
	require.NoError(t, other.Run(ctx, scope, func(c context.Context) error {
		tx, e := s.Pool.Begin(c)
		if e != nil {
			return e
		}
		defer tx.Rollback(ctx)
		if e = ownership.Guard(c, tx); e != nil {
			return e
		}
		return tx.Commit(c)
	}))
	require.Less(t, time.Since(started), time.Minute)
}

func TestM2CanonicalBatchRetractionAndAudit(t *testing.T) {
	s := m2Store(t)
	ctx := context.Background()
	ref := "fire:" + uuid.NewString()
	h := sampleEvent(domain.SourceSatelit, ref)
	h.HazardType = domain.HazardWildfire
	h.Attributes = map[string]any{"detection_count": 2, "detection_keys": []string{"A|2026-11-12|1|0|0", "B|2026-11-12|1|0|0"}, "satellites": []string{"A", "B"}, "max_confidence": "h", "total_frp_mw": 20}
	b := store.CanonicalBatch{ID: uuid.NewString(), Source: "SATELIT", Namespace: uuid.NewString(), Previous: -1, Position: 0, Changes: []store.CanonicalChange{{Ref: ref, Event: &h}}}
	require.NoError(t, s.ApplyCanonical(ctx, b, "batch"))
	require.NoError(t, s.ApplyCanonical(ctx, b, "duplicate"))
	original, e := s.GetByID(ctx, uuid.NewSHA1(uuid.NameSpaceOID, []byte("SATELIT\x00"+ref)).String())
	require.NoError(t, e)
	page, e := s.AuditEvents(ctx, store.AuditQuery{Source: "SATELIT", Since: original.IngestedAt.Time, Limit: 1})
	require.NoError(t, e)
	require.Len(t, page.Items, 1)
	require.False(t, page.Partial)
	checksum, e := s.AuditChecksum(ctx, "SATELIT")
	require.NoError(t, e)
	sum := sha256.Sum256([]byte(ref))
	require.Equal(t, hex.EncodeToString(sum[:]), checksum["checksum"])
	require.EqualValues(t, 1, checksum["count"])
	location, e := s.AuditLocation(ctx, original.HazardID, "node-a", "node-b")
	require.NoError(t, e)
	require.Len(t, location["locations"], 2)
	changed := b
	changed.Changes = nil
	require.ErrorIs(t, s.ApplyCanonical(ctx, changed, "identity-conflict"), store.ErrOrder)
	retraction := store.CanonicalBatch{ID: uuid.NewString(), Source: "SATELIT", Namespace: b.Namespace, Previous: 0, Position: 1, Changes: []store.CanonicalChange{{Ref: ref, Delete: true}}}
	require.NoError(t, s.ApplyCanonical(ctx, retraction, "delete"))
	_, e = s.GetByID(ctx, original.HazardID)
	require.ErrorIs(t, e, store.ErrNotFound)
	require.NoError(t, s.ApplyCanonical(ctx, b, "old-delivery"))
	_, e = s.GetByID(ctx, original.HazardID)
	require.ErrorIs(t, e, store.ErrNotFound)
	b.ID = uuid.NewString()
	b.Previous = 1
	b.Position = 2
	require.NoError(t, s.ApplyCanonical(ctx, b, "restore"))
	restored, e := s.GetByID(ctx, original.HazardID)
	require.NoError(t, e)
	require.Equal(t, original.HazardID, restored.HazardID)
}
func TestM2VisibilityRecoversUnpublishedCommit(t *testing.T) {
	s := m2Store(t)
	ctx := context.Background()
	h := sampleEvent(domain.SourceBMKG, uuid.NewString())
	authority := ownership.New(s.Pool)
	require.NoError(t, authority.Run(ctx, "test:visibility", func(c context.Context) error { _, e := s.SaveDedup(c, h, "visibility"); return e }))
	_, e := s.Pool.Exec(ctx, `UPDATE canonical_visibility SET published=false`)
	require.NoError(t, e)
	page, e := s.QueryPage(ctx, store.Filters{}, store.PageRequest{Limit: 100})
	require.NoError(t, e)
	require.True(t, page.Partial)
	require.Empty(t, page.Events)
	recovery, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { s.RunVisibilityRecovery(recovery); close(done) }()
	defer func() { cancel(); <-done }()
	require.Eventually(t, func() bool { _, e := s.GetByID(ctx, h.HazardID); return e == nil }, 5*time.Second, 25*time.Millisecond)
}
func TestM2CompleteQuarantineSeparatesOriginalIdentity(t *testing.T) {
	s := m2Store(t)
	ctx := context.Background()
	authority := ownership.New(s.Pool)
	payload := bytes.Repeat([]byte("original\x00bytes\n"), 12000)
	require.NoError(t, authority.Run(ctx, "PVMBG", func(c context.Context) error {
		for _, identity := range []string{"source-record-1", "source-record-2", "source-record-1"} {
			failure := clients.Failure{Identity: identity, Source: "PVMBG", Entity: "/volcanic-reports", PayloadStream: bytes.NewReader(payload), Truncated: true, Reason: "oversized fixture", CorrelationID: "quarantine"}
			if e := s.RecordFailure(c, failure); e != nil {
				return e
			}
		}
		return nil
	}))
	rows, e := s.Pool.Query(ctx, `SELECT message_id::text,attempts,truncated FROM failed_ingestions ORDER BY original_identity`)
	require.NoError(t, e)
	var ids []string
	var attempts []int64
	for rows.Next() {
		var id string
		var n int64
		var truncated bool
		require.NoError(t, rows.Scan(&id, &n, &truncated))
		require.False(t, truncated)
		ids = append(ids, id)
		attempts = append(attempts, n)
	}
	rows.Close()
	require.Len(t, ids, 2)
	require.Equal(t, []int64{2, 1}, attempts)
	for _, id := range ids {
		var out bytes.Buffer
		n, _, _, e := s.WriteFailurePayload(ctx, id, &out)
		require.NoError(t, e)
		require.Equal(t, int64(len(payload)), n)
		require.Equal(t, payload, out.Bytes())
	}
}

func TestM2SharedPolicyAndParkedReplayFence(t *testing.T) {
	s := m2Store(t)
	ctx := context.Background()
	require.NoError(t, clients.SetVolcanicAdapter(1))
	defer clients.SetVolcanicAdapter(1)
	baseline := filepath.Join("..", "..", "schemas", "volcanic-v1.avsc")
	reader, e := avrowire.LoadSourceReader(baseline, "VolcanicHistoryPage")
	require.NoError(t, e)
	readers, e := avrowire.NewReaderSwitch(reader, "VolcanicHistoryPage")
	require.NoError(t, e)
	require.NoError(t, s.SetPolicy(ctx, 2))
	authority := ownership.New(s.Pool)
	require.NoError(t, authority.Run(ctx, "PVMBG", func(c context.Context) error {
		require.Error(t, s.CommitVolcanic(c, nil, nil, "obsolete-adapter", 0))
		return nil
	}))
	require.NoError(t, s.SyncPolicy(ctx, readers, baseline))
	require.Equal(t, 2, clients.VolcanicAdapter())
	require.NoError(t, authority.Run(ctx, "PVMBG", func(c context.Context) error {
		if e := s.CommitVolcanic(c, nil, nil, "approved-adapter", 0); e != nil {
			return e
		}
		return s.RecordFailure(c, clients.Failure{Source: "PVMBG", Entity: "/volcanic-reports", Identity: "original-response", Payload: []byte("original bytes"), Reason: "breaking schema"})
	}))
	require.Error(t, s.BlockParkedVolcanic(ctx, "PVMBG"))
	var id string
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT message_id::text FROM failed_ingestions`).Scan(&id))
	_, e = s.RequestReplay(ctx, []string{id, id})
	require.Error(t, e)
	selected, e := s.RequestReplay(ctx, []string{id})
	require.NoError(t, e)
	require.EqualValues(t, 1, selected)
	retained, e := s.NextLegacyReplay(ctx)
	require.NoError(t, e)
	require.Equal(t, []byte("original bytes"), retained.Payload)
	require.NoError(t, authority.Run(ctx, "PVMBG", func(c context.Context) error {
		require.Error(t, s.CommitVolcanic(c, nil, nil, "must-wait", 0))
		return nil
	}))
	require.NoError(t, authority.Run(ctx, "replay:volcanic", func(c context.Context) error {
		if e := s.CommitVolcanic(c, nil, nil, "retained-replay", 0); e != nil {
			return e
		}
		return s.FinishLegacyReplay(c, id, nil)
	}))
	require.NoError(t, s.BlockParkedVolcanic(ctx, "PVMBG"))
}

func TestM2CatalogUsesOwnerAndStableSourceIdentity(t *testing.T) {
	s := m2Store(t)
	router := gin.New()
	m2api.Register(router, s, m2api.Config{AdminToken: "private-import-owner"})
	server := httptest.NewServer(router)
	defer server.Close()
	event := sampleEvent(domain.SourceBMKG, "catalog-original-ref")
	body, e := json.Marshal(map[string]any{"events": []domain.HazardEvent{event}})
	require.NoError(t, e)
	for _, authenticated := range []bool{false, true, true} {
		req, e := http.NewRequest("POST", server.URL+"/internal/ops/catalog", bytes.NewReader(body))
		require.NoError(t, e)
		req.Header.Set("Content-Type", "application/json")
		if authenticated {
			req.Header.Set("Authorization", "Bearer private-import-owner")
		}
		response, e := server.Client().Do(req)
		require.NoError(t, e)
		response.Body.Close()
		if authenticated {
			require.Equal(t, 200, response.StatusCode)
		} else {
			require.Equal(t, 401, response.StatusCode)
		}
	}
	page, e := s.AuditEvents(context.Background(), store.AuditQuery{Source: "BMKG", Limit: 100})
	require.NoError(t, e)
	require.Len(t, page.Items, 1)
	require.Equal(t, "catalog-original-ref", page.Items[0].SourceRefID)
	require.Equal(t, uuid.NewSHA1(uuid.NameSpaceOID, []byte("BMKG\x00catalog-original-ref")).String(), page.Items[0].HazardID)
}
