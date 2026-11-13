package store

import (
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"aggregator/internal/migrations"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// These adapters are deliberately test-only. One-copy local tests establish no
// production fencing, lease expiry or replicated durability guarantee.
type labFence struct{}

func (labFence) Protect(ctx context.Context, tx pgx.Tx, o canonical.Ownership, _ []canonical.Mutation) error {
	var epoch int64
	var holder string
	err := tx.QueryRow(ctx, "SELECT epoch,holder FROM m2_authority WHERE scope=$1 FOR SHARE", o.Scope).Scan(&epoch, &holder)
	if err != nil || epoch != o.Epoch || holder != o.Holder {
		return canonical.Fail(403, "stale_owner", "test owner is stale")
	}
	return nil
}

type labDurability struct{}

func (labDurability) Prepare(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, "SET LOCAL synchronous_commit=on")
	return err
}
func (labDurability) Confirm(context.Context, string) error { return nil }
func m2Connection(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	c, e := pgx.Connect(context.Background(), dsn)
	require.NoError(t, e)
	t.Cleanup(func() { c.Close(context.Background()) })
	return c
}
func m2Database(t *testing.T, citus bool) (string, *pgx.Conn) {
	t.Helper()
	raw := os.Getenv("M2_DATABASE_URL")
	if raw == "" {
		t.Skip("M2_DATABASE_URL absent: isolated database proof not executed")
	}
	u, e := url.Parse(raw)
	require.NoError(t, e)
	host, _, e := net.SplitHostPort(u.Host)
	require.NoError(t, e)
	require.Contains(t, []string{"127.0.0.1", "localhost"}, host, "M2 proof requires a loopback-owned lab")
	name := "m2_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	admins := []*pgx.Conn{m2Connection(t, raw)}
	if citus {
		for _, key := range []string{"M2_WORKER1_URL", "M2_WORKER2_URL"} {
			dsn := os.Getenv(key)
			require.NotEmpty(t, dsn)
			admins = append(admins, m2Connection(t, dsn))
		}
	}
	for _, c := range admins {
		_, e = c.Exec(context.Background(), "CREATE DATABASE "+pgx.Identifier{name}.Sanitize())
		require.NoError(t, e)
	}
	u.Path = "/" + name
	dsn := u.String()
	c := m2Connection(t, dsn)
	if citus {
		for i, admin := range admins {
			u2, e := url.Parse(admin.Config().ConnString())
			require.NoError(t, e)
			u2.Path = "/" + name
			node := m2Connection(t, u2.String())
			_, e = node.Exec(context.Background(), "CREATE EXTENSION citus")
			require.NoError(t, e)
			pw, _ := u.User.Password()
			_, e = node.Exec(context.Background(), "INSERT INTO pg_dist_authinfo(nodeid,rolename,authinfo) VALUES(0,'postgres',$1)", "password="+pw)
			require.NoError(t, e)
			if i > 0 {
				_, e = c.Exec(context.Background(), "SELECT citus_add_node($1,5432)", fmt.Sprintf("worker%d", i))
				require.NoError(t, e)
			}
		}
	}
	// Preserve database state inside the run's retained volumes for inspection.
	if dir := os.Getenv("M2_EVIDENCE_DIR"); dir != "" {
		file, e := os.OpenFile(filepath.Join(dir, "databases.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		require.NoError(t, e)
		raw, _ := json.Marshal(map[string]any{"test": t.Name(), "database": name, "citus": citus})
		_, e = file.Write(append(raw, '\n'))
		require.NoError(t, e)
		require.NoError(t, file.Close())
		return dsn, c
	}
	// Cleanup only databases whose exact generated names are held by this test.
	t.Cleanup(func() {
		c.Close(context.Background())
		for _, admin := range admins {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
			cancel()
		}
	})
	return dsn, c
}
func m2Schema(t *testing.T, c *pgx.Conn, target string, upgrade bool) {
	t.Helper()
	ctx := context.Background()
	dir := filepath.Join("..", "..", "..", "store", "migrations")
	if upgrade {
		b, e := os.ReadFile(filepath.Join(dir, "0001_m1.sql"))
		require.NoError(t, e)
		_, e = c.Exec(ctx, string(b))
		require.NoError(t, e)
		_, e = c.Exec(ctx, `INSERT INTO hazard_events(hazard_id,source,source_ref_id,hazard_type,severity,area_name,latitude,longitude,occurred_at,ingested_at,attributes,revision) VALUES('00000000-0000-4000-8000-000000000001','PVMBG','upgrade','VOLCANIC','WASPADA','Upgrade',0,0,'2026-01-01 00:00:00.123456Z','2026-01-02 00:00:00.654321Z','{"keys":["a","b"]}',7);
 INSERT INTO hazard_outbox(hazard_id,event_payload,correlation_id) VALUES('00000000-0000-4000-8000-000000000001','{}','upgrade');
 INSERT INTO source_history_cursors VALUES('PVMBG','volcanic-reports',42);
 INSERT INTO report_lifecycle_events(event_id,record_id,revision,event_payload) VALUES('00000000-0000-4000-8000-000000000002','endpoint',3,'{"event_type":"source_status_changed"}');
 INSERT INTO report_lifecycle_outbox(event_id) VALUES('00000000-0000-4000-8000-000000000002');
 INSERT INTO source_observations(source,entity,record_id,payload,history_sequence) VALUES('PVMBG','volcanic-reports','upgrade','{"original":true}',42);
 INSERT INTO source_poll_state(endpoint,state,revision) VALUES('PVMBG','fresh',8);
 INSERT INTO report_notices VALUES('00000000-0000-4000-8000-000000000003','PVMBG','notice','{"state":"pending"}','pending',4);
 INSERT INTO failed_ingestions(message_id,source,entity,digest,raw_payload,truncated,retryable,reason,status,correlation_id) VALUES('00000000-0000-4000-8000-000000000004','PVMBG','volcanic-reports','digest',decode('00ff','hex'),false,true,'fixture','retrying','fixture');
 INSERT INTO report_lifecycle_events(event_id,record_id,revision,event_payload) VALUES('00000000-0000-4000-8000-000000000005','00000000-0000-4000-8000-000000000001',7,'{"event_type":"hazard_corrected","revision":7,"hazard_revision":7}');
 INSERT INTO report_lifecycle_outbox(event_id) VALUES('00000000-0000-4000-8000-000000000005')`)
		require.NoError(t, e)
	}
	require.NoError(t, migrations.Up(ctx, c, dir, target, ""))
	require.NoError(t, migrations.Up(ctx, c, dir, target, ""))
	if upgrade {
		var rev, cursor, initial, lifecycle int
		var id, at string
		require.NoError(t, c.QueryRow(ctx, "SELECT hazard_id::text,revision,to_char(ingested_at AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS.US') FROM hazard_events WHERE source_ref_id='upgrade'").Scan(&id, &rev, &at))
		require.Equal(t, "00000000-0000-4000-8000-000000000001", id)
		require.Equal(t, 7, rev)
		require.Equal(t, "2026-01-02 00:00:00.654321", at)
		require.NoError(t, c.QueryRow(ctx, "SELECT cursor FROM source_history_cursors WHERE source='PVMBG'").Scan(&cursor))
		require.Equal(t, 42, cursor)
		require.NoError(t, c.QueryRow(ctx, "SELECT count(*) FROM hazard_outbox").Scan(&initial))
		require.Equal(t, 1, initial)
		require.NoError(t, c.QueryRow(ctx, "SELECT count(*) FROM report_lifecycle_outbox").Scan(&lifecycle))
		require.Equal(t, 1, lifecycle)
		var moved, observations, notices, failures int
		var bytes string
		require.NoError(t, c.QueryRow(ctx, "SELECT (SELECT count(*) FROM canonical_lifecycle_outbox),(SELECT count(*) FROM source_observations),(SELECT count(*) FROM report_notices),(SELECT count(*) FROM failed_ingestions),(SELECT encode(raw_payload,'hex') FROM failed_ingestions)").Scan(&moved, &observations, &notices, &failures, &bytes))
		require.Equal(t, 1, moved)
		require.Equal(t, 1, observations)
		require.Equal(t, 1, notices)
		require.Equal(t, 1, failures)
		require.Equal(t, "00ff", bytes)
	}
}
func m2Store(t *testing.T, dsn string) *Store {
	t.Helper()
	var output io.Writer = io.Discard
	if dir := os.Getenv("M2_EVIDENCE_DIR"); dir != "" {
		file, err := os.OpenFile(filepath.Join(dir, "sql-"+strings.NewReplacer("/", "-", "\\", "-").Replace(t.Name())+".jsonl"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		require.NoError(t, err)
		t.Cleanup(func() { file.Close() })
		output = file
	}
	s, e := New(context.Background(), dsn, slog.New(slog.NewJSONHandler(output, nil)))
	require.NoError(t, e)
	t.Cleanup(s.Close)
	_, e = s.Pool.Exec(context.Background(), "CREATE TABLE m2_authority(scope text PRIMARY KEY,epoch bigint,holder text); INSERT INTO m2_authority VALUES('fixture',1,'owner')")
	require.NoError(t, e)
	s.Canonical = CanonicalOptions{Fence: labFence{}, Durability: labDurability{}, MultiEntityValidated: true}
	return s
}
func m2Batch(ref string, pos, rev int64) canonical.Batch {
	return canonical.Batch{ContractVersion: 1, BatchID: uuid.NewString(), Ownership: canonical.Ownership{Scope: "fixture", Epoch: 1, Holder: "owner"}, Mutations: []canonical.Mutation{{OperationID: uuid.NewString(), Source: domain.SourceBMKG, SourceRefID: ref, Operation: "upsert", SourceOrder: canonical.SourceOrder{Namespace: "fixture-v1", Position: pos}, ExpectedRevision: rev, Event: &canonical.MutableEvent{HazardType: domain.HazardSeismic, Severity: domain.SeverityWaspada, AreaName: "Fixture", Latitude: 0, Longitude: 0, OccurredAt: time.Date(2026, 1, 1, 0, 0, 0, 123456000, time.UTC), Attributes: map[string]any{"value": json.Number("9007199254740993")}}}}}
}
func m2Apply(t *testing.T, s *Store, b canonical.Batch) canonical.Result {
	t.Helper()
	r, e := m2Retry(s, b)
	require.NoError(t, e)
	return r
}

// Simulated durable caller retries only an unavailable/unknown outcome using
// the unchanged batch and operation identities. The adapter deadline stays 2s.
func m2Retry(s *Store, b canonical.Batch) (canonical.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		r, e := s.ApplyBatch(ctx, b)
		if e == nil {
			return r, nil
		}
		var p *canonical.Error
		if !errors.As(e, &p) || p.Status != 503 {
			return r, e
		}
		if ctx.Err() != nil {
			return r, e
		}
		_ = s.RecoverAttempts(ctx)
		select {
		case <-ctx.Done():
			return r, e
		case <-time.After(10 * time.Millisecond):
		}
	}
}
func m2Error(t *testing.T, err error, code string) {
	t.Helper()
	var p *canonical.Error
	require.ErrorAs(t, err, &p)
	require.Equal(t, code, p.Code)
}
func TestM2Migrations(t *testing.T) {
	for _, target := range []string{"postgres", "citus"} {
		for _, upgrade := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s_upgrade_%v", target, upgrade), func(t *testing.T) {
				_, c := m2Database(t, target == "citus")
				m2Schema(t, c, target, upgrade)
				if target == "citus" {
					var n int
					require.NoError(t, c.QueryRow(context.Background(), "SELECT count(*) FROM pg_dist_partition WHERE logicalrelid IN ('canonical_entities'::regclass,'hazard_events'::regclass,'hazard_outbox'::regclass,'canonical_lifecycle_events'::regclass,'canonical_lifecycle_outbox'::regclass)").Scan(&n))
					require.Equal(t, 5, n)
					var heap int64
					require.NoError(t, c.QueryRow(context.Background(), "SELECT sum(pg_relation_size(logicalrelid)) FROM pg_dist_partition WHERE partmethod='h'").Scan(&heap))
					require.Zero(t, heap, "distributed rows must not retain a physical coordinator heap")

				}
			})
		}
	}
	t.Run("runtime_grants", func(t *testing.T) {
		dsn, c := m2Database(t, false)
		role := "runtime_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		_, e := c.Exec(context.Background(), "CREATE ROLE "+pgx.Identifier{role}.Sanitize()+" NOLOGIN")
		require.NoError(t, e)
		require.NoError(t, migrations.Up(context.Background(), c, filepath.Join("..", "..", "..", "store", "migrations"), "postgres", role))
		restricted := m2Connection(t, dsn)
		_, e = restricted.Exec(context.Background(), "SET ROLE "+pgx.Identifier{role}.Sanitize())
		require.NoError(t, e)
		_, e = restricted.Exec(context.Background(), "SELECT route_key,active FROM hazard_events LIMIT 0")
		require.NoError(t, e)
		_, e = restricted.Exec(context.Background(), "ALTER TABLE hazard_events ADD forbidden int")
		require.Error(t, e)
	})
	t.Run("reject_partial", func(t *testing.T) {
		_, c := m2Database(t, false)
		_, e := c.Exec(context.Background(), "CREATE TABLE hazard_events(hazard_id uuid)")
		require.NoError(t, e)
		require.Error(t, migrations.Up(context.Background(), c, filepath.Join("..", "..", "..", "store", "migrations"), "postgres", ""))
	})
}
func TestM2WriteCore(t *testing.T) {
	dsn, c := m2Database(t, true)
	m2Schema(t, c, "citus", false)
	s := m2Store(t, dsn)
	ctx := context.Background()
	b := m2Batch("delimiter:|\t雪\u2028<", 1, 0)
	first := m2Apply(t, s, b)
	require.Equal(t, int64(1), first.Items[0].Revision)
	id := first.Items[0].HazardID
	again := b
	again.Ownership.Epoch = 99
	duplicate := m2Apply(t, s, again)
	require.Equal(t, "duplicate", duplicate.Outcome)
	conflict := b
	conflict.Mutations = append([]canonical.Mutation(nil), b.Mutations...)
	conflict.Mutations[0].ExpectedRevision = 7
	_, e := s.ApplyBatch(ctx, conflict)
	m2Error(t, e, "identity_conflict")
	current, e := s.Reconcile(ctx, domain.SourceBMKG, b.Mutations[0].SourceRefID)
	require.NoError(t, e)
	ingested := current.Event.IngestedAt.Time
	require.Equal(t, json.Number("9007199254740993"), current.Event.Attributes["value"])
	stale := m2Batch(b.Mutations[0].SourceRefID, 0, 1)
	_, e = s.ApplyBatch(ctx, stale)
	m2Error(t, e, "stale_operation")
	same := m2Batch(b.Mutations[0].SourceRefID, 2, 1)
	require.Equal(t, "no_change", m2Apply(t, s, same).Outcome)
	correction := m2Batch(b.Mutations[0].SourceRefID, 3, 1)
	correction.Mutations[0].Event.Attributes["value"] = json.Number("9007199254740992")
	require.Equal(t, int64(2), m2Apply(t, s, correction).Items[0].Revision)
	retract := m2Batch(b.Mutations[0].SourceRefID, 4, 2)
	retract.Mutations[0].Operation = "retract"
	retract.Mutations[0].Event = nil
	require.Equal(t, int64(3), m2Apply(t, s, retract).Items[0].Revision)
	inactive, e := s.Reconcile(ctx, domain.SourceBMKG, b.Mutations[0].SourceRefID)
	require.NoError(t, e)
	require.False(t, inactive.Active)
	require.Nil(t, inactive.Event)
	_, _, e = s.GetByIDWithMetadata(ctx, id)
	require.ErrorIs(t, e, ErrNotFound)
	restore := m2Batch(b.Mutations[0].SourceRefID, 5, 3)
	require.Equal(t, int64(4), m2Apply(t, s, restore).Items[0].Revision)
	current, e = s.Reconcile(ctx, domain.SourceBMKG, b.Mutations[0].SourceRefID)
	require.NoError(t, e)
	require.Equal(t, id, current.HazardID)
	require.True(t, ingested.Equal(current.Event.IngestedAt.Time))
	batch := m2Batch("atomic-first", 1, 0)
	bad := m2Batch(b.Mutations[0].SourceRefID, 6, 123).Mutations[0]
	batch.Mutations = append(batch.Mutations, bad)
	_, e = s.ApplyBatch(ctx, batch)
	m2Error(t, e, "revision_conflict")
	_, e = s.Reconcile(ctx, domain.SourceBMKG, "atomic-first")
	m2Error(t, e, "not_found")
	unknown := m2Batch("unknown-retract", 1, 0)
	unknown.Mutations[0].Operation = "retract"
	unknown.Mutations[0].Event = nil
	u := m2Apply(t, s, unknown)
	require.Equal(t, int64(1), u.Items[0].Revision)
	old := m2Batch("unknown-retract", 0, 1)
	_, e = s.ApplyBatch(ctx, old)
	m2Error(t, e, "stale_operation")
	legacy := domain.HazardEvent{Source: domain.SourceBMKG, SourceRefID: b.Mutations[0].SourceRefID}
	_, e = s.SaveDedup(ctx, legacy, "legacy")
	require.Error(t, e)
	wrong := m2Batch(b.Mutations[0].SourceRefID, 6, 4)
	wrong.Mutations[0].SourceOrder.Namespace = "new"
	_, e = s.ApplyBatch(ctx, wrong)
	m2Error(t, e, "stale_operation")
	require.NoError(t, s.TransitionNamespace(ctx, b.Ownership, domain.SourceBMKG, b.Mutations[0].SourceRefID, "fixture-v1", "new"))
	m2Apply(t, s, wrong)
	staleOwner := m2Batch("stale-owner", 1, 0)
	staleOwner.Ownership.Epoch = 2
	_, e = s.ApplyBatch(ctx, staleOwner)
	m2Error(t, e, "stale_owner")
	_, e = c.Exec(ctx, "INSERT INTO canonical_entities(route_key,hazard_id,source,source_ref_id,revision,active,ingested_at) VALUES('mismatch',$1,'BMKG','wrong',1,true,now())", uuid.NewString())
	require.Error(t, e)
	for _, ref := range []string{"a|b:c", "雪<&>\u2028\u2029", "a\t\n\"\\b"} {
		var got string
		require.NoError(t, c.QueryRow(ctx, "SELECT canonical_route('BMKG',$1)", ref).Scan(&got))
		want, e := canonical.Route("BMKG", ref)
		require.NoError(t, e)
		require.Equal(t, want, got)
	}
	reused := m2Batch("reuse-other-entity", 1, 0)
	reused.Mutations[0].OperationID = b.Mutations[0].OperationID
	_, e = s.ApplyBatch(ctx, reused)
	m2Error(t, e, "identity_conflict")
	t.Run("concurrent_duplicates", func(t *testing.T) {
		batch := m2Batch("concurrent", 1, 0)
		var wg sync.WaitGroup
		errs := make(chan error, 16)
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); _, err := m2Retry(s, batch); errs <- err }()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		var n int
		require.NoError(t, c.QueryRow(ctx, "SELECT count(*) FROM canonical_entities WHERE source_ref_id='concurrent'").Scan(&n))
		require.Equal(t, 1, n)
	})
	t.Run("publication_lifecycle", func(t *testing.T) {
		count := 0
		for {
			found, err := s.DeliverLifecycle(ctx, func(_ context.Context, event domain.LifecycleEvent) error {
				require.NotEmpty(t, event.EventID)
				count++
				return nil
			})
			require.NoError(t, err)
			if !found {
				break
			}
		}
		require.Greater(t, count, 0)
	})
	t.Run("production_fail_closed", func(t *testing.T) {
		opts := s.Canonical
		s.Canonical = CanonicalOptions{}
		defer func() { s.Canonical = opts }()
		_, e := s.ApplyBatch(ctx, m2Batch("no-provider", 1, 0))
		m2Error(t, e, "temporarily_unavailable")
	})
	if dir := os.Getenv("M2_EVIDENCE_DIR"); dir != "" {
		rows, err := c.Query(ctx, "SELECT p.logicalrelid::text,p.colocationid,s.shardid,n.nodename FROM pg_dist_partition p JOIN pg_dist_shard s USING(logicalrelid) JOIN pg_dist_placement place USING(shardid) JOIN pg_dist_node n USING(groupid) WHERE p.partmethod='h' ORDER BY 1,3")
		require.NoError(t, err)
		placements := []any{}
		for rows.Next() {
			v, err := rows.Values()
			require.NoError(t, err)
			placements = append(placements, v)
		}
		require.NoError(t, rows.Err())
		rows.Close()
		var digest string
		require.NoError(t, c.QueryRow(ctx, "SELECT md5(coalesce(string_agg(to_jsonb(e)::text,E'\\n' ORDER BY source,source_ref_id),'')) FROM canonical_entities e").Scan(&digest))
		raw, err := json.MarshalIndent(map[string]any{"placements": placements, "entity_state_md5_diagnostic": digest}, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "catalog.json"), raw, 0600))
	}
	t.Log("single-copy lab adapters only; production authority and durability remain blocked")
}
func TestM2Visibility(t *testing.T) {
	dsn, c := m2Database(t, true)
	m2Schema(t, c, "citus", false)
	s := m2Store(t, dsn)
	ctx := context.Background()
	a, b := m2Batch("visibility-a", 1, 0), m2Batch("visibility-b", 1, 0)
	// Find two distinct actual storage owners through Citus catalogs.
	owner := func(ref string) string {
		var node string
		require.NoError(t, c.QueryRow(ctx, `SELECT n.nodename FROM pg_dist_placement p JOIN pg_dist_node n USING(groupid) WHERE p.shardid=get_shard_id_for_distribution_column('hazard_events',canonical_route('BMKG',$1)) AND n.noderole='primary'`, ref).Scan(&node))
		return node
	}
	for i := 0; owner(a.Mutations[0].SourceRefID) == owner(b.Mutations[0].SourceRefID); i++ {
		require.Less(t, i, 100)
		b = m2Batch(fmt.Sprintf("visibility-b-%d", i), 1, 0)
	}
	ra, rb := m2Apply(t, s, a), m2Apply(t, s, b)
	readOne := func(ctx context.Context, ref string) (string, error) {
		var severity string
		e := s.Pool.QueryRow(ctx, "SELECT severity FROM hazard_events WHERE route_key=$1 AND source_ref_id=$2", mustRoute(domain.SourceBMKG, ref), ref).Scan(&severity)
		return severity, e
	}
	makeCorrection := func(pos, rev int64, severity domain.Severity) canonical.Batch {
		x := m2Batch(a.Mutations[0].SourceRefID, pos, rev)
		y := m2Batch(b.Mutations[0].SourceRefID, pos, rev)
		x.Mutations = append(x.Mutations, y.Mutations[0])
		for i := range x.Mutations {
			x.Mutations[i].Event.Severity = severity
		}
		return x
	}
	before, e := readOne(ctx, a.Mutations[0].SourceRefID)
	require.NoError(t, e)
	m2Apply(t, s, makeCorrection(2, 1, domain.SeveritySiaga))
	after, e := readOne(ctx, b.Mutations[0].SourceRefID)
	require.NoError(t, e)
	require.NotEqual(t, before, after, "unprotected per-unit counterexample must reproduce")
	m2Apply(t, s, makeCorrection(3, 2, domain.SeverityWaspada))
	attempts := 0
	var pair []string
	start := time.Now()
	e = s.validatedRead(ctx, func(ctx context.Context) error {
		attempts++
		x, err := readOne(ctx, a.Mutations[0].SourceRefID)
		if err != nil {
			return err
		}
		if attempts == 1 {
			m2Apply(t, s, makeCorrection(4, 3, domain.SeveritySiaga))
		}
		y, err := readOne(ctx, b.Mutations[0].SourceRefID)
		pair = []string{x, y}
		return err
	})
	require.NoError(t, e)
	require.GreaterOrEqual(t, attempts, 2)
	require.Equal(t, []string{"SIAGA", "SIAGA"}, pair)
	// A live pending attempt blocks reads; it cannot be aged out or published.
	tx, e := s.beginMutation(ctx)
	require.NoError(t, e)
	startBlocked := time.Now()
	_, _, e = s.GetByIDWithMetadata(ctx, ra.Items[0].HazardID)
	m2Error(t, e, "temporarily_unavailable")
	require.Less(t, time.Since(startBlocked), 2500*time.Millisecond)
	require.NoError(t, tx.Rollback(ctx))
	_, _, e = s.GetByIDWithMetadata(ctx, rb.Items[0].HazardID)
	require.NoError(t, e)
	var reads, retries, unavailable int
	var maxLatency time.Duration
	var mu sync.Mutex
	var readers sync.WaitGroup
	stop := make(chan struct{})
	failures := make(chan error, 3)
	for i := 0; i < 3; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				calls := 0
				started := time.Now()
				err := s.validatedRead(ctx, func(ctx context.Context) error {
					calls++
					rows, err := s.Pool.Query(ctx, "SELECT severity FROM hazard_events WHERE source_ref_id=ANY($1::text[]) ORDER BY source_ref_id", []string{a.Mutations[0].SourceRefID, b.Mutations[0].SourceRefID})
					if err != nil {
						return err
					}
					defer rows.Close()
					values := []string{}
					for rows.Next() {
						var v string
						if err = rows.Scan(&v); err != nil {
							return err
						}
						values = append(values, v)
					}
					if err = rows.Err(); err != nil {
						return err
					}
					if len(values) != 2 || values[0] != values[1] {
						return fmt.Errorf("fractured coordinator result: %v", values)
					}
					return nil
				})
				mu.Lock()
				reads++
				if calls > 1 {
					retries += calls - 1
				}
				if time.Since(started) > maxLatency {
					maxLatency = time.Since(started)
				}
				if err != nil {
					var p *canonical.Error
					if errors.As(err, &p) && p.Status == 503 {
						unavailable++
					} else {
						failures <- err
						mu.Unlock()
						return
					}
				}
				mu.Unlock()
			}
		}()
	}
	for i := 0; i < 8; i++ {
		severity := domain.SeverityWaspada
		if i%2 == 1 {
			severity = domain.SeveritySiaga
		}
		m2Apply(t, s, makeCorrection(int64(5+i), int64(4+i), severity))
	}
	close(stop)
	readers.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	evidence := map[string]any{"unprotected_pair": []string{before, after}, "validated_pair": pair, "validated_attempts": attempts, "elapsed_ms": time.Since(start).Milliseconds(), "owners": []string{owner(a.Mutations[0].SourceRefID), owner(b.Mutations[0].SourceRefID)}, "read_deadline_ms": 2000, "production_gate": "not automatically enabled", "concurrent_reads": reads, "concurrent_retries": retries, "unavailable_reads": unavailable, "max_read_latency_ms": maxLatency.Milliseconds()}
	if dir := os.Getenv("M2_EVIDENCE_DIR"); dir != "" {
		raw, _ := json.MarshalIndent(evidence, "", "  ")
		require.NoError(t, os.WriteFile(filepath.Join(dir, "visibility.json"), raw, 0600))
	}
	t.Log(evidence)
}
