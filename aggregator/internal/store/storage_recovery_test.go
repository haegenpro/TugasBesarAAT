package store

import (
	"aggregator/internal/audit"
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"aggregator/internal/timeutil"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

type recoveryPlacement struct {
	Shard int64  `json:"shard"`
	Group int    `json:"group"`
	Host  string `json:"host"`
	Rows  int64  `json:"rows"`
	Bytes int64  `json:"physical_bytes"`
}

func recoveryExpectedContent(event *canonical.MutableEvent) any {
	if event == nil {
		return nil
	}
	fields := map[string]any{"hazard_type": event.HazardType, "severity": event.Severity, "area_name": event.AreaName, "latitude": event.Latitude, "longitude": event.Longitude, "occurred_at": timeutil.New(event.OccurredAt), "attributes": event.Attributes}
	raw, err := json.Marshal(fields)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(raw))
}

func TestM7EvidenceWireFormat(t *testing.T) {
	event := &canonical.MutableEvent{HazardType: domain.HazardSeismic, Severity: domain.SeverityNormal, AreaName: "A", OccurredAt: time.Date(2026, 1, 1, 0, 0, 0, 123456000, time.UTC), Attributes: map[string]any{}}
	require.Equal(t, "16db9e20157ad74e140ac54b221a12f575c4d43151669c79548ebdac74b7b482", recoveryExpectedContent(event), "independent sorted JSON vector with canonical millisecond wire timestamp")
	receipt, _ := json.Marshal(canonical.Item{Revision: 1})
	raw, err := json.Marshal(map[string]any{"result": json.RawMessage(receipt)})
	require.NoError(t, err)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.IsType(t, map[string]any{}, decoded["result"], "receipt must be a JSON object, never byte-slice base64")
}

func recoveryPlacements(t *testing.T, s *Store) []recoveryPlacement {
	t.Helper()
	ctx := context.Background()
	rows, e := s.Pool.Query(ctx, `SELECT sh.shardid,n.groupid,n.nodename FROM pg_dist_shard sh JOIN pg_dist_placement p USING(shardid) JOIN pg_dist_node n USING(groupid) WHERE sh.logicalrelid='hazard_events'::regclass AND n.noderole='primary' ORDER BY sh.shardid`)
	require.NoError(t, e)
	var placements []recoveryPlacement
	for rows.Next() {
		var p recoveryPlacement
		require.NoError(t, rows.Scan(&p.Shard, &p.Group, &p.Host))
		placements = append(placements, p)
	}
	require.NoError(t, rows.Err())
	rows.Close()
	for i := range placements {
		p := &placements[i]
		pool := s.Reads.Groups[p.Group]
		require.NotNil(t, pool)
		table := fmt.Sprintf("hazard_events_%d", p.Shard)
		require.NoError(t, pool.QueryRow(ctx, "SELECT count(*),pg_total_relation_size($1::regclass) FROM "+pgx.Identifier{table}.Sanitize(), table).Scan(&p.Rows, &p.Bytes))
	}
	return placements
}

func recoverySafeMove(placements []recoveryPlacement, shard int64, destination string) bool {
	all := map[int64]bool{}
	held := map[int64]bool{}
	exists := false
	for _, p := range placements {
		if p.Rows <= 0 {
			return false
		}
		all[p.Shard] = true
		if p.Host == destination {
			held[p.Shard] = true
		}
		if p.Shard == shard && p.Host != destination {
			exists = true
		}
	}
	held[shard] = true
	return exists && len(held) < len(all)
}

func recoveryWorkerActivity(t *testing.T, s *Store) map[int]json.RawMessage {
	t.Helper()
	out := map[int]json.RawMessage{}
	for group, pool := range s.Reads.Groups {
		if group == 0 {
			continue
		}
		var raw json.RawMessage
		require.NoError(t, pool.QueryRow(context.Background(), `SELECT jsonb_build_object('sampled_at',clock_timestamp(),'database_bytes',pg_database_size(current_database()),'inserts',coalesce(sum(n_tup_ins),0),'updates',coalesce(sum(n_tup_upd),0),'deletes',coalesce(sum(n_tup_del),0),'interpretation','asynchronous PostgreSQL statistics, not a capacity benchmark') FROM pg_stat_user_tables`).Scan(&raw))
		out[group] = raw
	}
	return out
}

func TestM7RejectsUnsafeTransientDestination(t *testing.T) {
	p := []recoveryPlacement{{Shard: 1, Host: "a", Rows: 1}, {Shard: 2, Host: "a", Rows: 1}, {Shard: 3, Host: "b", Rows: 1}, {Shard: 4, Host: "b", Rows: 1}}
	require.True(t, recoverySafeMove(p, 1, "b"))
	p[1].Host = "b"
	require.False(t, recoverySafeMove(p, 1, "b"))
	require.False(t, recoverySafeMove(p, 99, "b"))
}

// Real application canonical paths, test-only single-copy authority/durability.
// This is intentionally independent of the HA group experiment.
func TestM7OnlineMovementAndReconciliation(t *testing.T) {
	dsn, c := m2Database(t, true)
	m2Schema(t, c, "citus", false)
	s := m2Store(t, dsn)
	m4ReadSetup(t, s, dsn, true)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	loadStarted := time.Now()
	activityBefore := recoveryWorkerActivity(t, s)
	acknowledgements := []map[string]any{}
	latest := map[string]canonical.Batch{}
	initialID := map[string]string{}
	initialAt := map[string]string{}
	apply := func(b canonical.Batch) (canonical.Result, error) {
		r, e := m2Retry(s, b)
		if e != nil {
			return r, e
		}
		m := b.Mutations[0]
		if len(r.Items) != 1 || r.Items[0].Revision != m.ExpectedRevision+1 {
			return r, errors.New("unexpected independently expected material revision")
		}
		entity, e := s.Reconcile(ctx, m.Source, m.SourceRefID)
		if e != nil {
			return r, e
		}
		if entity.HazardID != r.Items[0].HazardID {
			return r, errors.New("receipt identity differs from reconciliation")
		}
		var at time.Time
		if e = s.Pool.QueryRow(ctx, "SELECT ingested_at FROM canonical_entities WHERE source=$1 AND source_ref_id=$2", m.Source, m.SourceRefID).Scan(&at); e != nil {
			return r, e
		}
		if old, ok := initialID[m.SourceRefID]; ok && (old != entity.HazardID || initialAt[m.SourceRefID] != at.UTC().Format(time.RFC3339Nano)) {
			return r, errors.New("canonical UUID/first-ingestion time changed")
		}
		initialID[m.SourceRefID] = entity.HazardID
		initialAt[m.SourceRefID] = at.UTC().Format(time.RFC3339Nano)
		content := recoveryExpectedContent(m.Event)
		operation, _ := json.Marshal(r.Items[0])
		acknowledgements = append(acknowledgements, map[string]any{"source": m.Source, "source_ref_id": m.SourceRefID, "operation_id": m.OperationID, "batch_id": b.BatchID, "hazard_id": entity.HazardID, "revision": r.Items[0].Revision, "active": m.Operation != "retract", "ingested_at": at.UTC().Format(time.RFC3339Nano), "content_sha256": content, "result": json.RawMessage(operation)})
		latest[m.SourceRefID] = b
		return r, nil
	}
	for i := 0; i < 64; i++ {
		b := m2Batch(fmt.Sprintf("recovery-%03d", i), 1, 0)
		b.BatchID = fmt.Sprintf("recovery-create-%03d", i)
		b.Mutations[0].OperationID = b.BatchID
		m := b.Mutations[0].Event
		m.AreaName = "Synthetic regional concentration"
		m.Latitude = -6
		m.Longitude = 107
		if i == 63 {
			m.Latitude = 1
			m.Longitude = 125
		}
		m.Attributes = map[string]any{"fixture": "synthetic-recovery-v1", "padding": strings.Repeat("x", 128<<10)}
		_, e := apply(b)
		require.NoError(t, e)
	}
	// Keep original operation identities for duplicate retries; retraction remains
	// an inactive identity and has an independent lifecycle publication intent.
	ret := m2Batch("recovery-063", 2, 1)
	ret.Mutations[0].Operation = "retract"
	ret.Mutations[0].Event = nil
	_, e := apply(ret)
	require.NoError(t, e)
	before := recoveryPlacements(t, s)
	loadMS := time.Since(loadStarted).Milliseconds()
	activityLoaded := recoveryWorkerActivity(t, s)
	require.Len(t, before, 4)
	groups := map[string]int{}
	for _, p := range before {
		require.Positive(t, p.Rows)
		groups[p.Host]++
	}
	require.Len(t, groups, 2)
	shard := before[0]
	destination := "worker1"
	if shard.Host == destination {
		destination = "worker2"
	}
	require.True(t, recoverySafeMove(before, shard.Shard, destination), "including transient destination copies")
	var moving, active atomic.Bool
	var written, readOK, partialReads, retries atomic.Int64
	moveDone := make(chan error, 1)
	moveStarted := time.Now()
	moving.Store(true)
	go func() {
		_, err := s.Pool.Exec(ctx, `SELECT citus_move_shard_placement($1,$2,5432,$3,5432,'force_logical') /* recovery_online_move */`, shard.Shard, shard.Host, destination)
		moving.Store(false)
		moveDone <- err
	}()
	// Observe the actual SQL operation; do not start with a timing-only sleep.
	m2Eventually(t, 5*time.Second, func() bool {
		var present bool
		err := c.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE state='active' AND query LIKE 'SELECT citus_move_shard_placement%recovery_online_move%')`).Scan(&present)
		if err == nil && present {
			active.Store(true)
			return true
		}
		return !moving.Load()
	})
	workCtx, stop := context.WithCancel(ctx)
	defer stop()
	writerDone := make(chan error, 1)
	readerDone := make(chan error, 1)
	readCompleted := make(chan struct{}, 1)
	go func() {
		for pos := int64(2); ; pos++ {
			if workCtx.Err() != nil {
				writerDone <- nil
				return
			}
			b := m2Batch("recovery-000", pos, pos-1)
			b.Mutations[0].Event.AreaName = "Synthetic regional burst"
			b.Mutations[0].Event.Attributes = map[string]any{"burst": pos}
			_, err := apply(b)
			if err != nil {
				writerDone <- err
				return
			}
			if moving.Load() && active.Load() {
				written.Add(1)
			}
			// Let a validated read finish between burst corrections. This is a
			// synchronized workload schedule, rather than an unbounded busy writer.
			select {
			case <-readCompleted:
			case <-workCtx.Done():
				writerDone <- nil
				return
			}
		}
	}()
	go func() {
		for workCtx.Err() == nil {
			page, err := s.AuditEvents(workCtx, audit.Query{Limit: 100, Scope: "audit"})
			if err != nil {
				var ce *canonical.Error
				if workCtx.Err() != nil {
					break
				}
				if errors.As(err, &ce) && ce.Status == 503 {
					retries.Add(1)
					continue
				}
				readerDone <- err
				return
			}
			if page.Partial {
				if len(page.Items) != 0 || page.NextCursor == nil || len(page.Unavailable) == 0 {
					readerDone <- fmt.Errorf("unsafe partial page: count=%d cursor=%v unavailable=%v", len(page.Items), page.NextCursor, page.Unavailable)
					return
				}
				partialReads.Add(1)
			} else if len(page.Items) != 63 {
				readerDone <- fmt.Errorf("incorrect complete canonical population: count=%d", len(page.Items))
				return
			}
			if moving.Load() && active.Load() {
				readOK.Add(1)
			}
			select {
			case readCompleted <- struct{}{}:
			default:
			}
		}
		readerDone <- nil
	}()
	moveErr := <-moveDone
	moveMS := time.Since(moveStarted).Milliseconds()
	stop()
	writerErr := <-writerDone
	readerErr := <-readerDone
	require.NoError(t, writerErr)
	require.NoError(t, readerErr)
	if moveErr != nil {
		if strings.Contains(moveErr.Error(), "not supported") || strings.Contains(moveErr.Error(), "does not exist") {
			t.Log("RECOVERY_BLOCKED: pinned Citus online movement unavailable")
		}
		require.NoError(t, moveErr, "online movement is required; never downgrade to blocking writes")
	}
	after := recoveryPlacements(t, s)
	// Inspect physical heaps, including any old source retained after movement,
	// rather than trusting the coordinator's routing map alone.
	physicalUnits := []map[string]any{}
	for group, pool := range s.Reads.Groups {
		if group == 0 {
			continue
		}
		var memberID string
		require.NoError(t, pool.QueryRow(ctx, "SELECT system_identifier::text FROM pg_control_system()").Scan(&memberID))
		physicalShards := []string{}
		// Citus may hide shard heaps from ordinary catalog listings. Resolve
		// every known fixture shard on every worker, including former owners.
		for _, shard := range before {
			var present bool
			require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass($1)::oid IS NOT NULL", fmt.Sprintf("public.hazard_events_%d", shard.Shard)).Scan(&present))
			if present {
				physicalShards = append(physicalShards, fmt.Sprint(shard.Shard))
			}
		}
		require.NotEmpty(t, physicalShards, "physical holdings must be actually observed, never inferred from an empty listing")
		require.Less(t, len(physicalShards), 4, "a worker must not retain every physical shard")
		physicalUnits = append(physicalUnits, map[string]any{"name": fmt.Sprintf("worker%d", group), "node": fmt.Sprintf("local-container-worker%d", group), "group": group, "identity_verified": true, "system_id": memberID, "holdings_complete": true, "complete": true, "shards": physicalShards})
	}
	for _, p := range after {
		if p.Shard == shard.Shard {
			require.Equal(t, destination, p.Host)
		}
	}
	require.Positive(t, written.Load(), "canonical acknowledgement must occur while movement is active")
	require.Positive(t, readOK.Load(), "a complete or explicitly safe partial read must return while movement is active")
	var operations, batches, pending, lifecycle int
	require.NoError(t, c.QueryRow(ctx, `SELECT (SELECT count(*) FROM canonical_operations),(SELECT count(*) FROM canonical_batches),(SELECT count(*) FROM hazard_outbox),(SELECT count(*) FROM canonical_lifecycle_outbox)`).Scan(&operations, &batches, &pending, &lifecycle))
	require.Equal(t, len(acknowledgements), operations)
	require.Equal(t, operations, batches)
	require.Positive(t, pending)
	require.Positive(t, lifecycle)
	for _, b := range latest {
		r, err := m2Retry(s, b)
		require.NoError(t, err)
		require.Equal(t, "duplicate", r.Outcome)
	}
	// Independently expected active references: 000..062, including duplicate
	// source identifiers exactly once per logical canonical record.
	refs := []string{}
	for i := 0; i < 63; i++ {
		refs = append(refs, fmt.Sprintf("recovery-%03d", i))
	}
	sort.Strings(refs)
	wantHash := fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(refs, "\n"))))
	sum, err := s.AuditChecksum(ctx, "BMKG")
	require.NoError(t, err)
	require.False(t, sum.Partial)
	require.Equal(t, int64(63), sum.Count)
	require.Equal(t, wantHash, sum.Checksum)
	entities := []map[string]any{}
	for ref := range latest {
		v, err := s.Reconcile(ctx, domain.SourceBMKG, ref)
		require.NoError(t, err)
		require.Equal(t, initialID[ref], v.HazardID)
		var actualAt time.Time
		require.NoError(t, c.QueryRow(ctx, "SELECT ingested_at FROM canonical_entities WHERE source='BMKG' AND source_ref_id=$1", ref).Scan(&actualAt))
		require.Equal(t, initialAt[ref], actualAt.UTC().Format(time.RFC3339Nano))
		var observedDigest any
		if v.Event != nil {
			// Preserve exact JSON numbers while dropping immutable wire fields.
			raw, e := json.Marshal(v.Event)
			require.NoError(t, e)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &fields))
			for _, key := range []string{"hazard_id", "source", "source_ref_id", "ingested_at"} {
				delete(fields, key)
			}
			raw, e = json.Marshal(fields)
			require.NoError(t, e)
			observedDigest = fmt.Sprintf("%x", sha256.Sum256(raw))
		}
		entities = append(entities, map[string]any{"source": v.Source, "source_ref_id": ref, "hazard_id": v.HazardID, "revision": v.Revision, "active": v.Active, "ingested_at": actualAt.UTC().Format(time.RFC3339Nano), "content_sha256": observedDigest})
	}
	rows, err := c.Query(ctx, "SELECT source,operation_id,batch_id,result FROM canonical_operations ORDER BY operation_id")
	require.NoError(t, err)
	observedOps := []map[string]any{}
	for rows.Next() {
		var source, id, batch string
		var result json.RawMessage
		require.NoError(t, rows.Scan(&source, &id, &batch, &result))
		observedOps = append(observedOps, map[string]any{"source": source, "operation_id": id, "batch_id": batch, "result": result})
	}
	require.NoError(t, rows.Err())
	rows.Close()
	placementDoc := map[string]any{"before": before, "after": after, "transient_guard": "destination union checked before move", "writes_during_move": written.Load(), "reads_during_move": readOK.Load(), "read_validation_retries": retries.Load(), "pending_publications": pending, "pending_lifecycle_publications": lifecycle}
	placementDoc["safe_partial_reads"] = partialReads.Load()
	placementDoc["worker_activity_before_load"] = activityBefore
	placementDoc["worker_activity_after_load"] = activityLoaded
	placementDoc["worker_activity_after_burst"] = recoveryWorkerActivity(t, s)
	document := map[string]any{"version": 1, "dataset": map[string]any{"id": "synthetic-recovery-v1", "sha256": fmt.Sprintf("%x", sha256.Sum256([]byte("recovery-000..063; 63 concentrated at (-6,107), 1 at (1,125); source-pair hash routing"))), "synthetic": true}, "checkpoint": map[string]any{"id": "after-online-move", "stable": true, "checksum": map[string]any{"source": "BMKG", "count": 63, "checksum": wantHash}}, "acknowledgements": acknowledgements, "uncertain_operations": []any{}, "observed": map[string]any{"entities": entities, "operations": observedOps, "checksum": sum}, "placement": placementDoc, "timings": map[string]any{"input_lag_ms": nil, "canonical_lag_ms": nil}, "scope": "single-copy core; test-only authority/durability"}
	document["observed"].(map[string]any)["topology"] = map[string]any{"verified": true, "units": physicalUnits, "scope": "canonical hazard_events heaps in isolated worker containers; no separate-kernel placement claim"}
	document["timings"].(map[string]any)["load_ms"] = loadMS
	document["timings"].(map[string]any)["online_move_ms"] = moveMS
	if dir := os.Getenv("M2_EVIDENCE_DIR"); dir != "" {
		raw, err := json.MarshalIndent(document, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "recovery-evidence.json"), raw, 0600))
	}
	t.Logf("movement: writes=%d reads=%d retries=%d; pending canonical=%d lifecycle=%d", written.Load(), readOK.Load(), retries.Load(), pending, lifecycle)
}
