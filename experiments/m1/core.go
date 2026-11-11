package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

//go:embed schema.sql
var candidateSchema string

type Shard struct {
	ID      int64  `json:"shard_id"`
	Service string `json:"service"`
}
type Seed struct {
	Route, Ref, ID, Service string
	Shard                   int64
}
type State struct {
	Active                                 int   `json:"active"`
	Revision                               int64 `json:"revision_sum"`
	Operations, Outbox, Lifecycle, Batches int
}

func jsonString(s string) (string, error) {
	if !utf8.ValidString(s) {
		return "", errors.New("invalid UTF-8 identity")
	}
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"', '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if r < 32 {
				fmt.Fprintf(&b, `\u%04x`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return b.String(), nil
}
func routeKey(source, ref string) (string, error) {
	a, e := jsonString(source)
	if e != nil {
		return "", e
	}
	b, e := jsonString(ref)
	return "[" + a + "," + b + "]", e
}
func uuid() string {
	b := make([]byte, 16)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	b[6] = b[6]&15 | 64
	b[8] = b[8]&63 | 128
	h := hex.EncodeToString(b)
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

func (l *Lab) runCore(ctx context.Context) {
	l.result(ctx, "distribution", l.distribution)
	// Setup failure blocks dependent experiments, without suppressing their requested status.
	c, err := l.connect(ctx, "coord")
	if err != nil {
		for _, n := range []string{"correction_batches", "degraded_reads", "snapshot_capture"} {
			l.Report.Results = append(l.Report.Results, Result{Name: n, Status: "blocked", Error: "candidate schema unavailable"})
		}
		return
	}
	var exists bool
	err = c.QueryRow(ctx, "SELECT to_regclass('hazards') IS NOT NULL").Scan(&exists)
	c.Close(context.Background())
	if err != nil || !exists {
		for _, n := range []string{"correction_batches", "degraded_reads", "snapshot_capture"} {
			l.Report.Results = append(l.Report.Results, Result{Name: n, Status: "blocked", Error: "candidate schema unavailable"})
		}
		return
	}
	l.result(ctx, "correction_batches", l.batches)
	l.result(ctx, "degraded_reads", l.degraded)
	l.result(ctx, "snapshot_capture", l.snapshot)
}

func (l *Lab) admitted(ctx context.Context) (*pgx.Conn, pgx.Tx, func(), error) {
	c, err := l.connect(ctx, "coord")
	if err != nil {
		return nil, nil, func() {}, err
	}
	if _, err = c.Exec(ctx, "SELECT pg_advisory_lock_shared($1)", gateKey); err != nil {
		c.Close(context.Background())
		return nil, nil, func() {}, err
	}
	finish := func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = c.Exec(cleanup, "SELECT pg_advisory_unlock_shared($1)", gateKey)
		_ = c.Close(cleanup)
	}
	tx, err := c.Begin(ctx)
	if err != nil {
		finish()
		return nil, nil, func() {}, err
	}
	return c, tx, finish, nil
}
func (l *Lab) mutate(ctx context.Context, fn func(pgx.Tx) error) error {
	_, tx, done, err := l.admitted(ctx)
	if err != nil {
		return err
	}
	defer done()
	defer tx.Rollback(context.Background())
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (l *Lab) shards(ctx context.Context) ([]Shard, error) {
	c, err := l.connect(ctx, "coord")
	if err != nil {
		return nil, err
	}
	defer c.Close(context.Background())
	rows, err := c.Query(ctx, `SELECT s.shardid,n.nodename FROM pg_dist_shard s JOIN pg_dist_placement p USING(shardid) JOIN pg_dist_node n USING(groupid) WHERE s.logicalrelid='hazards'::regclass AND n.noderole='primary' ORDER BY s.shardid`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Shard
	for rows.Next() {
		var s Shard
		if err = rows.Scan(&s.ID, &s.Service); err != nil {
			return nil, err
		}
		if !contains(l.DB, s.Service) {
			return nil, fmt.Errorf("unexpected catalog destination %q", s.Service)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func (l *Lab) seeds(ctx context.Context) ([]Seed, error) {
	shards, err := l.shards(ctx)
	if err != nil {
		return nil, err
	}
	byID := map[int64]string{}
	for _, s := range shards {
		byID[s.ID] = s.Service
	}
	c, err := l.connect(ctx, "coord")
	if err != nil {
		return nil, err
	}
	defer c.Close(context.Background())
	chosen := map[string]Seed{}
	for i := 0; i < 128 && len(chosen) < 2; i++ {
		ref := fmt.Sprintf("probe-%03d", i)
		route, _ := routeKey("PVMBG", ref)
		var shard int64
		err = c.QueryRow(ctx, "SELECT get_shard_id_for_distribution_column('hazards',$1::text)", route).Scan(&shard)
		if err != nil {
			return nil, err
		}
		service := byID[shard]
		if service == "" {
			return nil, errors.New("unmapped logical shard")
		}
		if _, ok := chosen[service]; !ok {
			chosen[service] = Seed{Route: route, Ref: ref, Service: service, Shard: shard}
		}
	}
	if len(chosen) != 2 {
		return nil, errors.New("fixtures did not reach both workers")
	}
	var out []Seed
	for _, s := range chosen {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Service < out[j].Service })
	for i := range out {
		out[i].ID = fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1)
	}
	return out, nil
}
func (l *Lab) reset(ctx context.Context) ([]Seed, error) {
	seed, err := l.seeds(ctx)
	if err != nil {
		return nil, err
	}
	setup, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	// Idempotent fixture setup between serialized cases, not an application
	// mutation retry policy. Allow routing to settle after a worker restart.
	err = until(setup, func() error {
		return l.mutate(setup, func(tx pgx.Tx) error {
			if _, e := tx.Exec(setup, "TRUNCATE operations,outbox,lifecycle,batches,snapshots,hazards,captures"); e != nil {
				return e
			}
			for _, s := range seed {
				if _, e := tx.Exec(setup, "INSERT INTO hazards(route_key,hazard_id,source,source_ref_id) VALUES($1,$2,'PVMBG',$3)", s.Route, s.ID, s.Ref); e != nil {
					return e
				}
			}
			return nil
		})
	})
	return seed, err
}

func (l *Lab) distribution(ctx context.Context, d map[string]any) error {
	c, err := l.connect(ctx, "coord")
	if err != nil {
		return err
	}
	defer c.Close(context.Background())
	if _, err = c.Exec(ctx, "CREATE TABLE legacy(hazard_id uuid PRIMARY KEY,source text NOT NULL,source_ref_id text NOT NULL,UNIQUE(source,source_ref_id))"); err != nil {
		return err
	}
	_, legacyErr := c.Exec(ctx, "SELECT create_distributed_table('legacy','source_ref_id')")
	if legacyErr == nil {
		return errors.New("expected legacy UUID-only primary key to be rejected")
	}
	d["legacy_constraint_rejection"] = legacyErr.Error()
	if _, err = c.Exec(ctx, candidateSchema); err != nil {
		return err
	}
	shards, err := l.shards(ctx)
	if err != nil {
		return err
	}
	d["placements"] = shards
	if len(shards) != 4 {
		return fmt.Errorf("expected four logical shards, got %d", len(shards))
	}
	for _, ref := range []string{"a", "a|b", "quote\"slash\\tab\t", "é火", "<&>", "line\u2028separator"} {
		route, _ := routeKey("PVMBG", ref)
		var sqlRoute string
		if err = c.QueryRow(ctx, "SELECT lab_route('PVMBG',$1)", ref).Scan(&sqlRoute); err != nil {
			return err
		}
		if route != sqlRoute {
			return fmt.Errorf("routing encoding mismatch for %q", ref)
		}
		if err = l.mutate(ctx, func(tx pgx.Tx) error {
			_, e := tx.Exec(ctx, "INSERT INTO hazards(route_key,hazard_id,source,source_ref_id) VALUES($1,$2,'PVMBG',$3)", route, uuid(), ref)
			return e
		}); err != nil {
			return err
		}
	}
	d["encoding_cases"] = 6
	_, bad := c.Exec(ctx, "INSERT INTO hazards(route_key,hazard_id,source,source_ref_id) VALUES('wrong',$1,'PVMBG','mismatch')", uuid())
	if bad == nil {
		return errors.New("mismatched routing key accepted")
	}
	d["route_binding_rejection"] = bad.Error()
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	route, _ := routeKey("PVMBG", "concurrent")
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- l.mutate(ctx, func(tx pgx.Tx) error {
				_, e := tx.Exec(ctx, "INSERT INTO hazards(route_key,hazard_id,source,source_ref_id) VALUES($1,$2,'PVMBG','concurrent') ON CONFLICT(route_key,source,source_ref_id) DO NOTHING", route, uuid())
				return e
			})
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			return e
		}
	}
	var n int
	var id string
	if err = c.QueryRow(ctx, "SELECT count(*),min(hazard_id::text) FROM hazards WHERE route_key=$1 AND source='PVMBG' AND source_ref_id='concurrent'", route).Scan(&n, &id); err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("concurrent duplicate rows: %d", n)
	}
	d["concurrent_submissions"] = 16
	d["deduplicated_entities"] = n
	if err = c.QueryRow(ctx, "SELECT count(*) FROM hazards WHERE hazard_id=$1", id).Scan(&n); err != nil || n != 1 {
		return fmt.Errorf("UUID-only scatter lookup failed: %v, count %d", err, n)
	}
	d["uuid_lookup"] = "distributed scatter; no global UUID-only index assumed"
	if err = l.mutate(ctx, func(tx pgx.Tx) error {
		_, e := tx.Exec(ctx, "INSERT INTO operations VALUES($1,'valid-op',$2);", route, id)
		if e != nil {
			return e
		}
		_, e = tx.Exec(ctx, "INSERT INTO outbox VALUES($1,'valid-event',$2,1)", route, id)
		if e != nil {
			return e
		}
		_, e = tx.Exec(ctx, "INSERT INTO lifecycle VALUES($1,'valid-transition',$2,1)", route, id)
		return e
	}); err != nil {
		return err
	}
	_, bad = c.Exec(ctx, "INSERT INTO outbox VALUES($1,'orphan',$2,1)", route, uuid())
	if bad == nil {
		return errors.New("orphan publication intent accepted")
	}
	d["foreign_key_rejection"] = bad.Error()
	_, err = c.Exec(ctx, `CREATE TABLE upgrade_probe(hazard_id uuid PRIMARY KEY,source text NOT NULL,source_ref_id text NOT NULL,ingested_at timestamptz NOT NULL,UNIQUE(source,source_ref_id)); INSERT INTO upgrade_probe VALUES('00000000-0000-4000-8000-000000000099','BMKG','upgrade','2026-11-11T00:00:00Z'); ALTER TABLE upgrade_probe ADD COLUMN route_key text; UPDATE upgrade_probe SET route_key=lab_route(source,source_ref_id); ALTER TABLE upgrade_probe ALTER COLUMN route_key SET NOT NULL; ALTER TABLE upgrade_probe DROP CONSTRAINT upgrade_probe_pkey; ALTER TABLE upgrade_probe DROP CONSTRAINT upgrade_probe_source_source_ref_id_key; ALTER TABLE upgrade_probe ADD PRIMARY KEY(route_key,hazard_id); ALTER TABLE upgrade_probe ADD UNIQUE(route_key,source,source_ref_id); ALTER TABLE upgrade_probe ADD CHECK(route_key=lab_route(source,source_ref_id)); SELECT create_distributed_table('upgrade_probe','route_key',colocate_with=>'hazards')`)
	if err != nil {
		return err
	}
	if err = c.QueryRow(ctx, "SELECT count(*) FROM upgrade_probe WHERE hazard_id='00000000-0000-4000-8000-000000000099' AND ingested_at='2026-11-11T00:00:00Z'").Scan(&n); err != nil || n != 1 {
		return errors.New("representative migration lost identity/time")
	}
	d["migration"] = "representative key migration preserves UUID/ingestion time; not the complete application upgrade"
	// ONLY still passes through Citus' distributed planner. Inspect the physical
	// coordinator heap instead of mislabeling a logical distributed count.
	var heapBytes int64
	if err = c.QueryRow(ctx, "SELECT pg_relation_size('hazards'::regclass)").Scan(&heapBytes); err != nil {
		return err
	}
	d["coordinator_heap_bytes"] = heapBytes
	if heapBytes != 0 {
		return errors.New("unexpected physical coordinator heap allocation; inspect local retention")
	}
	return nil
}

func (l *Lab) state(ctx context.Context) (State, error) {
	c, err := l.connect(ctx, "coord")
	if err != nil {
		return State{}, err
	}
	defer c.Close(context.Background())
	var s State
	err = c.QueryRow(ctx, `SELECT (SELECT count(*) FROM hazards WHERE active),(SELECT coalesce(sum(revision),0) FROM hazards),(SELECT count(*) FROM operations),(SELECT count(*) FROM outbox),(SELECT count(*) FROM lifecycle),(SELECT count(*) FROM batches)`).Scan(&s.Active, &s.Revision, &s.Operations, &s.Outbox, &s.Lifecycle, &s.Batches)
	return s, err
}
func batchFingerprint(seed []Seed) string {
	h := sha256.Sum256([]byte(seed[0].Route + "\x00" + seed[1].Route + "\x00merge-v1"))
	return hex.EncodeToString(h[:])
}
func prepareBatch(ctx context.Context, tx pgx.Tx, id string, seed []Seed) (bool, error) {
	var fingerprint string
	err := tx.QueryRow(ctx, "SELECT fingerprint FROM batches WHERE batch_id=$1", id).Scan(&fingerprint)
	if err == nil {
		if fingerprint != batchFingerprint(seed) {
			return false, errors.New("identity_conflict")
		}
		return true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO batches VALUES($1,$2,'applied')", id, batchFingerprint(seed)); err != nil {
		return false, err
	}
	for i, s := range seed {
		tag, e := tx.Exec(ctx, "UPDATE hazards SET active=$2,revision=2,severity='SIAGA' WHERE route_key=$1 AND revision=1", s.Route, i == 0)
		if e != nil {
			return false, e
		}
		if tag.RowsAffected() != 1 {
			return false, errors.New("revision_conflict")
		}
		event := fmt.Sprintf("%s:%d", id, i)
		if _, e = tx.Exec(ctx, "INSERT INTO operations VALUES($1,$2,$3)", s.Route, event, s.ID); e != nil {
			return false, e
		}
		if _, e = tx.Exec(ctx, "INSERT INTO outbox VALUES($1,$2,$3,2)", s.Route, event, s.ID); e != nil {
			return false, e
		}
		if _, e = tx.Exec(ctx, "INSERT INTO lifecycle VALUES($1,$2,$3,2)", s.Route, event, s.ID); e != nil {
			return false, e
		}
	}
	return false, nil
}
func (l *Lab) applyBatch(ctx context.Context, id string, seed []Seed) (bool, error) {
	duplicate := false
	err := l.mutate(ctx, func(tx pgx.Tx) error { var e error; duplicate, e = prepareBatch(ctx, tx, id, seed); return e })
	return duplicate, err
}
func settled(s State) bool {
	return s == (State{Active: 2, Revision: 2}) || s == (State{Active: 1, Revision: 4, Operations: 2, Outbox: 2, Lifecycle: 2, Batches: 1})
}

func (l *Lab) batches(ctx context.Context, d map[string]any) error {
	seed, err := l.reset(ctx)
	if err != nil {
		return err
	}
	d["actual_cross_worker_targets"] = seed
	rollback := errors.New("intentional rollback")
	err = l.mutate(ctx, func(tx pgx.Tx) error {
		if _, e := prepareBatch(ctx, tx, "rollback", seed); e != nil {
			return e
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		return fmt.Errorf("rollback injection: %v", err)
	}
	s, err := l.state(ctx)
	if err != nil {
		return err
	}
	if s != (State{Active: 2, Revision: 2}) {
		return fmt.Errorf("rollback leaked changes: %+v", s)
	}
	d["rollback"] = s
	if err = l.interruptCommit(ctx, seed, d); err != nil {
		return err
	}
	seed, err = l.reset(ctx)
	if err != nil {
		return err
	}
	proxy, err := newCommitProxy(ctx, l.DSN["coord"])
	if err != nil {
		return err
	}
	defer proxy.Close()
	c, err := pgx.Connect(ctx, proxy.DSN)
	if err != nil {
		return err
	}
	if _, err = c.Exec(ctx, "SELECT pg_advisory_lock_shared($1)", gateKey); err != nil {
		return err
	}
	tx, err := c.Begin(ctx)
	if err != nil {
		return err
	}
	if _, err = prepareBatch(ctx, tx, "lost-response", seed); err != nil {
		return err
	}
	err = tx.Commit(ctx)
	c.Close(context.Background())
	if err == nil {
		return errors.New("proxy did not hide the actual COMMIT response")
	}
	select {
	case <-proxy.Dropped:
	case <-ctx.Done():
		return ctx.Err()
	}
	d["lost_commit_reply"] = err.Error()
	duplicate, err := l.applyBatch(ctx, "lost-response", seed)
	if err != nil {
		return err
	}
	if !duplicate {
		return errors.New("unknown-outcome retry was not duplicate")
	}
	s, err = l.state(ctx)
	if err != nil {
		return err
	}
	want := State{Active: 1, Revision: 4, Operations: 2, Outbox: 2, Lifecycle: 2, Batches: 1}
	if s != want {
		return fmt.Errorf("retry duplicated effects: %+v", s)
	}
	d["retry_state"] = s
	// Real per-unit reads deliberately straddle the commit. No assertion about a
	// single coordinator SELECT is inferred from this two-query counterexample.
	seed, err = l.reset(ctx)
	if err != nil {
		return err
	}
	b, err := l.connect(ctx, seed[1].Service)
	if err != nil {
		return err
	}
	defer b.Close(context.Background())
	var oldLoser bool
	if err = b.QueryRow(ctx, "SELECT active FROM "+shardTable("hazards", seed[1].Shard)+" WHERE hazard_id=$1", seed[1].ID).Scan(&oldLoser); err != nil {
		return err
	}
	committed := make(chan error, 1)
	go func() { _, e := l.applyBatch(ctx, "visibility", seed); committed <- e }()
	if err = <-committed; err != nil {
		return err
	}
	a, err := l.connect(ctx, seed[0].Service)
	if err != nil {
		return err
	}
	defer a.Close(context.Background())
	var newRevision int64
	if err = a.QueryRow(ctx, "SELECT revision FROM "+shardTable("hazards", seed[0].Shard)+" WHERE hazard_id=$1", seed[0].ID).Scan(&newRevision); err != nil {
		return err
	}
	d["per_unit_read_counterexample"] = map[string]any{"loser_read_before_commit_active": oldLoser, "survivor_read_after_commit_revision": newRevision, "single_coordinator_select_tested": false, "transaction_recovery_passed": true}
	if oldLoser && newRevision == 2 {
		return errors.New("unversioned live per-unit merge exposes a mixed correction batch; blocks that read design, not proven 2PC recovery")
	}
	return errors.New("expected visibility counterexample was not established")
}

func shardTable(table string, id int64) string {
	return pgx.Identifier{"public", fmt.Sprintf("%s_%d", table, id)}.Sanitize()
}
func (l *Lab) prepared(ctx context.Context, service string) (int, error) {
	c, e := l.connect(ctx, service)
	if e != nil {
		return 0, e
	}
	defer c.Close(context.Background())
	var n int
	e = c.QueryRow(ctx, "SELECT count(*) FROM pg_prepared_xacts").Scan(&n)
	return n, e
}
func (l *Lab) interruptCommit(ctx context.Context, seed []Seed, d map[string]any) error {
	c, tx, done, err := l.admitted(ctx)
	if err != nil {
		return err
	}
	defer done()
	defer tx.Rollback(context.Background())
	if _, err = prepareBatch(ctx, tx, "interrupted", seed); err != nil {
		return err
	}
	if err = l.fault(ctx, "pause", seed[1].Service); err != nil {
		return err
	}
	defer func() {
		restore, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = l.fault(restore, "unpause", seed[1].Service)
	}()
	commitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	doneCommit := make(chan error, 1)
	go func() { doneCommit <- tx.Commit(commitCtx) }()
	observe, stop := context.WithTimeout(ctx, 8*time.Second)
	err = until(observe, func() error {
		n, e := l.prepared(observe, seed[0].Service)
		if e != nil {
			return e
		}
		if n == 0 {
			return errors.New("waiting for first participant PREPARE")
		}
		return nil
	})
	stop()
	if err != nil {
		return fmt.Errorf("blocked: PREPARE fault point not observed: %w", err)
	}
	admin, err := l.connect(ctx, "coord")
	if err != nil {
		return err
	}
	_, err = admin.Exec(ctx, "SELECT pg_catalog.pg_terminate_backend($1::integer)", int32(c.PgConn().PID()))
	admin.Close(context.Background())
	if err != nil {
		return err
	}
	if err = l.fault(ctx, "unpause", seed[1].Service); err != nil {
		return err
	}
	select {
	case e := <-doneCommit:
		d["interrupted_commit_client_error"] = errString(e)
	case <-commitCtx.Done():
		return errors.New("blocked: interrupted COMMIT did not return")
	}
	settle, closeSettle := context.WithTimeout(ctx, 40*time.Second)
	defer closeSettle()
	err = until(settle, func() error {
		for _, service := range []string{seed[0].Service, seed[1].Service} {
			n, e := l.prepared(settle, service)
			if e != nil {
				return e
			}
			if n != 0 {
				return errors.New("prepared decisions still unresolved")
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("blocked: unresolved distributed transaction: %w", err)
	}
	s, err := l.state(ctx)
	if err != nil {
		return err
	}
	d["recovered_interrupted_state"] = s
	if !settled(s) {
		return fmt.Errorf("distributed recovery exposed partial durable batch: %+v", s)
	}
	return nil
}
