package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type View struct {
	Service  string
	Conn     *pgx.Conn
	Tx       pgx.Tx
	Snapshot string
	PID      uint32
}
type Capture struct {
	ID     string
	Epoch  int64
	Views  []View
	Shards []Shard
}
type FrozenRow struct {
	Route, ID string
	Active    bool
	Revision  int64
	Severity  string
}

func (c *Capture) close() {
	for _, v := range c.Views {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = v.Tx.Rollback(ctx)
		_ = v.Conn.Close(ctx)
		cancel()
	}
}
func (l *Lab) pin(ctx context.Context, d map[string]any) (*Capture, error) {
	gate, err := l.connect(ctx, "coord")
	if err != nil {
		return nil, err
	}
	defer gate.Close(context.Background())
	budget, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	start := time.Now()
	if _, err = gate.Exec(budget, "SELECT pg_advisory_lock($1)", gateKey); err != nil {
		return nil, fmt.Errorf("capture admission deadline: %w", err)
	}
	defer func() {
		release, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		_, _ = gate.Exec(release, "SELECT pg_advisory_unlock($1)", gateKey)
	}()
	capture := &Capture{ID: uuid(), Epoch: 1}
	success := false
	defer func() {
		if !success {
			capture.close()
		}
	}()
	// A released client lock is not proof that orphaned prepared decisions resolved.
	if err = until(budget, func() error {
		for _, s := range []string{"coord", "worker1", "worker2"} {
			n, e := l.prepared(budget, s)
			if e != nil {
				return e
			}
			if n != 0 {
				return errors.New("unresolved prepared transaction")
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if _, err = gate.Exec(budget, "INSERT INTO captures VALUES($1,1,'building')", capture.ID); err != nil {
		return nil, err
	}
	capture.Shards, err = l.shards(budget)
	if err != nil {
		return nil, err
	}
	// The coordinator view pins routing/capture metadata; worker views pin actual rows.
	for _, service := range []string{"coord", "worker1", "worker2"} {
		c, e := l.connect(budget, service)
		if e != nil {
			return nil, e
		}
		tx, e := c.BeginTx(budget, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		if e != nil {
			c.Close(context.Background())
			return nil, e
		}
		v := View{Service: service, Conn: c, Tx: tx, PID: c.PgConn().PID()}
		capture.Views = append(capture.Views, v)
		var snapshot string
		var standby bool
		if e = tx.QueryRow(budget, "SELECT pg_export_snapshot(),pg_is_in_recovery()").Scan(&snapshot, &standby); e != nil {
			return nil, e
		}
		if standby {
			return nil, errors.New("required primary changed while pinning")
		}
		capture.Views[len(capture.Views)-1].Snapshot = snapshot
	}
	d["gate_acquire_drain_pin_ms"] = time.Since(start).Milliseconds()
	var diagnostic []map[string]any
	for _, v := range capture.Views {
		diagnostic = append(diagnostic, map[string]any{"service": v.Service, "snapshot": v.Snapshot, "backend_pid": v.PID})
	}
	d["pinned_views"] = diagnostic
	success = true
	return capture, nil // Deferred unlock runs before any copying.
}

func (l *Lab) viewRows(ctx context.Context, capture *Capture, consume func(Shard, []FrozenRow) error) error {
	for _, s := range capture.Shards {
		var view *View
		for i := range capture.Views {
			if capture.Views[i].Service == s.Service {
				view = &capture.Views[i]
				break
			}
		}
		if view == nil {
			return errors.New("capture view missing")
		}
		afterRoute, afterID := "", "00000000-0000-0000-0000-000000000000"
		for {
			rows, err := view.Tx.Query(ctx, "SELECT route_key,hazard_id::text,active,revision,severity FROM "+shardTable("hazards", s.ID)+" WHERE (route_key,hazard_id)>($1,$2::uuid) ORDER BY route_key,hazard_id LIMIT 100", afterRoute, afterID)
			if err != nil {
				return err
			}
			batch := []FrozenRow{}
			for rows.Next() {
				var r FrozenRow
				if err = rows.Scan(&r.Route, &r.ID, &r.Active, &r.Revision, &r.Severity); err != nil {
					rows.Close()
					return err
				}
				batch = append(batch, r)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			if len(batch) == 0 {
				break
			}
			if err = consume(s, batch); err != nil {
				return err
			}
			last := batch[len(batch)-1]
			afterRoute, afterID = last.Route, last.ID
		}
	}
	return nil
}
func rowHash(h hash.Hash, s Shard, r FrozenRow) {
	fmt.Fprintf(h, "%d|%s|%s|%t|%d|%s\n", s.ID, r.Route, r.ID, r.Active, r.Revision, r.Severity)
}
func (l *Lab) captureHash(ctx context.Context, capture *Capture) (string, int, error) {
	h := sha256.New()
	count := 0
	err := l.viewRows(ctx, capture, func(s Shard, rows []FrozenRow) error {
		for _, r := range rows {
			rowHash(h, s, r)
			count++
		}
		return nil
	})
	return hex.EncodeToString(h.Sum(nil)), count, err
}
func (l *Lab) publish(ctx context.Context, capture *Capture) error {
	c, err := l.connect(ctx, "coord")
	if err != nil {
		return err
	}
	defer c.Close(context.Background())
	tag, err := c.Exec(ctx, "UPDATE captures SET status='ready' WHERE capture_id=$1 AND epoch=$2 AND status='building'", capture.ID, capture.Epoch)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("stale_capture_epoch")
	}
	return nil
}
func (l *Lab) abortCapture(ctx context.Context, capture *Capture) error {
	capture.close()
	c, err := l.connect(ctx, "coord")
	if err != nil {
		return err
	}
	defer c.Close(context.Background())
	_, err = c.Exec(ctx, "UPDATE captures SET status='aborted' WHERE capture_id=$1 AND epoch=$2 AND status='building'", capture.ID, capture.Epoch)
	return err
}

func (l *Lab) snapshot(ctx context.Context, d map[string]any) error {
	seed, err := l.reset(ctx)
	if err != nil {
		return err
	}
	// Hold an admitted mutation, observe exclusive lock waiting, then allow it to commit.
	_, tx, done, err := l.admitted(ctx)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "UPDATE hazards SET severity='NORMAL' WHERE route_key=$1", seed[0].Route); err != nil {
		done()
		return err
	}
	type pinned struct {
		c       *Capture
		e       error
		details map[string]any
	}
	ready := make(chan pinned, 1)
	go func() { details := map[string]any{}; c, e := l.pin(ctx, details); ready <- pinned{c, e, details} }()
	observe, stop := context.WithTimeout(ctx, time.Second)
	err = until(observe, func() error {
		c, e := l.connect(observe, "coord")
		if e != nil {
			return e
		}
		defer c.Close(context.Background())
		var n int
		e = c.QueryRow(observe, "SELECT count(*) FROM pg_stat_activity WHERE wait_event='advisory' AND query LIKE '%pg_advisory_lock(%'").Scan(&n)
		if e != nil {
			return e
		}
		if n == 0 {
			return errors.New("capture not yet waiting")
		}
		return nil
	})
	stop()
	if err != nil {
		tx.Rollback(context.Background())
		done()
		p := <-ready
		if p.c != nil {
			p.c.close()
		}
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		done()
		p := <-ready
		if p.c != nil {
			p.c.close()
		}
		return err
	}
	done()
	p := <-ready
	if p.e != nil {
		return p.e
	}
	for k, v := range p.details {
		d[k] = v
	}
	capture := p.c
	defer capture.close()
	before, n, err := l.captureHash(ctx, capture)
	if err != nil {
		return err
	}
	d["cut_hash"] = before
	d["cut_count"] = n
	// Admission has resumed, but no snapshot row has been copied yet.
	err = l.mutate(ctx, func(tx pgx.Tx) error {
		if _, e := tx.Exec(ctx, "UPDATE hazards SET revision=revision+1,severity='AWAS' WHERE route_key=$1", seed[0].Route); e != nil {
			return e
		}
		if _, e := tx.Exec(ctx, "UPDATE hazards SET active=false,revision=revision+1 WHERE route_key=$1", seed[1].Route); e != nil {
			return e
		}
		route, _ := routeKey("BMKG", "created-during-copy")
		_, e := tx.Exec(ctx, "INSERT INTO hazards(route_key,hazard_id,source,source_ref_id) VALUES($1,$2,'BMKG','created-during-copy')", route, uuid())
		return e
	})
	if err != nil {
		return err
	}
	d["live_commit_before_first_copy"] = true
	write, err := l.connect(ctx, "coord")
	if err != nil {
		return err
	}
	defer write.Close(context.Background())
	copied := 0
	err = l.viewRows(ctx, capture, func(_ Shard, rows []FrozenRow) error {
		copyTx, e := write.Begin(ctx)
		if e != nil {
			return e
		}
		defer copyTx.Rollback(context.Background())
		for _, r := range rows {
			if _, e = copyTx.Exec(ctx, "INSERT INTO snapshots VALUES($1,$2,$3,$4,$5,$6)", r.Route, capture.ID, r.ID, r.Active, r.Revision, r.Severity); e != nil {
				return e
			}
			copied++
		}
		return copyTx.Commit(ctx)
	})
	if err != nil {
		return err
	}
	if copied != n {
		return errors.New("capture copy count mismatch")
	}
	// Compare actual frozen table rows against retained views, not a cached full dataset.
	snapshotShards := map[int64]int64{}
	mapping, e := write.Query(ctx, `SELECT h.shardid,s.shardid FROM pg_dist_shard h JOIN pg_dist_shard s ON h.shardminvalue=s.shardminvalue AND h.shardmaxvalue=s.shardmaxvalue WHERE h.logicalrelid='hazards'::regclass AND s.logicalrelid='snapshots'::regclass`)
	if e != nil {
		return e
	}
	for mapping.Next() {
		var source, target int64
		if e = mapping.Scan(&source, &target); e != nil {
			mapping.Close()
			return e
		}
		snapshotShards[source] = target
	}
	e = mapping.Err()
	mapping.Close()
	if e != nil {
		return e
	}
	h := sha256.New()
	for _, s := range capture.Shards {
		target, ok := snapshotShards[s.ID]
		if !ok {
			return errors.New("snapshot shard mapping missing")
		}
		worker, e := l.connect(ctx, s.Service)
		if e != nil {
			return e
		}
		rows, e := worker.Query(ctx, "SELECT route_key,hazard_id::text,active,revision,severity FROM "+shardTable("snapshots", target)+" WHERE snapshot_id=$1 ORDER BY route_key,hazard_id", capture.ID)
		if e != nil {
			worker.Close(context.Background())
			return e
		}
		for rows.Next() {
			var r FrozenRow
			if e = rows.Scan(&r.Route, &r.ID, &r.Active, &r.Revision, &r.Severity); e != nil {
				rows.Close()
				worker.Close(context.Background())
				return e
			}
			rowHash(h, s, r)
		}
		e = rows.Err()
		rows.Close()
		worker.Close(context.Background())
		if e != nil {
			return e
		}
	}
	frozen := hex.EncodeToString(h.Sum(nil))
	d["frozen_hash"] = frozen
	if frozen != before {
		return errors.New("materialized rows differ from the pinned cut")
	}
	if err = l.publish(ctx, capture); err != nil {
		return err
	}
	var liveCount int
	if err = write.QueryRow(ctx, "SELECT count(*) FROM hazards").Scan(&liveCount); err != nil {
		return err
	}
	d["live_count_after_mutation"] = liveCount
	if liveCount != n+1 {
		return errors.New("live writes did not progress during capture")
	}
	capture.close()
	// A lost retained worker view aborts; it cannot be reconstructed from current rows.
	lost, err := l.pin(ctx, map[string]any{})
	if err != nil {
		return err
	}
	defer lost.close()
	var victim View
	for _, v := range lost.Views {
		if v.Service == "worker1" {
			victim = v
			break
		}
	}
	admin, err := l.connect(ctx, victim.Service)
	if err != nil {
		return err
	}
	_, err = admin.Exec(ctx, "SELECT pg_catalog.pg_terminate_backend($1::integer)", int32(victim.PID))
	admin.Close(context.Background())
	if err != nil {
		return err
	}
	_, _, viewErr := l.captureHash(ctx, lost)
	if viewErr == nil {
		return errors.New("lost capture view silently replaced")
	}
	if err = l.abortCapture(ctx, lost); err != nil {
		return err
	}
	if err = l.publish(ctx, lost); err == nil {
		return errors.New("aborted capture published")
	}
	d["lost_view"] = "aborted; publication refused; no replacement view"
	// Epoch change models an authorized successor; it is not a production lease service.
	stale, err := l.pin(ctx, map[string]any{})
	if err != nil {
		return err
	}
	defer stale.close()
	if _, err = write.Exec(ctx, "UPDATE captures SET epoch=epoch+1,status='aborted' WHERE capture_id=$1", stale.ID); err != nil {
		return err
	}
	if err = l.publish(ctx, stale); err == nil {
		return errors.New("stale epoch published")
	}
	d["stale_publication"] = "rejected by persisted capture epoch"
	if err = l.ownerTermination(ctx, d); err != nil {
		return err
	}
	d["limits"] = "experiment-only advisory admission and epoch; lease expiry/freeze safety and replica durability remain Role 2 gates"
	return nil
}

func (l *Lab) ownerTermination(ctx context.Context, d map[string]any) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	child := exec.Command(exe, "_hold-gate")
	child.Env = append(os.Environ(), "M1_HOLD_DSN="+l.DSN["coord"])
	stdout, err := child.StdoutPipe()
	if err != nil {
		return err
	}
	if err = child.Start(); err != nil {
		return err
	}
	defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if scanner.Scan() {
			ready <- scanner.Text()
		} else {
			ready <- ""
		}
	}()
	select {
	case msg := <-ready:
		if strings.TrimSpace(msg) != "READY" {
			return errors.New("gate owner did not acquire admission")
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return errors.New("gate owner startup deadline")
	}
	// Admission attempts cannot bypass the holder. This also tests the bounded gate deadline.
	blocked, cancel := context.WithTimeout(ctx, 2*time.Second)
	_, _, done, admissionErr := l.admitted(blocked)
	done()
	cancel()
	if admissionErr == nil {
		return errors.New("writer bypassed exclusive gate")
	}
	if err = child.Process.Kill(); err != nil {
		return err
	}
	_ = child.Wait()
	resume, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	err = l.mutate(resume, func(tx pgx.Tx) error {
		_, e := tx.Exec(resume, "UPDATE hazards SET severity=severity WHERE source='BMKG'")
		return e
	})
	if err != nil {
		return fmt.Errorf("owner death did not release admission: %w", err)
	}
	d["owner_termination"] = "real child process killed; blocked writer resumed within two-second deadline"
	return nil
}
