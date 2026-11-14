package store

import (
	"aggregator/internal/audit"
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"aggregator/internal/rawinput"
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"hash"
	"strings"
	"sync"
	"time"
)

type snapshotService struct {
	ctx     context.Context
	cancel  context.CancelFunc
	views   map[int]*pgxpool.Pool
	reads   map[int]*pgxpool.Pool
	mu      sync.Mutex
	stopped bool
	workers sync.WaitGroup
	fault   func(context.Context, string) error // In-process synchronized test hook only.
}
type snapshotUnit struct {
	ID                            int64
	Group                         int
	Source, Target, Marker, Route string
	Count                         int64
	Sum                           string
	State                         []byte
	Last                          *time.Time
	LastID                        string
	Complete                      bool
	Verified                      bool
	VerifyCount                   int64
	VerifyHash                    []byte
	VerifyTime                    *time.Time
	VerifyID                      string
}
type pinnedView struct {
	conn *pgxpool.Conn
	tx   pgx.Tx
}

func (s *Store) ConfigureSnapshots(parent context.Context) (func(), error) {
	if s.Reads.Inventory == nil || s.Canonical.Durability == nil {
		return nil, canonical.Unavailable()
	}
	if _, ok := s.Canonical.Durability.(interface {
		ConfirmCapture(context.Context, string, string) error
	}); !ok {
		return nil, fmt.Errorf("snapshot publication requires capture durability confirmation")
	}
	for _, q := range []string{`SELECT capture_id,route_key,attributes FROM audit_snapshot_records LIMIT 0`, `SELECT capture_id,source_shard,hash_state FROM audit_snapshot_units LIMIT 0`, `SELECT record_count,sha256 FROM audit_snapshot_markers LIMIT 0`} {
		if _, e := s.Pool.Exec(parent, q); e != nil {
			return nil, e
		}
	}
	ctx, cancel := context.WithCancel(parent)
	svc := &snapshotService{ctx: ctx, cancel: cancel, views: map[int]*pgxpool.Pool{}, reads: map[int]*pgxpool.Pool{}}
	closePools := func() {
		for _, p := range svc.views {
			p.Close()
		}
		for _, p := range svc.reads {
			p.Close()
		}
	}
	groups := map[int]*pgxpool.Pool{0: s.Pool}
	for id, p := range s.Reads.Groups {
		groups[id] = p
	}
	for id, p := range groups {
		for _, view := range []bool{true, false} {
			c := p.Config().Copy()
			c.MaxConns = 2
			c.MinConns = 0
			if view {
				c.MaxConns = 1
			}
			c.ConnConfig.RuntimeParams["statement_timeout"] = "2000"
			c.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "15000"
			pool, e := pgxpool.NewWithConfig(ctx, c)
			if e != nil {
				cancel()
				closePools()
				return nil, e
			}
			if view {
				svc.views[id] = pool
			} else {
				svc.reads[id] = pool
			}
		}
	}
	s.snapshots = svc
	svc.workers.Add(1)
	go func() {
		defer svc.workers.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			s.cleanupSnapshots(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return func() {
		svc.mu.Lock()
		svc.stopped = true
		cancel()
		svc.mu.Unlock()
		svc.workers.Wait()
		closePools()
	}, nil
}
func (s *Store) snapshotHook(ctx context.Context, point string) error {
	if s.snapshots.fault != nil {
		return s.snapshots.fault(ctx, point)
	}
	return nil
}
func (s *Store) snapshotLayout(ctx context.Context) ([]snapshotUnit, string, error) {
	shards, topology, e := s.readCatalog(ctx)
	if e != nil {
		return nil, "", e
	}
	if len(shards) > 256 {
		return nil, "", fmt.Errorf("snapshot unit quota exceeded")
	}
	out := []snapshotUnit{}
	for _, shard := range shards {
		u := snapshotUnit{ID: shard.ID, Group: shard.Group, Source: shard.Table, Target: "audit_snapshot_records", Marker: "audit_snapshot_markers", Route: "snapshot-marker"}
		if shard.ID != 0 {
			e = s.Pool.QueryRow(ctx, `SELECT 'audit_snapshot_records_'||t.shardid,'audit_snapshot_markers_'||m.shardid FROM pg_dist_shard s JOIN pg_dist_shard t ON t.logicalrelid='audit_snapshot_records'::regclass AND t.shardminvalue=s.shardminvalue AND t.shardmaxvalue=s.shardmaxvalue JOIN pg_dist_shard m ON m.logicalrelid='audit_snapshot_markers'::regclass AND m.shardminvalue=s.shardminvalue AND m.shardmaxvalue=s.shardmaxvalue WHERE s.shardid=$1`, shard.ID).Scan(&u.Target, &u.Marker)
			if e != nil {
				return nil, "", e
			}
			e = s.Pool.QueryRow(ctx, `SELECT canonical_route('BMKG','audit-marker-'||i) FROM generate_series(1,100000) i WHERE get_shard_id_for_distribution_column('hazard_events',canonical_route('BMKG','audit-marker-'||i))=$1 LIMIT 1`, shard.ID).Scan(&u.Route)
			if e != nil {
				return nil, "", e
			}
		}
		out = append(out, u)
	}
	return out, topology, nil
}
func (s *Store) snapshotLayoutValid(ctx context.Context, units []snapshotUnit, topology string) error {
	shards, current, e := s.readCatalog(ctx)
	if e != nil || current != topology || len(shards) != len(units) {
		return canonical.Unavailable()
	}
	for i, u := range units {
		if shards[i].ID != u.ID || shards[i].Group != u.Group || shards[i].Table != u.Source {
			return canonical.Unavailable()
		}
	}
	return s.distributedSettled(ctx, topology)
}
func (s *Store) StartSnapshot(ctx context.Context, source *domain.Source) (audit.Session, error) {
	svc := s.snapshots
	if svc == nil {
		return audit.Session{}, canonical.Unavailable()
	}
	if source != nil && !domain.ValidSource(string(*source)) {
		return audit.Session{}, canonical.Fail(400, "bad_request", "invalid snapshot source")
	}
	ctx, stop := context.WithTimeout(ctx, canonical.Deadline)
	defer stop()
	// Resolve placement and marker routes before closing admission.
	units, topology, e := s.snapshotLayout(ctx)
	if e != nil {
		return audit.Session{}, e
	}
	views := map[int]*pinnedView{}
	release := func() {
		for _, v := range views {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
			_ = v.tx.Rollback(cleanup)
			cancel()
			v.conn.Release()
		}
	}
	token, _, e := s.CaptureBoundary(ctx, "audit-"+uuid.NewString(), source, func(call context.Context) (func(), error) {
		required := map[int]bool{0: true}
		for _, u := range units {
			required[u.Group] = true
		}
		for group := range required {
			pool := svc.views[group]
			if pool == nil {
				return release, canonical.Unavailable()
			}
			conn, e := pool.Acquire(call)
			if e != nil {
				return release, e
			}
			tx, e := conn.BeginTx(call, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
			if e != nil {
				conn.Release()
				return release, e
			}
			views[group] = &pinnedView{conn, tx}
			var snapshot string
			var primary bool
			e = tx.QueryRow(call, `SELECT pg_export_snapshot(),NOT pg_is_in_recovery()`).Scan(&snapshot, &primary)
			if e != nil || !primary {
				return release, canonical.Unavailable()
			}
		}
		if e := s.snapshotLayoutValid(call, units, topology); e != nil {
			return release, e
		}
		return release, nil
	})
	if e != nil {
		return audit.Session{}, e
	}
	failed := true
	defer func() {
		if failed {
			release()
			cleanup, cancel := context.WithTimeout(context.Background(), canonical.Deadline)
			defer cancel()
			_ = s.AbortCapture(cleanup, token, "capture descriptor unavailable")
		}
	}()
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return audit.Session{}, e
	}
	defer tx.Rollback(ctx)
	if e = s.ProtectCaptureCopy(ctx, tx, token); e != nil {
		return audit.Session{}, e
	}
	for _, u := range units {
		_, e = tx.Exec(ctx, `INSERT INTO audit_snapshot_units(capture_id,source_shard,group_id,source_table,target_table,marker_table,marker_route) VALUES($1,$2,$3,$4,$5,$6,$7)`, token.ID, u.ID, u.Group, u.Source, u.Target, u.Marker, u.Route)
		if e != nil {
			return audit.Session{}, e
		}
	}
	if e = tx.Commit(ctx); e != nil {
		return audit.Session{}, canonical.Unavailable()
	}
	if e = s.confirmRecord(ctx, `SELECT (SELECT count(*) FROM audit_snapshot_units WHERE capture_id=$1)=$2`, token.ID, len(units)); e != nil {
		return audit.Session{}, e
	}
	status, e := s.snapshotStatus(ctx, token.ID)
	if e != nil {
		return audit.Session{}, e
	}
	svc.mu.Lock()
	if svc.stopped {
		svc.mu.Unlock()
		return audit.Session{}, canonical.Unavailable()
	}
	svc.workers.Add(1)
	svc.mu.Unlock()
	failed = false
	go func() {
		defer svc.workers.Done()
		defer release()
		s.copySnapshot(svc.ctx, token, units, views, topology, source)
	}()
	return status, nil
}
func (s *Store) snapshotStatus(ctx context.Context, id string) (audit.Session, error) {
	v := audit.Session{ID: id}
	e := s.Pool.QueryRow(ctx, `SELECT CASE WHEN state='ready' AND (idle_expires_at<=clock_timestamp() OR absolute_expires_at<=clock_timestamp()) THEN 'expired' WHEN state='building' AND (owner_expires_at<=clock_timestamp() OR copy_deadline<=clock_timestamp()) THEN 'aborted' ELSE state END,source,cut_at,idle_expires_at,absolute_expires_at,reason FROM platform_audit_captures WHERE capture_id=$1`, id).Scan(&v.Status, &v.Source, &v.Cut, &v.Expires, &v.Absolute, &v.Reason)
	if errors.Is(e, pgx.ErrNoRows) {
		return v, canonical.Fail(404, "not_found", "unknown snapshot")
	}
	if e != nil {
		return v, canonical.Unavailable()
	}
	return v, nil
}
func (s *Store) SnapshotStatus(ctx context.Context, id string) (audit.Session, error) {
	v, e := s.snapshotStatus(ctx, id)
	if e != nil {
		return v, e
	}
	if v.Status == "ready" {
		if e = s.TouchSnapshot(ctx, id); e != nil {
			return v, canonical.Fail(400, "cursor_expired", "snapshot expired")
		}
		return s.snapshotStatus(ctx, id)
	}
	return v, nil
}
func (s *Store) copySnapshot(parent context.Context, token CaptureToken, units []snapshotUnit, views map[int]*pinnedView, topology string, source *domain.Source) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Minute)
	defer cancel()
	renewed := make(chan struct{})
	go func() {
		defer close(renewed)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				call, stop := context.WithTimeout(ctx, canonical.Deadline)
				e := s.RenewCapture(call, token)
				stop()
				if e != nil {
					cancel()
					return
				}
			}
		}
	}()
	defer func() { cancel(); <-renewed }()
	failed := true
	reason := "capture interrupted"
	defer func() {
		if failed {
			cleanup, stop := context.WithTimeout(context.Background(), canonical.Deadline)
			defer stop()
			_ = s.AbortCapture(cleanup, token, reason)
		}
	}()
	if e := s.snapshotHook(ctx, "views_pinned"); e != nil {
		reason = e.Error()
		return
	}
	for {
		complete := true
		for i := range units {
			u := &units[i]
			if u.Verified {
				continue
			}
			complete = false
			call, stop := context.WithTimeout(ctx, canonical.Deadline)
			var e error
			for _, v := range views {
				var n int
				if e = v.tx.QueryRow(call, "SELECT 1").Scan(&n); e != nil {
					break
				}
			}
			if e == nil {
				if u.Complete {
					e = s.verifySnapshotPage(call, token, u)
				} else {
					e = s.copySnapshotPage(call, token, u, views[u.Group].tx, source)
				}
			}
			stop()
			if e != nil {
				reason = e.Error()
				return
			}
		}
		if complete {
			break
		}
	}
	call, stop := context.WithTimeout(ctx, canonical.Deadline)
	defer stop()
	if e := s.snapshotLayoutValid(call, units, topology); e != nil {
		reason = "capture topology/decisions unavailable"
		return
	}
	for _, v := range views {
		var value int
		if e := v.tx.QueryRow(call, "SELECT 1").Scan(&value); e != nil {
			reason = "retained view lost"
			return
		}
	}
	if e := s.snapshotHook(call, "before_snapshot_publication"); e != nil {
		reason = e.Error()
		return
	}
	e := s.PublishCapture(call, token, func(call context.Context, tx pgx.Tx) error {
		for _, u := range units {
			var count int64
			var sum string
			e := tx.QueryRow(call, `SELECT record_count,sha256 FROM audit_snapshot_markers WHERE route_key=$1 AND capture_id=$2 AND source_shard=$3`, u.Route, token.ID, u.ID).Scan(&count, &sum)
			if e != nil || count != u.Count || sum != u.Sum {
				return canonical.Unavailable()
			}
		}
		return s.confirmRecord(call, `SELECT NOT EXISTS(SELECT 1 FROM audit_snapshot_units WHERE capture_id=$1 AND (NOT complete OR NOT verified)) AND (SELECT count(*) FROM audit_snapshot_units WHERE capture_id=$1)=$2`, token.ID, len(units))
	})
	if e != nil { // Resolve original identity; never recapture after an unknown reply.
		check, done := context.WithTimeout(context.Background(), canonical.Deadline)
		v, err := s.snapshotStatus(check, token.ID)
		done()
		if err != nil || v.Status != "ready" {
			reason = "snapshot publication unresolved"
			return
		}
	}
	failed = false
}

type frozenRecord struct {
	Route string
	Event domain.HazardEvent
}

func recordDigest(r frozenRecord) []byte {
	e := r.Event
	attrs, _ := json.Marshal(e.Attributes)
	return rawinput.Compact(r.Route, e.HazardID, string(e.Source), e.SourceRefID, string(e.HazardType), string(e.Severity), e.AreaName, e.Latitude, e.Longitude, e.OccurredAt.UTC().Format(time.RFC3339Nano), e.IngestedAt.UTC().Format(time.RFC3339Nano), string(attrs))
}
func hashState(raw []byte) (hash.Hash, error) {
	h := sha256.New()
	if len(raw) > 0 {
		if e := h.(encoding.BinaryUnmarshaler).UnmarshalBinary(raw); e != nil {
			return nil, e
		}
	}
	return h, nil
}
func (s *Store) copySnapshotPage(ctx context.Context, token CaptureToken, u *snapshotUnit, view pgx.Tx, source *domain.Source) error {
	h, e := hashState(u.State)
	if e != nil {
		return e
	}
	var sourceText string
	if source != nil {
		sourceText = string(*source)
	}
	rows, e := view.Query(ctx, `SELECT route_key,`+selectColumns+` FROM `+physicalTable(u.Source)+` WHERE active AND ($1='' OR source=$1) AND ($2::timestamptz IS NULL OR (ingested_at,hazard_id)>($2,$3::uuid)) ORDER BY ingested_at,hazard_id LIMIT 100`, sourceText, u.Last, nullableID(u.LastID))
	if e != nil {
		return e
	}
	batch := []frozenRecord{}
	size := 0
	byteBound := false
	for rows.Next() {
		var r frozenRecord
		var src, typ, sev string
		var occurred, ingested time.Time
		var attrs []byte
		e = rows.Scan(&r.Route, &r.Event.HazardID, &src, &r.Event.SourceRefID, &typ, &sev, &r.Event.AreaName, &r.Event.Latitude, &r.Event.Longitude, &occurred, &ingested, &attrs)
		if e != nil {
			break
		}
		r.Event.Source = domain.Source(src)
		r.Event.HazardType = domain.HazardType(typ)
		r.Event.Severity = domain.Severity(sev)
		r.Event.OccurredAt.Time = occurred
		r.Event.IngestedAt.Time = ingested
		d := json.NewDecoder(strings.NewReader(string(attrs)))
		d.UseNumber()
		if e = d.Decode(&r.Event.Attributes); e != nil {
			break
		}
		n := len(recordDigest(r))
		if size+n > 1<<20 && len(batch) > 0 {
			byteBound = true
			break
		}
		size += n
		if size > 1<<20 {
			e = fmt.Errorf("snapshot page exceeds 1 MiB budget")
			break
		}
		batch = append(batch, r)
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		return e
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if e = s.ProtectCaptureCopy(ctx, tx, token); e != nil {
		return e
	}
	next := *u
	for _, r := range batch {
		v := r.Event
		attrs, _ := json.Marshal(v.Attributes)
		_, e = tx.Exec(ctx, `INSERT INTO audit_snapshot_records(capture_id,route_key,`+selectColumns+`) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) ON CONFLICT DO NOTHING`, token.ID, r.Route, v.HazardID, v.Source, v.SourceRefID, v.HazardType, v.Severity, v.AreaName, v.Latitude, v.Longitude, v.OccurredAt.Time, v.IngestedAt.Time, attrs)
		if e != nil {
			return e
		}
		_, _ = h.Write(recordDigest(r))
		_, _ = h.Write([]byte{'\n'})
		next.Count++
		at := v.IngestedAt.Time
		next.Last = &at
		next.LastID = v.HazardID
	}
	next.State, _ = h.(encoding.BinaryMarshaler).MarshalBinary()
	next.Sum = hex.EncodeToString(h.Sum(nil))
	next.Complete = !byteBound && len(batch) < 100
	_, e = tx.Exec(ctx, `UPDATE audit_snapshot_units SET record_count=$3,sha256=$4,hash_state=$5,last_time=$6,last_id=$7,complete=$8 WHERE capture_id=$1 AND source_shard=$2`, token.ID, u.ID, next.Count, next.Sum, next.State, next.Last, nullableID(next.LastID), next.Complete)
	if e != nil {
		return e
	}
	if next.Complete {
		_, e = tx.Exec(ctx, `INSERT INTO audit_snapshot_markers(route_key,capture_id,source_shard,record_count,sha256) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, u.Route, token.ID, u.ID, next.Count, next.Sum)
		if e != nil {
			return e
		}
	}
	if e = tx.Commit(ctx); e != nil { // Reconcile checkpoint after a lost reply while views remain alive.
		call, stop := context.WithTimeout(context.Background(), canonical.Deadline)
		defer stop()
		var count int64
		var sum string
		if s.Pool.QueryRow(call, `SELECT record_count,sha256 FROM audit_snapshot_units WHERE capture_id=$1 AND source_shard=$2`, token.ID, u.ID).Scan(&count, &sum) != nil || count != next.Count || sum != next.Sum {
			return canonical.Unavailable()
		}
	}
	*u = next
	return nil
}
func nullableID(id string) any {
	if id == "" {
		return nil
	}
	return id
}

func (s *Store) cleanupSnapshots(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, canonical.Deadline)
	defer cancel()
	ids, e := s.ExpireCaptures(ctx)
	if e != nil {
		return
	}
	for _, id := range ids {
		tx, e := s.Pool.Begin(ctx)
		if e != nil {
			return
		}
		var terminal bool
		e = tx.QueryRow(ctx, `SELECT state IN ('aborted','expired') FROM platform_audit_captures WHERE capture_id=$1 FOR UPDATE`, id).Scan(&terminal)
		if e != nil || !terminal {
			_ = tx.Rollback(ctx)
			continue
		}
		if e = s.Canonical.Durability.Prepare(ctx, tx); e != nil {
			_ = tx.Rollback(ctx)
			return
		}
		rows, e := tx.Query(ctx, `SELECT route_key,hazard_id::text FROM audit_snapshot_records WHERE capture_id=$1 LIMIT 100`, id)
		if e != nil {
			_ = tx.Rollback(ctx)
			return
		}
		type key struct{ r, id string }
		keys := []key{}
		for rows.Next() {
			var k key
			if e = rows.Scan(&k.r, &k.id); e != nil {
				break
			}
			keys = append(keys, k)
		}
		if e == nil {
			e = rows.Err()
		}
		rows.Close()
		for _, k := range keys {
			if e != nil {
				break
			}
			_, e = tx.Exec(ctx, `DELETE FROM audit_snapshot_records WHERE route_key=$1 AND capture_id=$2 AND hazard_id=$3`, k.r, id, k.id)
		}
		if e == nil && len(keys) == 0 {
			_, e = tx.Exec(ctx, `DELETE FROM audit_snapshot_markers WHERE capture_id=$1`, id)
			if e == nil {
				_, e = tx.Exec(ctx, `DELETE FROM audit_snapshot_units WHERE capture_id=$1`, id)
			}
		}
		if e == nil {
			e = tx.Commit(ctx)
		} else {
			_ = tx.Rollback(ctx)
		}
		if e != nil {
			return
		}
		if len(keys) == 0 {
			_ = s.MarkCaptureCleaned(ctx, id)
		}
	}
}

var _ audit.Sessions = (*Store)(nil)
