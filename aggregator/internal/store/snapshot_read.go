package store

import (
	"aggregator/internal/audit"
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/jackc/pgx/v5/pgxpool"
	"sort"
	"strconv"
	"strings"
	"time"
)

func (s *Store) snapshotUnits(ctx context.Context, id string) ([]snapshotUnit, error) {
	rows, e := s.Pool.Query(ctx, `SELECT source_shard,group_id,source_table,target_table,marker_table,marker_route,record_count,coalesce(sha256,''),complete,verified FROM audit_snapshot_units WHERE capture_id=$1 ORDER BY source_shard`, id)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []snapshotUnit{}
	for rows.Next() {
		var u snapshotUnit
		if e = rows.Scan(&u.ID, &u.Group, &u.Source, &u.Target, &u.Marker, &u.Route, &u.Count, &u.Sum, &u.Complete, &u.Verified); e != nil {
			return nil, e
		}
		out = append(out, u)
		if len(out) > 256 {
			return nil, canonical.Unavailable()
		}
	}
	return out, rows.Err()
}

// A completion marker is written after every immutable page in the same WAL
// stream. A verified replica carrying the matching marker has replayed the cut.
func markerMatches(ctx context.Context, p *pgxpool.Pool, id string, u snapshotUnit) bool {
	var n int64
	var sum string
	e := p.QueryRow(ctx, `SELECT record_count,sha256 FROM `+physicalTable(u.Marker)+` WHERE route_key=$1 AND capture_id=$2 AND source_shard=$3`, u.Route, id, u.ID).Scan(&n, &sum)
	return e == nil && n == u.Count && sum == u.Sum
}
func (s *Store) frozenReplica(ctx context.Context, id string, u snapshotUnit, inv audit.Inventory) (*pgxpool.Pool, error) {
	real, ok := s.Canonical.Durability.(*replicatedDurability)
	if !ok {
		return nil, canonical.Unavailable()
	}
	for _, g := range real.cfg.Groups {
		if g.ID != u.Group {
			continue
		}
		for _, m := range g.Members {
			eligible := false
			for _, unit := range inv.Units {
				if unit.Group == g.ID && unit.Name == m.Name && unit.Available && unit.RoleKnown {
					eligible = true
				}
			}
			if !eligible {
				continue
			}
			base := s.snapshots.reads[g.ID]
			if base == nil {
				continue
			}
			config := base.Config().Copy()
			config.ConnConfig.Host = m.Host
			config.ConnConfig.Port = uint16(m.Port)
			config.MaxConns = 1
			pool, e := pgxpool.NewWithConfig(ctx, config)
			if e != nil {
				continue
			}
			status, e := readReplication(ctx, pool)
			if e == nil && status.Formation == real.cfg.Formation && status.GroupID == g.ID && status.SystemID == g.SystemID && status.Member == m.Name && markerMatches(ctx, pool, id, u) {
				return pool, nil
			}
			pool.Close()
		}
	}
	return nil, canonical.Unavailable()
}

// Guard GC/expiry through result assembly and COMMIT. Caller must commit before
// emitting bytes. Pools/identities are selected only by the storage owner.
func (s *Store) snapshotRead(ctx context.Context, id, source string) ([]readShard, time.Time, func(bool) error, error) {
	if s.snapshots == nil {
		return nil, time.Time{}, nil, canonical.Unavailable()
	}
	v, e := s.snapshotStatus(ctx, id)
	if e != nil {
		return nil, time.Time{}, nil, e
	}
	if v.Status == "building" {
		return nil, time.Time{}, nil, canonical.Fail(503, "snapshot_not_ready", "snapshot building")
	}
	if v.Status != "ready" {
		return nil, time.Time{}, nil, canonical.Fail(400, "cursor_expired", "snapshot aborted or expired")
	}
	if v.Source != nil && source != "" && string(*v.Source) != source {
		return nil, time.Time{}, nil, canonical.Fail(400, "bad_request", "source exceeds snapshot scope")
	}
	if e = s.confirmSnapshotMetadata(ctx, id); e != nil {
		return nil, time.Time{}, nil, canonical.Unavailable()
	}
	if e = s.TouchSnapshot(ctx, id); e != nil {
		return nil, time.Time{}, nil, canonical.Fail(400, "cursor_expired", "snapshot expired")
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return nil, time.Time{}, nil, e
	}
	if e = s.ProtectSnapshotRead(ctx, tx, id); e != nil {
		_ = tx.Rollback(ctx)
		return nil, time.Time{}, nil, e
	}
	owned := []*pgxpool.Pool{}
	finished := false
	release := func(commit bool) error {
		if finished {
			return nil
		}
		finished = true
		defer func() {
			for _, p := range owned {
				p.Close()
			}
		}()
		if commit {
			if e := tx.Commit(ctx); e != nil {
				return canonical.CommitError(e)
			}
			return nil
		}
		return tx.Rollback(ctx)
	}
	units, e := s.snapshotUnits(ctx, id)
	if e != nil || len(units) == 0 {
		_ = release(false)
		return nil, time.Time{}, nil, canonical.Unavailable()
	}
	inv, e := s.readInventory(ctx)
	if e != nil {
		_ = release(false)
		return nil, time.Time{}, nil, e
	}
	catalog, _, e := s.readCatalog(ctx)
	if e != nil {
		_ = release(false)
		return nil, time.Time{}, nil, e
	}
	groups := map[int64]int{}
	for _, sh := range catalog {
		groups[sh.ID] = sh.Group
	}
	out := []readShard{}
	for _, u := range units {
		if !u.Complete || !u.Verified {
			_ = release(false)
			return nil, time.Time{}, nil, canonical.Unavailable()
		}
		if g, ok := groups[u.ID]; ok {
			u.Group = g
		} else {
			_ = release(false)
			return nil, time.Time{}, nil, canonical.Unavailable()
		}
		var pool *pgxpool.Pool
		if _, ok := inv.Primary(u.Group); ok {
			p := s.snapshots.reads[u.Group]
			if p != nil {
				call, stop := context.WithTimeout(ctx, 250*time.Millisecond)
				if markerMatches(call, p, id, u) {
					pool = p
				}
				stop()
			}
		}
		if pool == nil {
			call, stop := context.WithTimeout(ctx, 300*time.Millisecond)
			p, err := s.frozenReplica(call, id, u, inv)
			stop()
			if err == nil {
				pool = p
				owned = append(owned, p)
			}
		}
		out = append(out, readShard{ID: u.ID, Group: u.Group, Table: u.Target, pool: pool})
	}
	if v.Cut == nil {
		_ = release(false)
		return nil, time.Time{}, nil, canonical.Unavailable()
	}
	return out, *v.Cut, release, nil
}
func frozenWhere(q readSpec, id string) (string, []any) {
	where, args := q.where()
	args = append(args, id)
	return where + " AND capture_id=$" + strconv.Itoa(len(args)) + "::uuid", args
}
func (s *Store) SnapshotEvents(ctx context.Context, q audit.Query) (audit.Page, error) {
	out := audit.Page{Items: []domain.HazardEvent{}, Unavailable: []string{}}
	if q.Limit < 1 || q.Limit > 1000 || !s.Reads.Signer.Valid() {
		return out, canonical.Fail(400, "bad_request", "invalid page")
	}
	shards, _, release, e := s.snapshotRead(ctx, q.Snapshot, q.Source)
	if e != nil {
		return out, e
	}
	defer release(false)
	inv, e := s.readInventory(ctx)
	if e != nil {
		return out, e
	}
	out.Unavailable = inv.Unavailable()
	missing := false
	bytes := 0
	for _, shard := range shards {
		if shard.pool == nil {
			missing = true
			for _, u := range inv.Units {
				if u.Group == shard.Group {
					out.Unavailable = mergeNames(out.Unavailable, []string{u.Name})
				}
			}
			continue
		}
		spec := readSpec{Source: q.Source, Since: q.Since, Ingestion: true, Limit: q.Limit + 1}
		if q.After.ID != "" {
			spec.AfterID = q.After.ID
			spec.AfterTime = &q.After.Time
		}
		where, args := frozenWhere(spec, q.Snapshot)
		args = append(args, q.Limit+1)
		call, stop := context.WithTimeout(ctx, 500*time.Millisecond)
		rows, err := shard.pool.Query(call, `SELECT `+selectColumns+` FROM `+physicalTable(shard.Table)+where+spec.order()+" LIMIT $"+strconv.Itoa(len(args)), args...)
		if err == nil {
			for rows.Next() {
				event, readErr := scanRow(rows)
				if readErr != nil {
					err = readErr
					break
				}
				b, _ := json.Marshal(event)
				bytes += len(b)
				if bytes > 4<<20 {
					rows.Close()
					stop()
					return out, canonical.Fail(503, "snapshot_quota", "page memory budget exceeded")
				}
				out.Items = append(out.Items, event)
			}
			if err == nil {
				err = rows.Err()
			}
			rows.Close()
		}
		stop()
		if err != nil {
			missing = true
			for _, u := range inv.Units {
				if u.Group == shard.Group {
					out.Unavailable = mergeNames(out.Unavailable, []string{u.Name})
				}
			}
		}
	}
	if missing {
		out.Items = []domain.HazardEvent{}
	} else {
		sort.Slice(out.Items, func(i, j int) bool {
			a, b := out.Items[i], out.Items[j]
			if a.IngestedAt.Equal(b.IngestedAt.Time) {
				return a.HazardID < b.HazardID
			}
			return a.IngestedAt.Before(b.IngestedAt.Time)
		})
	}
	pos := q.After
	pos.Endpoint = "audit/events"
	pos.Scope = q.Scope
	pos.Filters = q.Filters()
	pos.Snapshot = &q.Snapshot
	pos.Units = requiredUnits(inv)
	more := len(out.Items) > q.Limit
	if more {
		out.Items = out.Items[:q.Limit]
	}
	if more || missing {
		if len(out.Items) > 0 {
			last := out.Items[len(out.Items)-1]
			pos.ID = last.HazardID
			pos.Time = last.IngestedAt.Time
		}
		next, e := s.Reads.Signer.Encode(pos)
		if e != nil {
			return out, e
		}
		out.NextCursor = &next
	}
	out.Partial = missing || len(out.Unavailable) > 0
	if e = release(true); e != nil {
		return audit.Page{}, e
	}
	return out, nil
}
func (s *Store) SnapshotChecksum(ctx context.Context, id, source string) (audit.Checksum, error) {
	out := audit.Checksum{Algorithm: "sha256", Unavailable: []string{}}
	shards, at, release, e := s.snapshotRead(ctx, id, source)
	if e != nil {
		return out, e
	}
	defer release(false)
	out.AsOf = at
	v, e := s.snapshotStatus(ctx, id)
	if e != nil {
		return out, e
	}
	if source == "" && v.Source != nil {
		source = string(*v.Source)
	}
	if source != "" {
		out.Source = &source
	}
	inv, e := s.readInventory(ctx)
	if e != nil {
		return out, e
	}
	out.Unavailable = inv.Unavailable()
	streams := []*referenceRows{}
	groups := map[*pgxpool.Pool][]readShard{}
	defer func() {
		for _, stream := range streams {
			stream.rows.Close()
		}
	}()
	for _, shard := range shards {
		if shard.pool != nil {
			groups[shard.pool] = append(groups[shard.pool], shard)
			continue
		}
		if shard.pool == nil {
			out.Partial = true
			for _, u := range inv.Units {
				if u.Group == shard.Group {
					out.Unavailable = mergeNames(out.Unavailable, []string{u.Name})
				}
			}
			continue
		}
	}
	for pool, group := range groups {
		parts := []string{}
		for _, shard := range group {
			parts = append(parts, `SELECT source_ref_id,source,hazard_id::text FROM `+physicalTable(shard.Table)+` WHERE capture_id=$1 AND ($2='' OR source=$2)`)
		}
		rows, err := pool.Query(ctx, `SELECT * FROM (`+strings.Join(parts, " UNION ALL ")+`) refs ORDER BY source_ref_id COLLATE "C",source COLLATE "C",hazard_id`, id, source)
		if err != nil {
			out.Partial = true
			for _, u := range inv.Units {
				if u.Group == group[0].Group {
					out.Unavailable = mergeNames(out.Unavailable, []string{u.Name})
				}
			}
			continue
		}
		stream := &referenceRows{rows: rows, group: group[0].Group}
		streams = append(streams, stream)
		if e = stream.next(); e != nil {
			return out, canonical.Unavailable()
		}
	}
	sum := sha256.New()
	for {
		var next *referenceRows
		for _, stream := range streams {
			if stream.valid && (next == nil || stream.reference < next.reference || (stream.reference == next.reference && (stream.source < next.source || (stream.source == next.source && stream.id < next.id)))) {
				next = stream
			}
		}
		if next == nil {
			break
		}
		if out.Count > 0 {
			_, _ = sum.Write([]byte{'\n'})
		}
		_, _ = sum.Write([]byte(next.reference))
		out.Count++
		if e = next.next(); e != nil {
			return out, canonical.Unavailable()
		}
	}
	out.Checksum = hex.EncodeToString(sum.Sum(nil))
	out.Partial = out.Partial || len(out.Unavailable) > 0
	if e = release(true); e != nil {
		return audit.Checksum{}, e
	}
	return out, nil
}
