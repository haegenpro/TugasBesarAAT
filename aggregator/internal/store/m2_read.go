package store

import (
	"aggregator/internal/domain"
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"sort"
	"strconv"
	"strings"
	"time"
)

const visibilityLock int64 = 4031002

var ErrUnavailable = errors.New("canonical visibility incomplete")

func hazardQuery(f Filters, table string, active bool) (string, []any) {
	columns := selectColumnsWithMetadata
	if f.SummaryOnly {
		// Freshness metadata still needs potential_tsunami, but the summary
		// path never needs precise coordinates, source IDs or raw attributes.
		columns = `hazard_id,source,''::text AS source_ref_id,hazard_type,severity,area_name,
		0::double precision AS latitude,0::double precision AS longitude,occurred_at,ingested_at,
		jsonb_build_object('potential_tsunami',attributes->'potential_tsunami') AS attributes,revision,last_seen_at`
	}
	query := `SELECT ` + columns + ` FROM ` + table + ` WHERE true`
	if active {
		query += ` AND active`
	}
	args := []any{}
	add := func(value any) string { args = append(args, value); return "$" + strconv.Itoa(len(args)) }
	if !f.Since.IsZero() {
		query += ` AND occurred_at>` + add(f.Since)
	}
	if f.HazardType != "" {
		query += ` AND hazard_type=` + add(f.HazardType)
	}
	if f.Severity != "" {
		query += ` AND severity=` + add(f.Severity)
	}
	if f.AreaName != "" {
		query += ` AND area_name ILIKE ` + add("%"+f.AreaName+"%")
	}
	if f.BeforeTime != nil {
		query += ` AND (occurred_at,hazard_id)<(` + add(*f.BeforeTime) + `,` + add(f.BeforeID) + `::uuid)`
	}
	query += ` ORDER BY occurred_at DESC,hazard_id DESC`
	if f.Limit > 0 {
		query += ` LIMIT ` + add(f.Limit)
	}
	return query, args
}
func collectHazards(rows pgx.Rows) ([]domain.HazardEvent, map[string]domain.RecordMetadata, error) {
	defer rows.Close()
	events := []domain.HazardEvent{}
	metadata := map[string]domain.RecordMetadata{}
	for rows.Next() {
		var m domain.RecordMetadata
		h, e := scanHazard(rows, &m)
		if e != nil {
			return nil, nil, e
		}
		events = append(events, h)
		metadata[h.HazardID] = m
	}
	return events, metadata, rows.Err()
}
func (s *Store) readBarrier(ctx context.Context) (pgx.Tx, error) {
	// Execute BEGIN and the shared lock as separate SQL statements in one
	// round trip. The publication check below MUST remain a later statement:
	// its READ COMMITTED snapshot must be taken after any lock wait completes.
	// Combining lock acquisition and that check into one SELECT could use a
	// snapshot from before the writer committed.
	tx, e := s.Pool.BeginTx(ctx, pgx.TxOptions{BeginQuery: fmt.Sprintf(
		"BEGIN; SELECT pg_advisory_xact_lock_shared(%d)", visibilityLock)})
	if e != nil {
		return nil, fmt.Errorf("%w: visibility lock: %v", ErrUnavailable, e)
	}
	deadline := time.Now().Add(200 * time.Millisecond)
	for {
		var pending bool
		if e = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM canonical_visibility WHERE NOT published)`).Scan(&pending); e != nil {
			tx.Rollback(ctx)
			return nil, fmt.Errorf("%w: publication state: %v", ErrUnavailable, e)
		}
		if !pending {
			break
		}
		if time.Now().After(deadline) {
			tx.Rollback(ctx)
			return nil, ErrUnavailable
		}
		select {
		case <-ctx.Done():
			tx.Rollback(ctx)
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}

	return tx, nil
}
func (s *Store) distributedQuery(ctx context.Context, f Filters) (Page, error) {
	out := Page{Events: []domain.HazardEvent{}, Metadata: map[string]domain.RecordMetadata{}, UnavailableUnits: []string{}}
	tx, e := s.readBarrier(ctx)
	if e != nil {
		out.Partial = true
		out.UnavailableUnits = []string{"canonical-visibility"}
		return out, nil
	}
	if s.WorkerReads {
		defer tx.Rollback(ctx)
		return s.queryWorkerGroups(ctx, tx, f, false)
	}
	query, args := hazardQuery(f, "hazard_events", true)
	rows, e := tx.Query(ctx, query, args...)
	if e == nil {
		out.Events, out.Metadata, e = collectHazards(rows)
	}
	tx.Rollback(ctx)
	if e == nil {
		return out, nil
	}
	// A failed distributed statement aborts its transaction. Reacquire the shared
	// barrier before reading reachable physical shards, and fail closed while
	// Citus is resolving an unfinished distributed commit.
	tx, e = s.readBarrier(ctx)
	if e != nil {
		out.Partial = true
		out.UnavailableUnits = []string{"canonical-visibility"}
		return out, nil
	}
	defer tx.Rollback(ctx)
	return s.queryWorkerGroups(ctx, tx, f, true)
}

func (s *Store) queryWorkerGroups(ctx context.Context, tx pgx.Tx, f Filters, fallback bool) (Page, error) {
	out := Page{Events: []domain.HazardEvent{}, Metadata: map[string]domain.RecordMetadata{}, UnavailableUnits: []string{}}
	var e error
	type group struct {
		member storageMember
		shards []int64
	}
	groups := map[int]*group{}
	catalog, e := tx.Query(ctx, `SELECT n.groupid,n.nodename,n.nodeport,s.shardid FROM pg_dist_shard s JOIN pg_dist_placement p USING(shardid) JOIN pg_dist_node n USING(groupid) WHERE s.logicalrelid='hazard_events'::regclass AND p.shardstate=1 AND n.noderole='primary' AND n.isactive ORDER BY n.groupid,s.shardid`)
	if e != nil {
		out.Partial = true
		out.UnavailableUnits = []string{"canonical-catalog"}
		return out, nil
	}
	for catalog.Next() {
		var m storageMember
		var shard int64
		if e = catalog.Scan(&m.Group, &m.Host, &m.Port, &shard); e != nil {
			catalog.Close()
			return out, e
		}
		if groups[m.Group] == nil {
			groups[m.Group] = &group{member: m}
		}
		groups[m.Group].shards = append(groups[m.Group].shards, shard)
	}
	e = catalog.Err()
	catalog.Close()
	if e != nil {
		return out, e
	}
	type result struct {
		events      []domain.HazardEvent
		metadata    map[string]domain.RecordMetadata
		unavailable string
	}
	results := make(chan result, len(groups))
	for _, g := range groups {
		go func(g *group) {
			r := result{metadata: map[string]domain.RecordMetadata{}}
			connection, e := s.workerConnection(ctx, g.member)
			if e != nil {
				r.unavailable = unit(g.member)
				results <- r
				return
			}
			defer connection.Release()
			var recovery bool
			if e = connection.QueryRow(ctx, `SELECT pg_is_in_recovery()`).Scan(&recovery); e != nil || recovery {
				r.unavailable = unit(g.member)
				results <- r
				return
			}
			query, args := groupHazardQuery(f, g.shards)
			rows, e := connection.Query(ctx, query, args...)
			if e == nil {
				r.events, r.metadata, e = collectHazards(rows)
			}
			if e != nil {
				r.unavailable = unit(g.member)
				r.events = nil
				r.metadata = map[string]domain.RecordMetadata{}
			}
			results <- r
		}(g)
	}
	out.Events = []domain.HazardEvent{}
	out.Metadata = map[string]domain.RecordMetadata{}
	out.Partial = fallback
	shards := 0
	for _, g := range groups {
		shards += len(g.shards)
	}
	if shards != 8 || len(groups) != 2 {
		out.Partial = true
		out.UnavailableUnits = append(out.UnavailableUnits, "canonical-catalog")
	}
	for range groups {
		r := <-results
		out.Events = append(out.Events, r.events...)
		for k, v := range r.metadata {
			out.Metadata[k] = v
		}
		if r.unavailable != "" {
			out.Partial = true
			out.UnavailableUnits = append(out.UnavailableUnits, r.unavailable)
		}
	}
	if out.Partial && len(out.UnavailableUnits) == 0 {
		out.UnavailableUnits = []string{"canonical-query"}
	}
	sort.Slice(out.Events, func(i, j int) bool {
		a, b := out.Events[i], out.Events[j]
		if a.OccurredAt.Equal(b.OccurredAt.Time) {
			return strings.Compare(a.HazardID, b.HazardID) > 0
		}
		return a.OccurredAt.After(b.OccurredAt.Time)
	})
	if f.Limit > 0 && len(out.Events) > f.Limit {
		out.Events = out.Events[:f.Limit]
	}
	sort.Strings(out.UnavailableUnits)
	return out, nil
}
