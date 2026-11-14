package store

import (
	"aggregator/internal/analytics"
	"aggregator/internal/canonical"
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"sort"
	"time"
)

type analyticsState struct {
	slots          chan struct{}
	pools          map[int]*pgxpool.Pool
	testAfterShard func(context.Context) error // Synchronized in-process tests only.
}

func (s *Store) ConfigureAnalytics(ctx context.Context) (func(), error) {
	if s.Reads.Inventory == nil {
		return nil, fmt.Errorf("analytics requires configured authoritative reads")
	}
	state := &analyticsState{slots: make(chan struct{}, 1), pools: map[int]*pgxpool.Pool{}}
	close := func() {
		for _, p := range state.pools {
			p.Close()
		}
	}
	groups := map[int]*pgxpool.Pool{0: s.Pool}
	for id, p := range s.Reads.Groups {
		groups[id] = p
	}
	for id, p := range groups {
		c := p.Config().Copy()
		c.MaxConns = 2
		c.MinConns = 0
		c.ConnConfig.RuntimeParams["statement_timeout"] = "1500"
		c.ConnConfig.RuntimeParams["work_mem"] = "4MB"
		c.ConnConfig.RuntimeParams["max_parallel_workers_per_gather"] = "0"
		pool, e := pgxpool.NewWithConfig(ctx, c)
		if e != nil {
			close()
			return nil, e
		}
		state.pools[id] = pool
	}
	s.analytics = state
	return close, nil
}

type aggregate struct {
	kind, key, typ string
	at             *time.Time
	count          int64
}

func aggregateSQL(table string, snapshot bool) string {
	population := "active"
	if snapshot {
		population = "capture_id=$5::uuid"
	}
	return `WITH b AS MATERIALIZED (SELECT hazard_type,severity,area_name,date_bin($1::interval,occurred_at,'1970-01-01'::timestamptz) AS bucket,
 (source='BMKG' AND hazard_type='SEISMIC' AND attributes->'potential_tsunami'='true'::jsonb AND jsonb_typeof(attributes->'warning_id')='string' AND attributes->>'warning_id'<>'' AND attributes->>'related_event_id'=source_ref_id) AS tsunami
 FROM ` + table + ` WHERE ` + population + ` AND occurred_at>=$2 AND occurred_at<$3 AND ($4='' OR source=$4))
 SELECT 'time',hazard_type,hazard_type,bucket,count(*) FROM b GROUP BY hazard_type,bucket
 UNION ALL SELECT 'severity',severity,'',NULL,count(*) FROM b GROUP BY severity
 UNION ALL SELECT 'area',area_name,'',NULL,count(*) FROM b GROUP BY area_name
 UNION ALL SELECT 'tsunami','','',bucket,count(*) FROM b WHERE tsunami GROUP BY bucket LIMIT 10012`
}
func (s *Store) AnalyticsSummary(ctx context.Context, q analytics.Query) (analytics.Result, error) {
	if s.analytics == nil {
		return analytics.Result{}, canonical.Unavailable()
	}
	select {
	case s.analytics.slots <- struct{}{}:
		defer func() { <-s.analytics.slots }()
	default:
		return analytics.Result{}, canonical.Fail(429, "overloaded", "analytics query capacity exhausted")
	}
	ctx, cancel := context.WithTimeout(ctx, canonical.Deadline)
	defer cancel()
	var out analytics.Result
	collect := func(ctx context.Context) error {
		out = analytics.Result{Query: q, Types: map[string]int64{"SEISMIC": 0, "VOLCANIC": 0, "WILDFIRE": 0, "COASTAL": 0}, Severity: map[string]int64{"NORMAL": 0, "WASPADA": 0, "SIAGA": 0, "AWAS": 0}, Areas: []analytics.Area{}, Series: []analytics.Bucket{}, Unavailable: []string{}, DataComplete: true, Mode: "live", AsOf: time.Now().UTC()}
		shards, topology, e := s.readCatalog(ctx)
		if e != nil {
			return e
		}
		inv, e := s.readInventory(ctx)
		if e != nil {
			return e
		}
		out.Unavailable = inv.Unavailable()
		var release func(bool) error
		if q.Snapshot != "" {
			var at time.Time
			shards, at, release, e = s.snapshotRead(ctx, q.Snapshot, q.Source)
			if e != nil {
				return e
			}
			defer release(false)
			out.Mode = "snapshot"
			out.AsOf = at
		}
		areas := map[string]int64{}
		buckets := map[time.Time]*analytics.Bucket{}
		step := 24 * time.Hour
		if q.Interval == "hour" {
			step = time.Hour
		}
		for at := q.From.Truncate(step); at.Before(q.To); at = at.Add(step) {
			b := &analytics.Bucket{At: at, Counts: map[string]int64{"SEISMIC": 0, "VOLCANIC": 0, "WILDFIRE": 0, "COASTAL": 0}}
			buckets[at] = b
		}
		for _, shard := range shards {
			unit, ok := inv.Primary(shard.Group)
			pool := s.analytics.pools[shard.Group]
			if q.Snapshot != "" {
				pool = shard.pool
				ok = pool != nil
			}
			if !ok || pool == nil {
				out.DataComplete = false
				for _, u := range inv.Units {
					if u.Group == shard.Group {
						out.Unavailable = mergeNames(out.Unavailable, []string{u.Name})
					}
				}
				continue
			}
			call, stop := context.WithTimeout(ctx, 500*time.Millisecond)
			args := []any{"1 " + q.Interval, q.From, q.To, q.Source}
			if q.Snapshot != "" {
				args = append(args, q.Snapshot)
			}
			rows, err := pool.Query(call, aggregateSQL(physicalTable(shard.Table), q.Snapshot != ""), args...)
			records := []aggregate{}
			bytes := 0
			quota := false
			if err == nil {
				for rows.Next() {
					var a aggregate
					if err = rows.Scan(&a.kind, &a.key, &a.typ, &a.at, &a.count); err != nil {
						break
					}
					bytes += len(a.key) + 128
					if bytes > analytics.MaxResponse || len(records) >= 10011 {
						quota = true
						break
					}
					records = append(records, a)
				}
				if err == nil {
					err = rows.Err()
				}
				rows.Close()
			}
			stop()
			if quota {
				return canonical.Fail(503, "analytics_quota", "aggregate memory budget exceeded")
			}
			if err != nil {
				out.DataComplete = false
				if unit.Name != "" {
					out.Unavailable = mergeNames(out.Unavailable, []string{unit.Name})
				} else {
					for _, u := range inv.Units {
						if u.Group == shard.Group {
							out.Unavailable = mergeNames(out.Unavailable, []string{u.Name})
						}
					}
				}
				continue
			}
			if hook := s.analytics.testAfterShard; hook != nil {
				if e := hook(ctx); e != nil {
					return e
				}
			}
			for _, a := range records {
				switch a.kind {
				case "area":
					areas[a.key] += a.count
				case "severity":
					out.Severity[a.key] += a.count
				case "time":
					out.Types[a.key] += a.count
					out.Total += a.count
					if b := buckets[a.at.UTC()]; b != nil {
						b.Counts[a.key] += a.count
					}
				case "tsunami":
					out.Tsunami += a.count
					if b := buckets[a.at.UTC()]; b != nil {
						b.Tsunami += a.count
					}
				}
			}
			if len(areas) > 5000 {
				return canonical.Fail(503, "analytics_quota", "area group budget exceeded")
			}
		}
		for name, count := range areas {
			out.Areas = append(out.Areas, analytics.Area{Name: name, Count: count})
		}
		sort.Slice(out.Areas, func(i, j int) bool { return out.Areas[i].Name < out.Areas[j].Name })
		for _, b := range buckets {
			out.Series = append(out.Series, *b)
		}
		sort.Slice(out.Series, func(i, j int) bool { return out.Series[i].At.Before(out.Series[j].At) })
		out.Partial = !out.DataComplete || len(out.Unavailable) > 0
		current, _, e := s.primaryTopology(ctx)
		if e != nil || current != topology {
			return canonical.Unavailable()
		}
		if q.Snapshot == "" {
			out.Generation, _, e = s.visibility(ctx)
			if e != nil {
				return e
			}
		}
		freshness, e := s.PollStatuses(ctx, []string{"BMKG", "BMKG_TSUNAMI", "PVMBG"})
		if e != nil {
			out.Freshness = map[string]string{"state": "unavailable"}
		} else {
			out.Freshness = map[string]any{"endpoints": freshness, "SATELIT": "policy_unavailable", "SENSOR_PANTAI": "policy_unavailable"}
		}
		body, e := json.Marshal(out)
		if e != nil || len(body) > analytics.MaxResponse {
			return canonical.Fail(503, "analytics_quota", "response budget exceeded")
		}
		if release != nil {
			return release(true)
		}
		return nil
	}
	var err error
	if q.Snapshot != "" {
		err = collect(ctx)
	} else {
		err = s.validatedRead(ctx, collect)
	}
	if err != nil {
		return analytics.Result{}, err
	}
	return out, nil
}

var _ analytics.Repository = (*Store)(nil)
