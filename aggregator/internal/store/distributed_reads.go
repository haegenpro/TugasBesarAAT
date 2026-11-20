package store

import (
	"aggregator/internal/audit"
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"aggregator/internal/pagecursor"
	"aggregator/internal/platform"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ReadOptions struct {
	// Private synchronized fault points for the isolated visibility proof.
	// No HTTP parameter or runtime flag can select them.
	testUnitReads     bool
	testUnitReadFault func(context.Context, string, int) error
	Signer            pagecursor.Signer
	Inventory         func(context.Context) (audit.Inventory, error)
	// Only storage-owned primary pools. Tests may inject lab primary pools;
	// deployment derives these from the captured formation, never HTTP input.
	Groups    map[int]*pgxpool.Pool
	Authority func(context.Context, readQuerier) (replicationStatus, error)
	// Live cuts combine visibility and physical identity in one SQL snapshot.
	// The final cut additionally checks Patroni's current leader lease.
	Cut           func(context.Context, readQuerier, bool) (int64, bool, replicationStatus, error)
	MetadataReady func() bool
}
type readShard struct {
	ID    int64
	Group int
	Table string
	pool  *pgxpool.Pool // Request-owned verified frozen-copy reader; never HTTP selected.
}
type readSpec struct {
	Source                            string
	Since                             time.Time
	Ingestion                         bool
	AfterTime                         *time.Time
	AfterID, ID, Type, Severity, Area string
	Limit                             int
}
type readResult struct {
	Events      []domain.HazardEvent
	Metadata    map[string]domain.RecordMetadata
	Unavailable []string
	Missing     bool
	Inventory   audit.Inventory
	Shards      []readShard
}

func (s *Store) readCatalog(ctx context.Context) ([]readShard, string, error) {
	if r, ok := s.Canonical.Durability.(*replicatedDurability); ok && r.cfg.Mode == "citus" {
		return s.readCitusCatalog(ctx)
	}
	topology, count, err := s.primaryTopology(ctx)
	if err != nil {
		return nil, "", err
	}
	if count == 0 {
		return []readShard{{Group: 0, Table: "hazard_events"}}, topology, nil
	}
	rows, err := s.Pool.Query(ctx, `SELECT s.shardid,p.groupid FROM pg_dist_shard s JOIN pg_dist_placement p USING(shardid) WHERE s.logicalrelid='hazard_events'::regclass AND p.shardstate=1 ORDER BY s.shardid`)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	shards := []readShard{}
	seen := map[int64]bool{}
	for rows.Next() {
		var v readShard
		if err = rows.Scan(&v.ID, &v.Group); err != nil {
			return nil, "", err
		}
		if seen[v.ID] {
			return nil, "", fmt.Errorf("ambiguous logical shard placement")
		}
		seen[v.ID] = true
		v.Table = "hazard_events_" + strconv.FormatInt(v.ID, 10)
		shards = append(shards, v)
	}
	if err = rows.Err(); err != nil {
		return nil, "", err
	}
	if len(shards) == 0 {
		return nil, "", canonical.Unavailable()
	}
	return shards, topology, nil
}

// Read the routing generation and all placements in one coordinator snapshot.
// Repeated metadata round trips consume the same two-second budget as the rows.
func (s *Store) readCitusCatalog(ctx context.Context) ([]readShard, string, error) {
	var topology string
	var count, inactive int
	var raw []byte
	err := s.Pool.QueryRow(ctx, `SELECT
 (SELECT coalesce(jsonb_agg(jsonb_build_array(nodeid,nodename,nodeport,groupid) ORDER BY nodeid),'[]'::jsonb)::text FROM pg_dist_node WHERE groupid>0 AND noderole='primary'),
 (SELECT count(*) FROM pg_dist_node WHERE groupid>0 AND noderole='primary'),
 (SELECT count(*) FROM pg_dist_node WHERE groupid>0 AND noderole='primary' AND NOT isactive),
 (SELECT coalesce(jsonb_agg(jsonb_build_object('ID',s.shardid,'Group',p.groupid) ORDER BY s.shardid),'[]'::jsonb) FROM pg_dist_shard s JOIN pg_dist_placement p USING(shardid) WHERE s.logicalrelid='hazard_events'::regclass AND p.shardstate=1)`).Scan(&topology, &count, &inactive, &raw)
	if err != nil {
		return nil, "", err
	}
	if count == 0 || inactive != 0 {
		return nil, "", canonical.Unavailable()
	}
	var shards []readShard
	if err = json.Unmarshal(raw, &shards); err != nil {
		return nil, "", canonical.Unavailable()
	}
	seen := map[int64]bool{}
	for i := range shards {
		if seen[shards[i].ID] {
			return nil, "", fmt.Errorf("ambiguous logical shard placement")
		}
		seen[shards[i].ID] = true
		shards[i].Table = "hazard_events_" + strconv.FormatInt(shards[i].ID, 10)
	}
	if len(shards) == 0 {
		return nil, "", canonical.Unavailable()
	}
	return shards, topology, nil
}
func (q readSpec) where() (string, []any) {
	sql := " WHERE active"
	args := []any{}
	add := func(v any) string { args = append(args, v); return "$" + strconv.Itoa(len(args)) }
	if q.Source != "" {
		sql += " AND source=" + add(q.Source)
	}
	column, comparison := "occurred_at", ">"
	if q.Ingestion {
		column, comparison = "ingested_at", ">="
	}
	if !q.Since.IsZero() {
		sql += " AND " + column + comparison + add(q.Since)
	}
	if q.ID != "" {
		sql += " AND hazard_id=" + add(q.ID) + "::uuid"
	}
	if q.Type != "" {
		sql += " AND hazard_type=" + add(q.Type)
	}
	if q.Severity != "" {
		sql += " AND severity=" + add(q.Severity)
	}
	if q.Area != "" {
		sql += " AND area_name ILIKE " + add("%"+q.Area+"%")
	}
	if q.AfterTime != nil {
		op := "<"
		if q.Ingestion {
			op = ">"
		}
		sql += " AND (" + column + ",hazard_id)" + op + "(" + add(*q.AfterTime) + "," + add(q.AfterID) + "::uuid)"
	}
	return sql, args
}
func (q readSpec) order() string {
	if q.Ingestion {
		return " ORDER BY ingested_at ASC,hazard_id ASC"
	}
	return " ORDER BY occurred_at DESC,hazard_id DESC"
}
func fetchRows(ctx context.Context, pool *pgxpool.Pool, tables []string, q readSpec) ([]domain.HazardEvent, map[string]domain.RecordMetadata, error) {
	where, args := q.where()
	parts := make([]string, 0, len(tables))
	for _, table := range tables {
		parts = append(parts, `SELECT `+selectColumnsWithMetadata+` FROM `+table+where)
	}
	sql := parts[0]
	if len(parts) > 1 {
		sql = `SELECT * FROM (` + strings.Join(parts, " UNION ALL ") + `) logical_rows`
	}
	sql += q.order()
	if q.Limit > 0 {
		args = append(args, q.Limit)
		sql += " LIMIT $" + strconv.Itoa(len(args))
	}
	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	events := []domain.HazardEvent{}
	metadata := map[string]domain.RecordMetadata{}
	for rows.Next() {
		var m domain.RecordMetadata
		e, err := scanHazard(rows, &m)
		if err != nil {
			return nil, nil, err
		}
		events = append(events, e)
		metadata[e.HazardID] = m
	}
	return events, metadata, rows.Err()
}
func physicalTable(name string) string { return pgx.Identifier{"public", name}.Sanitize() }
func mergeNames(a, b []string) []string {
	seen := map[string]bool{}
	for _, v := range a {
		seen[v] = true
	}
	for _, v := range b {
		seen[v] = true
	}
	out := make([]string, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}
func (s *Store) readInventory(ctx context.Context) (audit.Inventory, error) {
	if s.Reads.Inventory == nil {
		return audit.Inventory{}, canonical.Unavailable()
	}
	return s.Reads.Inventory(ctx)
}

type inventoryTopologyKey struct{}

func (s *Store) readInventoryAt(ctx context.Context, topology string) (audit.Inventory, error) {
	return s.readInventory(context.WithValue(ctx, inventoryTopologyKey{}, topology))
}
func (s *Store) readDistributed(ctx context.Context, q readSpec) (readResult, error) {
	ctx, span := platform.Start(ctx, "canonical distributed rows")
	defer span.End()
	out := readResult{Events: []domain.HazardEvent{}, Metadata: map[string]domain.RecordMetadata{}, Unavailable: []string{}}
	shards, topology, err := s.readCatalog(ctx)
	if err != nil {
		return out, canonical.Unavailable()
	}
	out.Shards = shards
	var wg sync.WaitGroup
	var events []domain.HazardEvent
	var metadata map[string]domain.RecordMetadata
	var normalErr error = canonical.Unavailable()
	if !s.Reads.testUnitReads {
		wg.Go(func() {
			routed, span := platform.Start(ctx, "canonical routed read")
			defer span.End()
			normal, stop := context.WithTimeout(routed, 750*time.Millisecond)
			defer stop()
			events, metadata, normalErr = fetchRows(normal, s.Pool, []string{"hazard_events"}, q)
		})
	}
	inventory, err := s.readInventoryAt(ctx, topology)
	wg.Wait()
	if err != nil {
		return out, canonical.Unavailable()
	}
	out.Inventory = inventory
	out.Unavailable = inventory.Unavailable()
	if normalErr == nil {
		for _, shard := range shards {
			if _, ok := inventory.Primary(shard.Group); !ok {
				normalErr = canonical.Unavailable()
				break
			}
		}
	}
	if normalErr == nil {
		out.Events, out.Metadata = events, metadata
	} else {
		fallback, span := platform.Start(ctx, "canonical physical fallback")
		var mu sync.Mutex
		for group, tables := range physicalGroups(shards) {
			wg.Go(func() {
				unit, ok := inventory.Primary(group)
				pool := s.Reads.Groups[group]
				var events []domain.HazardEvent
				var metadata map[string]domain.RecordMetadata
				var err error
				if !ok || pool == nil {
					err = canonical.Unavailable()
				} else {
					call, stop := context.WithTimeout(fallback, 500*time.Millisecond)
					defer stop()
					if hook := s.Reads.testUnitReadFault; hook != nil {
						err = hook(call, "before", group)
					}
					if err == nil {
						events, metadata, err = fetchRows(call, pool, tables, q)
					}
					if err == nil {
						if hook := s.Reads.testUnitReadFault; hook != nil {
							phase := "after"
							if len(events) > 0 {
								phase = "after_rows"
							}
							err = hook(call, phase, group)
						}
					}
				}
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					out.Missing = true
					if unit.Name != "" {
						out.Unavailable = append(out.Unavailable, unit.Name)
					}
					return
				}
				out.Events = append(out.Events, events...)
				for k, v := range metadata {
					out.Metadata[k] = v
				}
			})
		}
		wg.Wait()
		span.End()
		// No trustworthy missing-unit bound is maintained in M4. Emitting a later
		// available row would skip unseen earlier rows. The safe prefix is empty.
		if out.Missing && q.ID == "" {
			out.Events = []domain.HazardEvent{}
			out.Metadata = map[string]domain.RecordMetadata{}
		}
		sort.Slice(out.Events, func(i, j int) bool {
			a, b := out.Events[i], out.Events[j]
			at, bt := a.OccurredAt.Time, b.OccurredAt.Time
			if q.Ingestion {
				at, bt = a.IngestedAt.Time, b.IngestedAt.Time
			}
			if at.Equal(bt) {
				if q.Ingestion {
					return a.HazardID < b.HazardID
				}
				return a.HazardID > b.HazardID
			}
			if q.Ingestion {
				return at.Before(bt)
			}
			return at.After(bt)
		})
		if q.Limit > 0 && len(out.Events) > q.Limit {
			out.Events = out.Events[:q.Limit]
		}
	}
	out.Unavailable = mergeNames(out.Unavailable, nil)
	current, _, err := s.primaryTopology(ctx)
	if err != nil || current != topology {
		return readResult{}, canonical.Unavailable()
	}
	// Inventory is tied to this catalog generation; promotion during collection
	// cannot silently combine old/new owners in one successful result.
	if inventory.Topology != "" && inventory.Topology != topology {
		return readResult{}, canonical.Unavailable()
	}
	return out, nil
}

func physicalGroups(shards []readShard) map[int][]string {
	groups := map[int][]string{}
	for _, shard := range shards {
		groups[shard.Group] = append(groups[shard.Group], physicalTable(shard.Table))
	}
	return groups
}

func (s *Store) AuditEvents(ctx context.Context, q audit.Query) (audit.Page, error) {
	out := audit.Page{Items: []domain.HazardEvent{}, Unavailable: []string{}}
	if q.Limit < 1 || q.Limit > 1000 || !s.Reads.Signer.Valid() {
		return out, canonical.Unavailable()
	}
	var result readResult
	err := s.validatedRead(ctx, func(ctx context.Context) error {
		spec := readSpec{Source: q.Source, Since: q.Since, Ingestion: true, Limit: q.Limit + 1}
		if q.After.ID != "" {
			spec.AfterTime = &q.After.Time
			spec.AfterID = q.After.ID
		}
		var err error
		result, err = s.readDistributed(ctx, spec)
		return err
	})
	if err != nil {
		return out, canonical.Unavailable()
	}
	out.Items = result.Events
	out.Unavailable = result.Unavailable
	out.Partial = len(out.Unavailable) > 0 || result.Missing
	pos := q.After
	pos.Endpoint = "audit/events"
	pos.Scope = q.Scope
	pos.Filters = q.Filters()
	pos.Units = requiredUnits(result.Inventory)
	more := len(out.Items) > q.Limit
	if more {
		out.Items = out.Items[:q.Limit]
	}
	if more || result.Missing {
		if len(out.Items) > 0 {
			last := out.Items[len(out.Items)-1]
			pos.Time = last.IngestedAt.Time
			pos.ID = last.HazardID
		}
		token, err := s.Reads.Signer.Encode(pos)
		if err != nil {
			return out, err
		}
		out.NextCursor = &token
	}
	return out, nil
}
func requiredUnits(inventory audit.Inventory) []string {
	v := []string{}
	for _, u := range inventory.Units {
		v = append(v, u.Name)
	}
	sort.Strings(v)
	return v
}

type referenceRows struct {
	rows                  pgx.Rows
	reference, source, id string
	valid                 bool
	group                 int
}

func (r *referenceRows) next() error {
	r.valid = r.rows.Next()
	if !r.valid {
		return r.rows.Err()
	}
	return r.rows.Scan(&r.reference, &r.source, &r.id)
}
func (s *Store) AuditChecksum(ctx context.Context, source string) (audit.Checksum, error) {
	out := audit.Checksum{Algorithm: "sha256", Unavailable: []string{}}
	if source != "" {
		out.Source = &source
	}
	err := s.validatedRead(ctx, func(ctx context.Context) error {
		ctx, span := platform.Start(ctx, "canonical live checksum")
		defer span.End()
		shards, topology, err := s.readCatalog(ctx)
		if err != nil {
			return err
		}
		inventory, err := s.readInventoryAt(ctx, topology)
		if err != nil || (inventory.Topology != "" && inventory.Topology != topology) {
			return canonical.Unavailable()
		}
		groups := physicalGroups(shards)
		out.Count = 0
		out.Partial = false
		out.Unavailable = inventory.Unavailable()
		cursors := []*referenceRows{}
		defer func() {
			for _, r := range cursors {
				r.rows.Close()
			}
		}()
		type checksumStream struct {
			rows *referenceRows
			unit string
		}
		streams := make(chan checksumStream, len(groups))
		started := 0
		for group, tables := range groups {
			unit, available := inventory.Primary(group)
			pool := s.Reads.Groups[group]
			if !available || pool == nil {
				out.Partial = true
				continue
			}
			started++
			go func() {
				parts := make([]string, 0, len(tables))
				for _, table := range tables {
					parts = append(parts, `SELECT source_ref_id,source,hazard_id FROM `+table+` WHERE active AND ($1='' OR source=$1)`)
				}
				query := `SELECT source_ref_id,source,hazard_id FROM (` + strings.Join(parts, " UNION ALL ") + `) logical_rows ORDER BY source_ref_id COLLATE "C",source COLLATE "C",hazard_id`
				rows, err := pool.Query(ctx, query, source)
				if err != nil {
					streams <- checksumStream{unit: unit.Name}
					return
				}
				r := &referenceRows{rows: rows, group: group}
				if err = r.next(); err != nil {
					rows.Close()
					streams <- checksumStream{unit: unit.Name}
					return
				}
				streams <- checksumStream{rows: r}
			}()
		}
		for range started {
			stream := <-streams
			if stream.rows == nil {
				out.Unavailable = append(out.Unavailable, stream.unit)
				out.Partial = true
				continue
			}
			cursors = append(cursors, stream.rows)
		}
		hash := sha256.New()
		for {
			var best *referenceRows
			for _, r := range cursors {
				if !r.valid {
					continue
				}
				if best == nil || r.reference < best.reference || r.reference == best.reference && (r.source < best.source || r.source == best.source && r.id < best.id) {
					best = r
				}
			}
			if best == nil {
				break
			}
			if out.Count > 0 {
				_, _ = hash.Write([]byte{'\n'})
			}
			_, _ = hash.Write([]byte(best.reference))
			out.Count++
			if err = best.next(); err != nil {
				return canonical.Unavailable()
			}
			if ctx.Err() != nil {
				return canonical.Unavailable()
			}
		}
		current, _, err := s.primaryTopology(ctx)
		if err != nil || current != topology {
			return canonical.Unavailable()
		}
		out.Checksum = hex.EncodeToString(hash.Sum(nil))
		out.AsOf = time.Now().UTC()
		out.Unavailable = mergeNames(out.Unavailable, nil)
		out.Partial = out.Partial || len(out.Unavailable) > 0
		return nil
	})
	if err != nil {
		return audit.Checksum{}, canonical.Unavailable()
	}
	return out, nil
}

func (s *Store) AuditLocation(ctx context.Context, id string) (audit.Locations, error) {
	out := audit.Locations{ID: id, Locations: []audit.Location{}, Unavailable: []string{}}
	err := s.validatedRead(ctx, func(ctx context.Context) error {
		out.Locations = []audit.Location{}
		result, err := s.readDistributed(ctx, readSpec{ID: id, Ingestion: true, Limit: 2})
		if err != nil {
			return err
		}
		out.Partial = len(result.Unavailable) > 0 || result.Missing
		out.Unavailable = result.Unavailable
		if len(result.Events) == 0 {
			if result.Missing {
				// Existence is unknown, not absent. The prescribed location shape
				// can truthfully return no verified locations with partial metadata.
				return nil
			}
			return canonical.Fail(404, "not_found", "hazard does not exist")
		}
		if len(result.Events) != 1 {
			return canonical.Unavailable()
		}
		e := result.Events[0]
		route, _ := canonical.Route(string(e.Source), e.SourceRefID)
		group := 0
		if len(result.Shards) > 0 && result.Shards[0].ID != 0 {
			var shard int64
			if err = s.Pool.QueryRow(ctx, `SELECT get_shard_id_for_distribution_column('hazard_events',$1::text)`, route).Scan(&shard); err != nil {
				return err
			}
			found := false
			for _, p := range result.Shards {
				if p.ID == shard {
					group = p.Group
					found = true
					break
				}
			}
			if !found {
				return canonical.Unavailable()
			}
		}
		for _, u := range result.Inventory.Units {
			if u.Group != group {
				continue
			}
			if !u.RoleKnown || u.Node == "" {
				out.Partial = true
				out.Unavailable = mergeNames(out.Unavailable, []string{u.Name})
				continue
			}
			role := "replica"
			if u.Primary {
				role = "primary"
			}
			out.Locations = append(out.Locations, audit.Location{Node: u.Node, Unit: u.Name, Role: role})
		}
		sort.Slice(out.Locations, func(i, j int) bool { return out.Locations[i].Unit < out.Locations[j].Unit })
		return nil
	})
	if err != nil {
		var p *canonical.Error
		if errors.As(err, &p) {
			return out, p
		}
		return out, canonical.Unavailable()
	}
	return out, nil
}
