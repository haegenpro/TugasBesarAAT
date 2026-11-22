package store

import (
	"aggregator/internal/domain"
	"aggregator/internal/timeutil"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"sort"
	"strings"
	"time"
)

var ErrOrder = errors.New("canonical batch is out of order or reuses an identity")

type CanonicalChange struct {
	Ref    string              `json:"source_ref_id"`
	Event  *domain.HazardEvent `json:"event"`
	Delete bool                `json:"delete"`
}
type CanonicalBatch struct {
	ID        string            `json:"operation_id"`
	Source    string            `json:"source"`
	Namespace string            `json:"namespace"`
	Previous  int64             `json:"previous_position"`
	Position  int64             `json:"position"`
	Changes   []CanonicalChange `json:"changes"`
}

// MaxCanonicalSequence bounds how many original operations one owner call may apply.
const MaxCanonicalSequence = 200

func (s *Store) ApplyCanonical(ctx context.Context, b CanonicalBatch, cid string) error {
	return s.ApplyCanonicalSequence(ctx, []CanonicalBatch{b}, cid)
}

// ApplyCanonicalSequence applies consecutive original operations of one namespace, in predecessor
// order, in one owner transaction. Each operation keeps its own identity, digest, predecessor check
// and idempotency record exactly as if it had been sent alone; only the locks, the visibility mark
// and the commit are shared. An operation already applied with the same digest is skipped (a retry
// after a lost response); any rejected operation rejects the whole call, and nothing is committed.
func (s *Store) ApplyCanonicalSequence(ctx context.Context, batches []CanonicalBatch, cid string) error {
	if len(batches) == 0 || len(batches) > MaxCanonicalSequence {
		return ErrOrder
	}
	hashes := make([]string, len(batches))
	for k, b := range batches {
		if b.ID == "" || b.Namespace == "" || len(b.Namespace) > 256 || b.Position <= b.Previous || len(b.Changes) > 1000 || (b.Source != "SATELIT" && b.Source != "SENSOR_PANTAI") {
			return ErrOrder
		}
		if b.Source != batches[0].Source || b.Namespace != batches[0].Namespace {
			return ErrOrder
		}
		raw, e := json.Marshal(b)
		if e != nil {
			return e
		}
		digest := sha256.Sum256(raw)
		hashes[k] = hex.EncodeToString(digest[:])
	}
	tx, e := s.beginCanonicalTx(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, visibilityLock); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,7))`, batches[0].Source+":"+batches[0].Namespace); e != nil {
		return e
	}
	for k, b := range batches {
		if e = s.applyCanonicalLocked(ctx, tx, b, hashes[k], cid); e != nil {
			return e
		}
	}
	return tx.Commit(ctx)
}

// applyCanonicalLocked is one operation's state transition; the caller holds the locks.
func (s *Store) applyCanonicalLocked(ctx context.Context, tx pgx.Tx, b CanonicalBatch, hash, cid string) error {
	var old string
	e := tx.QueryRow(ctx, `SELECT digest FROM canonical_batches WHERE operation_id=$1`, b.ID).Scan(&old)
	if e == nil {
		if old != hash {
			return ErrOrder
		}
		return nil
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO canonical_progress(source,namespace,position) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, b.Source, b.Namespace, b.Previous); e != nil {
		return e
	}
	var pos int64
	if e = tx.QueryRow(ctx, `SELECT position FROM canonical_progress WHERE source=$1 AND namespace=$2 FOR UPDATE`, b.Source, b.Namespace).Scan(&pos); e != nil {
		return e
	}
	if pos != b.Previous {
		return ErrOrder
	}
	seen := map[string]bool{}
	if len(b.Changes) > 0 {
		if e = markVisibility(ctx, tx); e != nil {
			return e
		}
	}
	for _, change := range b.Changes {
		if change.Ref == "" || len(change.Ref) > 1024 || seen[change.Ref] {
			return ErrOrder
		}
		seen[change.Ref] = true
		if change.Delete {
			var id string
			var revision int64
			e = tx.QueryRow(ctx, `UPDATE hazard_events SET active=false,revision=revision+1 WHERE source=$1 AND source_ref_id=$2 AND active RETURNING hazard_id::text,revision`, b.Source, change.Ref).Scan(&id, &revision)
			if errors.Is(e, pgx.ErrNoRows) {
				continue
			}
			if e != nil {
				return e
			}
			if e = queueLifecycle(ctx, tx, domain.LifecycleEvent{EventType: "hazard_retracted", RecordID: id, Revision: revision, HazardRevision: revision, CorrelationID: cid}); e != nil {
				return e
			}
		} else {
			if change.Event == nil {
				return ErrOrder
			}
			h := *change.Event
			if (b.Source == "SATELIT" && h.HazardType != domain.HazardWildfire) || (b.Source == "SENSOR_PANTAI" && h.HazardType != domain.HazardCoastal) {
				return ErrOrder
			}
			if h.HazardType == domain.HazardWildfire && domain.ValidateWildfire(h.Attributes) != nil {
				return ErrOrder
			}
			if string(h.Source) != b.Source || h.SourceRefID != change.Ref || !domain.ValidHazardType(string(h.HazardType)) || h.HazardType == "" || !domain.ValidSeverity(string(h.Severity)) || h.Severity == "" || h.AreaName == "" || h.Attributes == nil || h.OccurredAt.IsZero() || h.Latitude < -90 || h.Latitude > 90 || h.Longitude < -180 || h.Longitude > 180 {
				return ErrOrder
			}
			h.HazardID = uuid.NewSHA1(uuid.NameSpaceOID, []byte(b.Source+"\x00"+change.Ref)).String()
			h.IngestedAt = timeutil.New(time.Now().UTC())
			if _, _, e = s.saveHazardTx(ctx, tx, h, cid, false); e != nil {
				return e
			}
			if _, e = tx.Exec(ctx, `UPDATE hazard_events SET active=true WHERE source=$1 AND source_ref_id=$2`, b.Source, change.Ref); e != nil {
				return e
			}
		}
	}
	if _, e = tx.Exec(ctx, `INSERT INTO canonical_batches(operation_id,digest,source,namespace,position) VALUES($1,$2,$3,$4,$5)`, b.ID, hash, b.Source, b.Namespace, b.Position); e != nil {
		return e
	}
	_, e = tx.Exec(ctx, `UPDATE canonical_progress SET position=$3 WHERE source=$1 AND namespace=$2`, b.Source, b.Namespace, b.Position)
	return e
}

type AuditPage struct {
	Items       []domain.HazardEvent `json:"items"`
	Next        *string              `json:"next_cursor"`
	Partial     bool                 `json:"partial"`
	Unavailable []string             `json:"unavailable_units"`
}
type AuditQuery struct {
	Source string
	Since  time.Time
	Limit  int
	Cursor string
}
type auditPosition struct {
	Source string
	Since  time.Time
	At     time.Time
	ID     string
}

func (s *Store) AuditEvents(ctx context.Context, q AuditQuery) (AuditPage, error) {
	out := AuditPage{Items: []domain.HazardEvent{}, Unavailable: []string{}}
	if q.Limit < 1 || q.Limit > 1000 {
		return out, ErrCursor
	}
	var after auditPosition
	if q.Cursor != "" {
		raw, e := base64.RawURLEncoding.DecodeString(q.Cursor)
		if len(q.Cursor) > 2048 || e != nil || json.Unmarshal(raw, &after) != nil || after.Source != q.Source || !after.Since.Equal(q.Since) || after.At.IsZero() {
			return out, ErrCursor
		}
		if _, e = uuid.Parse(after.ID); e != nil {
			return out, ErrCursor
		}
	}
	query := `SELECT ` + selectColumns + ` FROM hazard_events WHERE active AND ($1='' OR source=$1) AND ingested_at >= $2 AND ($3::timestamptz IS NULL OR (ingested_at,hazard_id)>($3,$4::uuid)) ORDER BY ingested_at,hazard_id LIMIT $5`
	var at any
	id := uuid.Nil.String()
	if !after.At.IsZero() {
		at = after.At
		id = after.ID
	}
	tx, e := s.readBarrier(ctx)
	if e != nil {
		return s.partialAudit(ctx, out), nil
	}
	defer tx.Rollback(ctx)
	rows, e := tx.Query(ctx, query, q.Source, q.Since, at, id, q.Limit+1)
	if e != nil {
		tx.Rollback(ctx)
		return s.partialAudit(ctx, out), nil
	}
	defer rows.Close()
	for rows.Next() {
		h, e := scanRow(rows)
		if e != nil {
			return out, e
		}
		out.Items = append(out.Items, h)
	}
	if rows.Err() != nil {
		tx.Rollback(ctx)
		return s.partialAudit(ctx, AuditPage{Items: []domain.HazardEvent{}}), nil
	}
	if len(out.Items) > q.Limit {
		out.Items = out.Items[:q.Limit]
		h := out.Items[q.Limit-1]
		b, _ := json.Marshal(auditPosition{q.Source, q.Since, h.IngestedAt.Time, h.HazardID})
		v := base64.RawURLEncoding.EncodeToString(b)
		out.Next = &v
	}
	return out, nil
}
func (s *Store) partialAudit(ctx context.Context, out AuditPage) AuditPage {
	out.Partial = true
	out.Next = nil
	out.Unavailable = s.UnavailableUnits(ctx)
	if len(out.Unavailable) == 0 {
		out.Unavailable = []string{"canonical-visibility"}
	}
	return out
}
func (s *Store) AuditChecksum(ctx context.Context, source string) (map[string]any, error) {
	hash := sha256.New()
	var count int64
	tx, e := s.readBarrier(ctx)
	var rows pgx.Rows
	if e == nil {
		defer tx.Rollback(ctx)
		rows, e = tx.Query(ctx, `SELECT source_ref_id FROM hazard_events WHERE active AND ($1='' OR source=$1) ORDER BY source_ref_id COLLATE "C"`, source)
	}
	unavailable := []string{}
	if e == nil {
		for rows.Next() {
			var ref string
			if e = rows.Scan(&ref); e != nil {
				break
			}
			if count > 0 {
				hash.Write([]byte{'\n'})
			}
			hash.Write([]byte(ref))
			count++
		}
		if e == nil {
			e = rows.Err()
		}
		rows.Close()
	}
	partial := e != nil
	if partial {
		if tx != nil {
			tx.Rollback(ctx)
		}
		count = 0
		hash.Reset()
		unavailable = s.UnavailableUnits(ctx)
		if len(unavailable) == 0 {
			unavailable = []string{"canonical-visibility"}
		}
	}
	var selected any
	if source != "" {
		selected = source
	}
	return map[string]any{"source": selected, "count": count, "checksum": hex.EncodeToString(hash.Sum(nil)), "algorithm": "sha256", "as_of": time.Now().UTC(), "partial": partial, "unavailable_units": unavailable}, nil
}

type storageMember struct {
	Group int
	Host  string
	Port  int
	Role  string
}

type rowQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func (s *Store) members(ctx context.Context) ([]storageMember, error) {
	return s.membersFrom(ctx, s.Pool)
}
func (s *Store) membersFrom(ctx context.Context, queryer rowQuerier) ([]storageMember, error) {
	rows, e := queryer.Query(ctx, `SELECT groupid,nodename,nodeport,noderole::text FROM pg_dist_node WHERE groupid>0 AND isactive ORDER BY groupid,noderole,nodename`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []storageMember
	for rows.Next() {
		var m storageMember
		if e = rows.Scan(&m.Group, &m.Host, &m.Port, &m.Role); e != nil {
			return nil, e
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
func (s *Store) memberConnection(ctx context.Context, m storageMember) (*pgx.Conn, error) {
	c := s.Pool.Config().ConnConfig.Copy()
	c.Host = m.Host
	c.Port = uint16(m.Port)
	c.ConnectTimeout = 500 * time.Millisecond
	c.Tracer = nil
	c.RuntimeParams["statement_timeout"] = "1000"
	return pgx.ConnectConfig(ctx, c)
}
func unit(m storageMember) string {
	node := "a"
	if strings.Contains(m.Host, "-b-0.") {
		node = "b"
	}
	return fmt.Sprintf("canonical-%d-%s", m.Group, node)
}
func (s *Store) UnavailableUnits(ctx context.Context) []string {
	members, e := s.members(ctx)
	if e != nil {
		return []string{"canonical-coordinator"}
	}
	results := make(chan string, len(members))
	for _, m := range members {
		go func(m storageMember) {
			c, cancel := context.WithTimeout(ctx, 1200*time.Millisecond)
			defer cancel()
			conn, e := s.memberConnection(c, m)
			if e == nil {
				e = conn.Ping(c)
				conn.Close(c)
			}
			if e != nil {
				results <- unit(m)
			} else {
				results <- ""
			}
		}(m)
	}
	out := []string{}
	for range members {
		if name := <-results; name != "" {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}
func (s *Store) AuditLocation(ctx context.Context, id, nodeA, nodeB string) (map[string]any, error) {
	tx, e := s.readBarrier(ctx)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback(ctx)
	h, e := scanRow(tx.QueryRow(ctx, `SELECT `+selectColumns+` FROM hazard_events WHERE hazard_id=$1 AND active`, id))
	if errors.Is(e, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if e != nil {
		return nil, e
	}
	var shard int64
	var group int
	if e = tx.QueryRow(ctx, `SELECT get_shard_id_for_distribution_column('hazard_events',$1::text)`, h.SourceRefID).Scan(&shard); e != nil {
		return nil, e
	}
	if e = tx.QueryRow(ctx, `SELECT groupid FROM pg_dist_placement WHERE shardid=$1 AND shardstate=1 LIMIT 1`, shard).Scan(&group); e != nil {
		return nil, e
	}
	members, e := s.membersFrom(ctx, tx)
	if e != nil {
		return nil, e
	}
	locations := []map[string]string{}
	unavailable := []string{}
	for _, m := range members {
		if m.Group != group {
			continue
		}
		c, cancel := context.WithTimeout(ctx, 1200*time.Millisecond)
		conn, e := s.memberConnection(c, m)
		var present, recovery bool
		if e == nil {
			e = conn.QueryRow(c, `SELECT pg_is_in_recovery(),EXISTS(SELECT 1 FROM `+pgx.Identifier{fmt.Sprintf("hazard_events_%d", shard)}.Sanitize()+` WHERE source_ref_id=$1 AND hazard_id=$2 AND active)`, h.SourceRefID, id).Scan(&recovery, &present)
			conn.Close(c)
		}
		cancel()
		if e != nil {
			unavailable = append(unavailable, unit(m))
			continue
		}
		if present {
			node := nodeA
			if strings.Contains(m.Host, "-b-0.") {
				node = nodeB
			}
			role := "primary"
			if recovery {
				role = "replica"
			}
			locations = append(locations, map[string]string{"node": node, "unit": unit(m), "role": role})
		}
	}
	return map[string]any{"hazard_id": id, "locations": locations, "partial": len(unavailable) > 0, "unavailable_units": unavailable}, nil
}

func (s *Store) Unprocessed(ctx context.Context, source string, limit int, token string) (map[string]any, error) {
	var p auditPosition
	if token != "" {
		b, e := base64.RawURLEncoding.DecodeString(token)
		if len(token) > 2048 || e != nil || json.Unmarshal(b, &p) != nil || p.Source != source || p.At.IsZero() {
			return nil, ErrCursor
		}
		if _, e = uuid.Parse(p.ID); e != nil {
			return nil, ErrCursor
		}
	}
	var at any
	id := uuid.Nil.String()
	if !p.At.IsZero() {
		at = p.At
		id = p.ID
	}
	rows, e := s.Pool.Query(ctx, `SELECT message_id::text,source,received_at,reason,retryable,attempts,status FROM failed_ingestions WHERE ($1='' OR source=$1) AND ($2::timestamptz IS NULL OR (received_at,message_id)>($2,$3::uuid)) ORDER BY received_at,message_id LIMIT $4`, source, at, id, limit+1)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	items := []map[string]any{}
	for rows.Next() {
		var message, src, reason, status string
		var received time.Time
		var retry bool
		var attempts int64
		if e = rows.Scan(&message, &src, &received, &reason, &retry, &attempts, &status); e != nil {
			return nil, e
		}
		items = append(items, map[string]any{"message_id": message, "source": src, "received_at": received, "reason": reason, "retryable": retry, "attempts": attempts, "status": status})
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	var next any
	if len(items) > limit {
		items = items[:limit]
		last := items[limit-1]
		b, _ := json.Marshal(auditPosition{Source: source, At: last["received_at"].(time.Time), ID: last["message_id"].(string)})
		next = base64.RawURLEncoding.EncodeToString(b)
	}
	return map[string]any{"items": items, "next_cursor": next}, nil
}

func (s *Store) Analytics(ctx context.Context, from, to time.Time, interval, source string) (map[string]any, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	types := map[string]int64{"SEISMIC": 0, "VOLCANIC": 0, "WILDFIRE": 0, "COASTAL": 0}
	severity := map[string]int64{"NORMAL": 0, "WASPADA": 0, "SIAGA": 0, "AWAS": 0}
	areas := map[string]int64{}
	type bucket struct {
		At      time.Time        `json:"at"`
		Counts  map[string]int64 `json:"counts"`
		Tsunami int64            `json:"tsunami_warning_overlay"`
	}
	buckets := map[time.Time]*bucket{}
	var total, tsunami int64
	query := `SELECT hazard_type,severity,area_name,date_trunc($1,occurred_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC',count(*),count(*) FILTER(WHERE source='BMKG' AND hazard_type='SEISMIC' AND attributes ? 'warning_id') FROM hazard_events WHERE active AND occurred_at >= $2 AND occurred_at < $3 AND ($4='' OR source=$4) GROUP BY 1,2,3,4 ORDER BY 4,1,2,3`
	tx, e := s.readBarrier(ctx)
	var rows pgx.Rows
	if e == nil {
		rows, e = tx.Query(ctx, query, interval, from, to, source)
	}
	complete := e == nil
	if e == nil {
		for rows.Next() {
			var typ, level, area string
			var at time.Time
			var n, t int64
			if e = rows.Scan(&typ, &level, &area, &at, &n, &t); e != nil {
				break
			}
			total += n
			tsunami += t
			types[typ] += n
			severity[level] += n
			areas[area] += n
			if buckets[at] == nil {
				buckets[at] = &bucket{At: at, Counts: map[string]int64{"SEISMIC": 0, "VOLCANIC": 0, "WILDFIRE": 0, "COASTAL": 0}}
			}
			buckets[at].Counts[typ] += n
			buckets[at].Tsunami += t
		}
		if e == nil {
			e = rows.Err()
		}
		rows.Close()
		complete = e == nil
	}
	if !complete {
		total = 0
		tsunami = 0
		for k := range types {
			types[k] = 0
		}
		for k := range severity {
			severity[k] = 0
		}
		areas = map[string]int64{}
		buckets = map[time.Time]*bucket{}
	}
	series := []*bucket{}
	for _, v := range buckets {
		series = append(series, v)
	}
	sort.Slice(series, func(i, j int) bool { return series[i].At.Before(series[j].At) })
	areaList := []map[string]any{}
	for k, n := range areas {
		areaList = append(areaList, map[string]any{"area_name": k, "count": n})
	}
	sort.Slice(areaList, func(i, j int) bool { return areaList[i]["area_name"].(string) < areaList[j]["area_name"].(string) })
	if tx != nil {
		tx.Rollback(ctx)
	}
	unavailable := s.UnavailableUnits(ctx)
	if !complete && len(unavailable) == 0 {
		unavailable = []string{"canonical-visibility"}
	}
	return map[string]any{"filters": map[string]any{"from": from.UTC(), "to": to.UTC(), "interval": interval, "source": source}, "total": total, "by_type": types, "by_severity": severity, "by_area": areaList, "series": series, "tsunami_warning_overlay": tsunami, "as_of": time.Now().UTC(), "mode": "live", "generation": 0, "data_complete": complete, "partial": !complete || len(unavailable) > 0, "unavailable_units": unavailable, "source_freshness": map[string]any{}}, nil
}

func (s *Store) GetSchema(ctx context.Context, id int) (string, error) {
	var text string
	e := s.Pool.QueryRow(ctx, `SELECT schema FROM schema_cache WHERE id=$1`, id).Scan(&text)
	return text, e
}
func (s *Store) PutSchema(ctx context.Context, id int, text string) error {
	r, e := s.Pool.Exec(ctx, `INSERT INTO schema_cache(id,schema) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET schema=excluded.schema WHERE schema_cache.schema=excluded.schema`, id, text)
	if e == nil && r.RowsAffected() != 1 {
		return ErrOrder
	}
	return e
}
func (s *Store) FindSchema(ctx context.Context, text string) (int, error) {
	var id int
	e := s.Pool.QueryRow(ctx, `SELECT id FROM schema_cache WHERE schema=$1`, text).Scan(&id)
	return id, e
}

type schemaCache struct{ store *Store }

func (s *Store) SchemaCache() schemaCache { return schemaCache{s} }
func (c schemaCache) Get(ctx context.Context, id int) (string, error) {
	return c.store.GetSchema(ctx, id)
}
func (c schemaCache) Put(ctx context.Context, id int, text string) error {
	return c.store.PutSchema(ctx, id, text)
}
func (c schemaCache) Find(ctx context.Context, text string) (int, error) {
	return c.store.FindSchema(ctx, text)
}
