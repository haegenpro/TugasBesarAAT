package wildfireprocessor

import (
	"aggregator/internal/canonical"
	"aggregator/internal/middleware"
	"aggregator/internal/platform"
	"aggregator/internal/rawinput"
	"aggregator/internal/store"
	"aggregator/internal/wildfire"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"net/http"
	"sort"
	"time"
)

type Engine struct {
	Store       *store.Store
	Config      Config
	provinces   []wildfire.Province
	fingerprint string
	// Fault is an in-process synchronized test hook, never an environment option.
	Fault            func(context.Context, string) error
	transactionFault func(context.Context, pgx.Tx) error
}
type Pending struct{ Reason string }

func (p *Pending) Error() string { return "wildfire pending: " + p.Reason }

type checkpoint struct {
	Old, New                 string
	Targets                  []string
	Target                   int
	Cursor, Root, Node, Kind string
	Hash                     []byte
	Verified                 int64
}
type job struct {
	Position           int64
	Phase, Topology    string
	CP                 checkpoint
	Records, Bytes     int64
	Late               bool
	Clock, Watermark   *time.Time
	OutputCount        int64
	Digest, NodeDigest *string
}
type record struct {
	Key     string
	Payload []byte
	At      *time.Time
	Active  bool
	Ref     string
}
type queryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func New(s *store.Store, c Config) (*Engine, error) {
	p, fp, err := c.validate()
	if err != nil {
		return nil, err
	}
	// Detach all caller-owned configuration memory.
	c.ProvinceJSON = append([]byte(nil), c.ProvinceJSON...)
	start, late := *c.InitialOffset, *c.AllowedLatenessSeconds
	c.InitialOffset = &start
	c.AllowedLatenessSeconds = &late
	return &Engine{Store: s, Config: c, provinces: p, fingerprint: fp}, nil
}
func (e *Engine) Check(ctx context.Context) error {
	for _, sql := range []string{`SELECT config_sha256,prepared_through,applied_through FROM wildfire_runs LIMIT 0`, `SELECT phase,checkpoint,state_sha256,sealed FROM wildfire_jobs LIMIT 0`, `SELECT route_key,payload,sha256 FROM wildfire_state LIMIT 0`, `SELECT node_key,component,done FROM wildfire_work LIMIT 0`, `SELECT ordinal,payload,sha256 FROM wildfire_output_chunks LIMIT 0`} {
		if _, err := e.Store.Pool.Exec(ctx, sql); err != nil {
			return err
		}
	}
	var stored string
	err := e.Store.Pool.QueryRow(ctx, `SELECT config_sha256 FROM wildfire_runs WHERE namespace=$1`, e.Config.Namespace).Scan(&stored)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if err == nil && stored != e.fingerprint {
		return fmt.Errorf("immutable run configuration mismatch")
	}
	return nil
}
func (e *Engine) begin(ctx context.Context, owner canonical.Ownership) (pgx.Tx, error) {
	if owner.Scope != "processor:SATELIT" {
		return nil, canonical.Fail(403, "stale_owner", "satellite processor authority required")
	}
	tx, err := e.Store.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if err = e.Store.ProtectProcessorState(ctx, tx, owner); err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}
func (e *Engine) one(ctx context.Context, q queryer, kind, key string, pos int64) (record, error) {
	var r record
	r.Key = key
	var hash string
	err := q.QueryRow(ctx, `SELECT payload,sha256,event_at,active,reference FROM wildfire_state WHERE route_key=wildfire_route($1,$2,$3) AND namespace=$1 AND kind=$2 AND entity_key=$3 AND position<=$4 ORDER BY position DESC LIMIT 1`, e.Config.Namespace, kind, key, pos).Scan(&r.Payload, &hash, &r.At, &r.Active, &r.Ref)
	if err == nil && rawinput.Hash(r.Payload) != hash {
		return r, fmt.Errorf("state byte integrity failed")
	}
	return r, err
}
func (e *Engine) page(ctx context.Context, q queryer, kind string, pos int64, after string) ([]record, error) {
	rows, err := q.Query(ctx, `SELECT entity_key,payload,sha256,event_at,active,reference FROM (SELECT DISTINCT ON(route_key,namespace,kind,entity_key) route_key,namespace,kind,entity_key,payload,sha256,event_at,active,reference FROM wildfire_state WHERE namespace=$1 AND kind=$2 AND position<=$3 AND entity_key COLLATE "C">$4 COLLATE "C" ORDER BY route_key,namespace,kind,entity_key,position DESC) v ORDER BY entity_key COLLATE "C" LIMIT $5`, e.Config.Namespace, kind, pos, after, e.Config.PageRows)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []record{}
	var bytes int64
	for rows.Next() {
		var r record
		var hash string
		if err = rows.Scan(&r.Key, &r.Payload, &hash, &r.At, &r.Active, &r.Ref); err != nil {
			return nil, err
		}
		bytes += int64(len(r.Payload) + len(r.Key) + len(r.Ref) + 256)
		if bytes > e.Config.CacheBytes/2 {
			return nil, &Pending{"page cache quota"}
		}
		if rawinput.Hash(r.Payload) != hash {
			return nil, fmt.Errorf("state integrity failed")
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
func (e *Engine) put(ctx context.Context, tx pgx.Tx, j *job, kind, key string, value any, at *time.Time, active bool, ref string) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	size := int64(len(b) + len(key) + len(ref) + 256)
	if size > e.Config.CacheBytes/2 || j.Bytes+size > e.Config.StagingBytes {
		return &Pending{"staging or record cache quota"}
	}
	sum := rawinput.Hash(b)
	var old string
	tag, err := tx.Exec(ctx, `INSERT INTO wildfire_state(route_key,namespace,kind,entity_key,position,payload,sha256,event_at,active,reference) VALUES(wildfire_route($1,$2,$3),$1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING`, e.Config.Namespace, kind, key, j.Position, b, sum, at, active, ref)
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		j.Records++
		j.Bytes += size
		return nil
	}
	err = tx.QueryRow(ctx, `SELECT sha256 FROM wildfire_state WHERE route_key=wildfire_route($1,$2,$3) AND namespace=$1 AND kind=$2 AND entity_key=$3 AND position=$4`, e.Config.Namespace, kind, key, j.Position).Scan(&old)
	if err == nil && old != sum {
		return fmt.Errorf("immutable staged identity conflict")
	}
	return err
}
func (e *Engine) enqueue(ctx context.Context, tx pgx.Tx, j *job, key, component string) error {
	size := int64(len(key)*2 + 256)
	if j.Bytes+size > e.Config.StagingBytes {
		return &Pending{"work queue quota"}
	}
	tag, err := tx.Exec(ctx, `INSERT INTO wildfire_work(route_key,namespace,position,node_key,component) VALUES(wildfire_route($1,'work',$3),$1,$2,$3,$4) ON CONFLICT DO NOTHING`, e.Config.Namespace, j.Position, key, component)
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		j.Bytes += size
	}
	if component != "" {
		_, err = tx.Exec(ctx, `UPDATE wildfire_work SET component=$4 WHERE route_key=wildfire_route($1,'work',$3) AND namespace=$1 AND position=$2 AND node_key=$3 AND component=''`, e.Config.Namespace, j.Position, key, component)
	}
	return err
}
func (e *Engine) input(ctx context.Context, q queryer, pos int64) (rawinput.Envelope, wildfire.Operation, error) {
	var b, payload []byte
	var env rawinput.Envelope
	err := q.QueryRow(ctx, `SELECT envelope,original_payload FROM input_history WHERE route_key=input_route('SATELIT',$1,$2) AND source='SATELIT' AND namespace=$1 AND position=$2`, e.Config.Namespace, pos).Scan(&b, &payload)
	if err != nil {
		return env, wildfire.Operation{}, err
	}
	if int64(len(b)+len(payload)) > e.Config.CacheBytes/2 {
		return env, wildfire.Operation{}, &Pending{"input cache quota"}
	}
	if err = json.Unmarshal(b, &env); err != nil {
		return env, wildfire.Operation{}, err
	}
	op, err := DecodeOperation(env, payload, e.Config.CacheBytes/4)
	return env, op, err
}
func (e *Engine) observation(ctx context.Context, q queryer, key string, pos int64) (wildfire.Observation, error) {
	r, err := e.one(ctx, q, "observation", key, pos)
	var o wildfire.Observation
	if err == nil {
		err = json.Unmarshal(r.Payload, &o)
	}
	return o, err
}
func eligible(o wildfire.Observation) bool {
	return o.Active && o.Detection != nil && o.Detection.Confidence != "l"
}
func (e *Engine) hook(ctx context.Context, name string) error {
	if e.Fault != nil {
		return e.Fault(ctx, name)
	}
	return nil
}

// Step commits at most one bounded work page. The source operation stays
// unprepared until activation; raw receipt never waits for this work loop.
func (e *Engine) Step(ctx context.Context, owner canonical.Ownership) (worked bool, resultErr error) {
	ctx, cancel := context.WithTimeout(ctx, canonical.Deadline)
	defer cancel()
	defer func() {
		if resultErr != nil && ctx.Err() == nil {
			e.recordPending(ctx, owner, resultErr.Error())
		}
	}()
	topology, err := e.Store.ProcessorStorageBoundary(ctx, "")
	if err != nil {
		return false, err
	}
	tx, err := e.begin(ctx, owner)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var received, prepared, initial int64
	err = tx.QueryRow(ctx, `SELECT initial_position,received_through,prepared_through FROM input_sessions WHERE source='SATELIT' AND namespace=$1`, e.Config.Namespace).Scan(&initial, &received, &prepared)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if initial != *e.Config.InitialOffset {
		return false, fmt.Errorf("history initial offset mismatch")
	}
	_, err = tx.Exec(ctx, `INSERT INTO wildfire_runs(namespace,config_sha256,prepared_through,applied_through) VALUES($1,$2,$3,$3) ON CONFLICT DO NOTHING`, e.Config.Namespace, e.fingerprint, initial-1)
	if err != nil {
		return false, err
	}
	var stored string
	var runPrepared int64
	var clock, watermark *time.Time
	err = tx.QueryRow(ctx, `SELECT config_sha256,prepared_through,virtual_now,watermark FROM wildfire_runs WHERE namespace=$1 FOR UPDATE`, e.Config.Namespace).Scan(&stored, &runPrepared, &clock, &watermark)
	if err != nil {
		return false, err
	}
	if stored != e.fingerprint {
		return false, fmt.Errorf("immutable run configuration mismatch")
	}
	if runPrepared != prepared {
		return false, canonical.Unavailable()
	}
	if prepared >= received {
		return false, nil
	}
	j := job{Position: prepared + 1, Phase: "start", Topology: topology, Clock: clock, Watermark: watermark}
	_, err = tx.Exec(ctx, `INSERT INTO wildfire_jobs(namespace,position,phase,topology,batch_id) VALUES($1,$2,'start',$3,$4) ON CONFLICT DO NOTHING`, e.Config.Namespace, j.Position, topology, wildfire.BatchIdentity(e.Config.Namespace, j.Position))
	if err != nil {
		return false, err
	}
	var cp []byte
	err = tx.QueryRow(ctx, `SELECT phase,checkpoint,topology,record_count,staged_bytes,late,next_clock,next_watermark,output_count,state_sha256,node_sha256 FROM wildfire_jobs WHERE namespace=$1 AND position=$2 FOR UPDATE`, e.Config.Namespace, j.Position).Scan(&j.Phase, &cp, &j.Topology, &j.Records, &j.Bytes, &j.Late, &j.Clock, &j.Watermark, &j.OutputCount, &j.Digest, &j.NodeDigest)
	if err != nil {
		return false, err
	}
	if err = json.Unmarshal(cp, &j.CP); err != nil {
		return false, err
	}
	if j.Topology != topology {
		return false, canonical.Unavailable()
	}
	envelope, _, err := e.input(ctx, tx, j.Position)
	if err != nil {
		return false, err
	}
	ctx = middleware.WithCorrelationID(ctx, envelope.CorrelationID)
	if envelope.Traceparent != nil {
		ctx = platform.Extract(ctx, http.Header{"Traceparent": []string{*envelope.Traceparent}})
	}
	ctx, span := platform.Start(ctx, "wildfire preparation step")
	defer span.End()
	if j.Phase == "start" {
		j.Clock, j.Watermark = clock, watermark
	}
	switch j.Phase {
	case "start":
		err = e.start(ctx, tx, &j)
	case "aliases":
		err = e.aliases(ctx, tx, &j)
	case "edges":
		err = e.edges(ctx, tx, &j)
	case "graph":
		err = e.graph(ctx, tx, &j)
	case "resolve":
		err = e.resolve(ctx, tx, &j)
	case "verify", "verify_nodes":
		err = e.verify(ctx, tx, &j)
	case "activate":
		err = e.activate(ctx, tx, &j)
	default:
		err = fmt.Errorf("unknown or inconsistent preparation phase %s", j.Phase)
	}
	if err != nil {
		return false, err
	}
	cp, err = json.Marshal(j.CP)
	if err != nil {
		return false, err
	}
	_, err = tx.Exec(ctx, `UPDATE wildfire_jobs SET phase=$3,checkpoint=$4,record_count=$5,staged_bytes=$6,late=$7,next_clock=$8,next_watermark=$9,output_count=$10,state_sha256=$11,node_sha256=$12,blocked_reason=NULL WHERE namespace=$1 AND position=$2`, e.Config.Namespace, j.Position, j.Phase, cp, j.Records, j.Bytes, j.Late, j.Clock, j.Watermark, j.OutputCount, j.Digest, j.NodeDigest)
	if err != nil {
		return false, err
	}
	if err = e.hook(ctx, "before_preparation_commit"); err != nil {
		return false, err
	}
	if e.transactionFault != nil {
		if err = e.transactionFault(ctx, tx); err != nil {
			return false, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return false, canonical.CommitError(err)
	}
	if err = e.hook(ctx, "after_preparation_commit"); err != nil {
		return false, err
	}
	err = e.Store.ConfirmProcessorRecord(ctx, `SELECT EXISTS(SELECT 1 FROM wildfire_jobs WHERE namespace=$1 AND position=$2 AND phase=$3)`, e.Config.Namespace, j.Position, j.Phase)
	platform.Gauge("wildfire_staging_bytes", float64(j.Bytes))
	if j.Phase == "active" {
		platform.Gauge("wildfire_prepared_through", float64(j.Position))
	}
	return true, err
}

func (e *Engine) recordPending(ctx context.Context, owner canonical.Ownership, reason string) {
	topology, err := e.Store.ProcessorStorageBoundary(ctx, "")
	if err != nil {
		return
	}
	tx, err := e.begin(ctx, owner)
	if err != nil {
		return
	}
	defer tx.Rollback(ctx)
	var fingerprint string
	err = tx.QueryRow(ctx, `SELECT config_sha256 FROM wildfire_runs WHERE namespace=$1`, e.Config.Namespace).Scan(&fingerprint)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return
	}
	if err == nil && fingerprint != e.fingerprint {
		return
	}
	var position int64
	find := tx.QueryRow(ctx, `SELECT prepared_through+1 FROM input_sessions WHERE source='SATELIT' AND namespace=$1 AND prepared_through<received_through`, e.Config.Namespace).Scan(&position)
	if find == nil {
		_, err = tx.Exec(ctx, `INSERT INTO wildfire_jobs(namespace,position,phase,topology,batch_id,blocked_reason) VALUES($1,$2,'start',$3,$4,$5) ON CONFLICT DO NOTHING`, e.Config.Namespace, position, topology, wildfire.BatchIdentity(e.Config.Namespace, position), reason)
		if err != nil {
			return
		}
	}
	_, err = tx.Exec(ctx, `UPDATE wildfire_jobs SET blocked_reason=$2 WHERE namespace=$1 AND NOT applied`, e.Config.Namespace, reason)
	if err == nil {
		_ = tx.Commit(ctx)
	}
	platform.Gauge("wildfire_blocked_output", 1)
}

// Fires returns a bounded page for inspection/testing, not a retained snapshot.
func (e *Engine) Fires(ctx context.Context, after string, limit int) ([]wildfire.Fire, error) {
	if limit < 1 || limit > e.Config.PageRows {
		return nil, fmt.Errorf("bounded fire page required")
	}
	var pos int64
	if err := e.Store.Pool.QueryRow(ctx, `SELECT prepared_through FROM wildfire_runs WHERE namespace=$1 AND config_sha256=$2`, e.Config.Namespace, e.fingerprint).Scan(&pos); err != nil {
		return nil, err
	}
	if _, err := e.Store.ProcessorStorageBoundary(ctx, ""); err != nil {
		return nil, err
	}
	rows, err := e.page(ctx, e.Store.Pool, "fire", pos, after)
	if err != nil {
		return nil, err
	}
	out := []wildfire.Fire{}
	for _, r := range rows {
		if len(out) == limit {
			break
		}
		var f wildfire.Fire
		if err = json.Unmarshal(r.Payload, &f); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, nil
}
func sortedFireIDs(fires map[string]wildfire.Fire) []string {
	ids := make([]string, 0, len(fires))
	for id := range fires {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
