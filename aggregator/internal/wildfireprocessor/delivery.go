package wildfireprocessor

import (
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"aggregator/internal/middleware"
	"aggregator/internal/platform"
	"aggregator/internal/rawinput"
	"aggregator/internal/wildfire"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

type output struct {
	Position, Count, Bytes int64
	ID                     string
	Digest                 *string
	Chunks, Cursor         int
	Sealed                 bool
	Topology               string
}

func (e *Engine) intentPage(ctx context.Context, pos int64, after string) ([]record, error) {
	rows, err := e.Store.Pool.Query(ctx, `SELECT entity_key,payload,sha256 FROM wildfire_state WHERE namespace=$1 AND kind='intent' AND position=$2 AND entity_key COLLATE "C">$3 COLLATE "C" ORDER BY entity_key COLLATE "C" LIMIT $4`, e.Config.Namespace, pos, after, e.Config.PageRows)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []record{}
	var total int64
	for rows.Next() {
		var r record
		var sum string
		if err = rows.Scan(&r.Key, &r.Payload, &sum); err != nil {
			return nil, err
		}
		total += int64(len(r.Payload) + 256)
		if total > e.Config.CacheBytes/2 {
			return nil, &Pending{"intent page quota"}
		}
		if sum != rawinput.Hash(r.Payload) {
			return nil, fmt.Errorf("intent integrity")
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
func (e *Engine) executable(ctx context.Context, o output) ([]byte, error) {
	mutations := []canonical.Mutation{}
	after := ""
	var size int64
	for {
		page, err := e.intentPage(ctx, o.Position, after)
		if err != nil {
			return nil, err
		}
		for _, r := range page {
			var intent wildfire.Intent
			if err = json.Unmarshal(r.Payload, &intent); err != nil {
				return nil, err
			}
			entity, err := e.Store.Reconcile(ctx, domain.SourceSatelit, intent.SourceRefID)
			var revision int64
			if err != nil {
				var problem *canonical.Error
				if !errors.As(err, &problem) || problem.Status != 404 {
					return nil, err
				}
			} else {
				revision = entity.Revision
			}
			m := canonical.Mutation{OperationID: intent.OperationID, Source: domain.SourceSatelit, SourceRefID: intent.SourceRefID, Operation: intent.Operation, SourceOrder: canonical.SourceOrder{Namespace: intent.SourceOrder.Namespace, Position: intent.SourceOrder.Position}, ExpectedRevision: revision}
			if intent.Event != nil {
				attrs, _ := json.Marshal(intent.Event.Attributes)
				var values map[string]any
				decoder := json.NewDecoder(bytes.NewReader(attrs))
				decoder.UseNumber()
				if err = decoder.Decode(&values); err != nil {
					return nil, err
				}
				v := intent.Event
				m.Event = &canonical.MutableEvent{HazardType: v.HazardType, Severity: v.Severity, AreaName: v.AreaName, Latitude: v.Latitude, Longitude: v.Longitude, OccurredAt: v.OccurredAt, Attributes: values}
			}
			encoded, err := json.Marshal(m)
			if err != nil {
				return nil, err
			}
			size += int64(len(encoded) + 1)
			if size+2 > e.Config.PreparedBytes {
				return nil, &Pending{"complete correction exceeds prepared quota"}
			}
			mutations = append(mutations, m)
			after = r.Key
		}
		if len(page) < e.Config.PageRows {
			break
		}
	}
	if int64(len(mutations)) != o.Count {
		return nil, canonical.Unavailable()
	}
	return json.Marshal(mutations)
}

func (e *Engine) seal(ctx context.Context, owner canonical.Ownership, o output) (bool, error) {
	body, err := e.executable(ctx, o)
	if err != nil {
		return false, err
	}
	sum := rawinput.Hash(body)
	if o.Digest != nil && *o.Digest != sum {
		return false, fmt.Errorf("canonical state changed during immutable request sealing")
	}
	chunks := (len(body) + (64 << 10) - 1) / (64 << 10)
	tx, err := e.begin(ctx, owner)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var through int64
	if err = tx.QueryRow(ctx, `SELECT applied_through FROM wildfire_runs WHERE namespace=$1 FOR UPDATE`, e.Config.Namespace).Scan(&through); err != nil {
		return false, err
	}
	if through != o.Position-1 {
		return false, canonical.Unavailable()
	}
	var cursor int
	var current *string
	var sealed bool
	var bytes int64
	err = tx.QueryRow(ctx, `SELECT output_sha256,seal_cursor,sealed,staged_bytes FROM wildfire_jobs WHERE namespace=$1 AND position=$2 AND phase='active' FOR UPDATE`, e.Config.Namespace, o.Position).Scan(&current, &cursor, &sealed, &bytes)
	if err != nil {
		return false, err
	}
	if sealed {
		return true, nil
	}
	if current != nil && *current != sum {
		return false, fmt.Errorf("sealed request identity conflict")
	}
	if bytes+int64(len(body)) > e.Config.StagingBytes {
		return false, &Pending{"sealed output staging quota"}
	}
	end := min(chunks, cursor+e.Config.PageRows)
	for i := cursor; i < end; i++ {
		b := body[i*(64<<10) : min(len(body), (i+1)*(64<<10))]
		key := strconv.FormatInt(o.Position, 10) + ":" + strconv.Itoa(i)
		_, err = tx.Exec(ctx, `INSERT INTO wildfire_output_chunks(route_key,namespace,position,ordinal,payload,sha256) VALUES(wildfire_route($1,'output',$2),$1,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, e.Config.Namespace, key, o.Position, i, b, rawinput.Hash(b))
		if err != nil {
			return false, err
		}
		var hash string
		if err = tx.QueryRow(ctx, `SELECT sha256 FROM wildfire_output_chunks WHERE route_key=wildfire_route($1,'output',$2) AND namespace=$1 AND position=$3 AND ordinal=$4`, e.Config.Namespace, key, o.Position, i).Scan(&hash); err != nil {
			return false, err
		}
		if hash != rawinput.Hash(b) {
			return false, fmt.Errorf("immutable output chunk conflict")
		}
	}
	_, err = tx.Exec(ctx, `UPDATE wildfire_jobs SET output_sha256=$3,output_bytes=$4,output_chunks=$5,seal_cursor=$6,sealed=$7 WHERE namespace=$1 AND position=$2`, e.Config.Namespace, o.Position, sum, len(body), chunks, end, end == chunks)
	if err != nil {
		return false, err
	}
	if err = e.hook(ctx, "before_seal_commit"); err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, canonical.CommitError(err)
	}
	return true, e.Store.ConfirmProcessorRecord(ctx, `SELECT EXISTS(SELECT 1 FROM wildfire_jobs WHERE namespace=$1 AND position=$2 AND output_sha256=$3 AND seal_cursor>=$4)`, e.Config.Namespace, o.Position, sum, end)
}

// StepDelivery seals or delivers only the earliest unapplied input. Immutable
// sealed bodies survive process death and are retried before revision checks.
func (e *Engine) StepDelivery(ctx context.Context, processor, delivery canonical.Ownership) (worked bool, resultErr error) {
	ctx, cancel := context.WithTimeout(ctx, canonical.Deadline)
	defer cancel()
	defer func() {
		if resultErr != nil && ctx.Err() == nil {
			e.recordPending(ctx, processor, resultErr.Error())
		}
	}()
	var applied, prepared int64
	if err := e.Store.Pool.QueryRow(ctx, `SELECT applied_through,prepared_through FROM wildfire_runs WHERE namespace=$1 AND config_sha256=$2`, e.Config.Namespace, e.fingerprint).Scan(&applied, &prepared); errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	if applied >= prepared {
		return false, nil
	}
	o := output{Position: applied + 1}
	err := e.Store.Pool.QueryRow(ctx, `SELECT batch_id,output_count,sealed,output_sha256,coalesce(output_bytes,0),coalesce(output_chunks,0),seal_cursor,topology FROM wildfire_jobs WHERE namespace=$1 AND position=$2 AND phase='active'`, e.Config.Namespace, o.Position).Scan(&o.ID, &o.Count, &o.Sealed, &o.Digest, &o.Bytes, &o.Chunks, &o.Cursor, &o.Topology)
	if err != nil {
		return false, err
	}
	if _, err = e.Store.ProcessorStorageBoundary(ctx, o.Topology); err != nil {
		return false, err
	}
	var state string
	if err = e.Store.Pool.QueryRow(ctx, `SELECT state FROM input_history WHERE route_key=input_route('SATELIT',$1,$2) AND source='SATELIT' AND namespace=$1 AND position=$2`, e.Config.Namespace, o.Position).Scan(&state); err != nil || state != "prepared" {
		return false, canonical.Unavailable()
	}
	if o.Count > 0 {
		if !o.Sealed {
			return e.seal(ctx, processor, o)
		}
		if o.Digest == nil || o.Bytes < 1 || o.Bytes > e.Config.PreparedBytes {
			return false, canonical.Unavailable()
		}
		env, _, err := e.input(ctx, e.Store.Pool, o.Position)
		if err != nil {
			return false, err
		}
		ctx = middleware.WithCorrelationID(ctx, env.CorrelationID)
		if env.Traceparent != nil {
			ctx = platform.Extract(ctx, http.Header{"Traceparent": []string{*env.Traceparent}})
		}
		ctx, span := platform.Start(ctx, "wildfire canonical batch")
		defer span.End()
		mutations, err := canonical.ResolvePrepared(ctx, e, e.Config.PreparedBytes, canonical.PreparedReference{ID: o.ID, Count: o.Count, SHA256: *o.Digest})
		if err != nil {
			return false, err
		}
		batch := canonical.Batch{ContractVersion: 1, BatchID: o.ID, Ownership: delivery, Mutations: mutations}
		if err = e.hook(ctx, "before_canonical_apply"); err != nil {
			return false, err
		}
		if _, err = e.Store.ApplyBatch(ctx, batch); err != nil {
			return false, err
		}
		if err = e.hook(ctx, "after_canonical_apply"); err != nil {
			return false, err
		}
	}
	if err = e.hook(ctx, "before_applied_commit"); err != nil {
		return false, err
	}
	tx, err := e.begin(ctx, processor)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var pending bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM canonical_attempts WHERE state='pending')`).Scan(&pending); err != nil || pending {
		return false, canonical.Unavailable()
	}
	tag, err := tx.Exec(ctx, `UPDATE wildfire_runs SET applied_through=$2 WHERE namespace=$1 AND applied_through=$2-1 AND prepared_through>=$2 AND config_sha256=$3`, e.Config.Namespace, o.Position, e.fingerprint)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() != 1 {
		return false, canonical.Unavailable()
	}
	tag, err = tx.Exec(ctx, `UPDATE input_sessions SET applied_through=$2 WHERE source='SATELIT' AND namespace=$1 AND applied_through=$2-1 AND prepared_through>=$2`, e.Config.Namespace, o.Position)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() != 1 {
		return false, canonical.Unavailable()
	}
	_, err = tx.Exec(ctx, `UPDATE input_history SET state='applied' WHERE route_key=input_route('SATELIT',$1,$2) AND source='SATELIT' AND namespace=$1 AND position=$2 AND state='prepared'`, e.Config.Namespace, o.Position)
	if err != nil {
		return false, err
	}
	_, err = tx.Exec(ctx, `UPDATE wildfire_jobs SET applied=true,blocked_reason=NULL WHERE namespace=$1 AND position=$2 AND phase='active'`, e.Config.Namespace, o.Position)
	if err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, canonical.Unavailable()
	}
	if err = e.hook(ctx, "after_applied_commit"); err != nil {
		return false, err
	}
	if err = e.Store.ConfirmProcessorRecord(ctx, `SELECT EXISTS(SELECT 1 FROM wildfire_runs WHERE namespace=$1 AND applied_through>=$2)`, e.Config.Namespace, o.Position); err != nil {
		return false, err
	}
	platform.Gauge("wildfire_applied_through", float64(o.Position))
	if env, _, find := e.input(ctx, e.Store.Pool, o.Position); find == nil {
		if at, find := time.Parse(time.RFC3339Nano, env.ReceivedAt); find == nil {
			platform.Observe("wildfire_input_to_canonical_seconds", time.Since(at).Seconds())
		}
	}
	return true, nil
}

type chunkReader struct {
	engine        *Engine
	ctx           context.Context
	cancel        context.CancelFunc
	position      int64
	chunks, index int
	buffer        *bytes.Reader
	closed        atomic.Bool
}

func (e *Engine) Open(ctx context.Context, ref canonical.PreparedReference) (io.ReadCloser, error) {
	var pos, count, size int64
	var sum string
	var chunks int
	err := e.Store.Pool.QueryRow(ctx, `SELECT position,output_count,output_sha256,output_bytes,output_chunks FROM wildfire_jobs WHERE namespace=$1 AND batch_id=$2 AND phase='active' AND sealed`, e.Config.Namespace, ref.ID).Scan(&pos, &count, &sum, &size, &chunks)
	if err != nil || ref.Count != count || ref.SHA256 != sum || size < 1 || size > e.Config.PreparedBytes || chunks < 1 {
		return nil, canonical.Unavailable()
	}
	ctx, cancel := context.WithCancel(ctx)
	return &chunkReader{engine: e, ctx: ctx, cancel: cancel, position: pos, chunks: chunks}, nil
}
func (r *chunkReader) Read(p []byte) (int, error) {
	if r.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	for r.buffer == nil || r.buffer.Len() == 0 {
		if r.index == r.chunks {
			return 0, io.EOF
		}
		key := strconv.FormatInt(r.position, 10) + ":" + strconv.Itoa(r.index)
		var b []byte
		var sum string
		err := r.engine.Store.Pool.QueryRow(r.ctx, `SELECT payload,sha256 FROM wildfire_output_chunks WHERE route_key=wildfire_route($1,'output',$2) AND namespace=$1 AND position=$3 AND ordinal=$4`, r.engine.Config.Namespace, key, r.position, r.index).Scan(&b, &sum)
		if err != nil {
			return 0, err
		}
		if len(b) > 64<<10 || rawinput.Hash(b) != sum {
			return 0, fmt.Errorf("output chunk integrity")
		}
		r.buffer = bytes.NewReader(b)
		r.index++
	}
	return r.buffer.Read(p)
}
func (r *chunkReader) Close() error {
	if !r.closed.Swap(true) {
		r.cancel()
	}
	return nil
}

var _ canonical.PreparedResolver = (*Engine)(nil)
