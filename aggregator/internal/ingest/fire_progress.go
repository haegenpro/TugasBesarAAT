package ingest

import (
	"aggregator/internal/wildfire"
	"context"
	"encoding/json"
	"errors"
	"time"

	"aggregator/internal/store"

	"github.com/jackc/pgx/v5"
)

// fireProgress is the wildfire source's progress row as seen by one preparation transaction.
//
// Preparation used to read and update source_progress several times per input (the prepared
// frontier for the gap check, the watermark, the output counter, region counts, the virtual clock,
// the frontiers), and its first update locked that row until the transaction committed, so
// ingestion appends and canonical completion queued behind every batch. Only the lane holder
// prepares a namespace (lease plus commit fence), so the row is read once without a lock, each
// input works on these in-memory values, and one statement plus frontiers() writes the result just
// before the commit. Each input's changes are snapshotted and restored when its savepoint rolls
// back, so the written result is exactly what the per-input statements would have produced.
type fireProgress struct {
	// single commits after every input (the one-input-per-transaction path), so it flushes first.
	single                bool
	loaded                bool
	source, namespace     string
	prepared, output      int64
	watermark, virtualNow *time.Time
	singletons, excluded  int64 // deltas since load
	late                  int64 // delta since load
	provinces             []wildfire.Province
}

func (p *fireProgress) load(ctx context.Context, tx pgx.Tx, i Input) error {
	if p.loaded && p.source == i.Source && p.namespace == i.Namespace {
		return nil
	}
	if p.loaded {
		if e := p.flush(ctx, tx); e != nil {
			return e
		}
	}
	*p = fireProgress{single: p.single, source: i.Source, namespace: i.Namespace}
	if e := tx.QueryRow(ctx, `SELECT prepared,output_position,watermark,virtual_now FROM source_progress WHERE source=$1 AND namespace=$2`, i.Source, i.Namespace).Scan(&p.prepared, &p.output, &p.watermark, &p.virtualNow); e != nil {
		return e
	}
	p.loaded = true
	return nil
}

// refreshPrepared recomputes the frontier from the inputs' states, the way every input used to.
// It is used only when the in-memory frontier says an input is out of order.
func (p *fireProgress) refreshPrepared(ctx context.Context, tx pgx.Tx) error {
	if e := frontiers(ctx, tx, p.source, p.namespace); e != nil {
		return e
	}
	return tx.QueryRow(ctx, `SELECT prepared FROM source_progress WHERE source=$1 AND namespace=$2`, p.source, p.namespace).Scan(&p.prepared)
}

// done records that input position is no longer pending; the frontier moves when it was next.
func (p *fireProgress) done(position int64) {
	if position == p.prepared+1 {
		p.prepared = position
	}
}

func (p *fireProgress) clock(virtualNow time.Time, lateness time.Duration) {
	if p.virtualNow == nil || virtualNow.After(*p.virtualNow) {
		v := virtualNow
		p.virtualNow = &v
	}
	w := virtualNow.Add(-lateness)
	if p.watermark == nil || w.After(*p.watermark) {
		p.watermark = &w
	}
}

func (p *fireProgress) regionProvinces(ctx context.Context, tx pgx.Tx) ([]wildfire.Province, error) {
	if p.provinces == nil {
		provinces, e := readProvinces(ctx, tx, p.namespace)
		if e != nil {
			return nil, e
		}
		p.provinces = provinces
	}
	return p.provinces, nil
}

// flush writes the accumulated progress and the frontiers once.
func (p *fireProgress) flush(ctx context.Context, tx pgx.Tx) error {
	if !p.loaded {
		return nil
	}
	locked := time.Now()
	_, e := tx.Exec(ctx, `UPDATE source_progress SET singletons=singletons+$3,excluded=excluded+$4,late_mutations=late_mutations+$5,
 output_position=GREATEST(output_position,$6),virtual_now=GREATEST(virtual_now,$7),watermark=GREATEST(watermark,$8) WHERE source=$1 AND namespace=$2`,
		p.source, p.namespace, p.singletons, p.excluded, p.late, p.output, p.virtualNow, p.watermark)
	lockWait("source_progress", locked)
	if e != nil {
		return e
	}
	if e = frontiers(ctx, tx, p.source, p.namespace); e != nil {
		return e
	}
	p.singletons, p.excluded, p.late = 0, 0, 0
	return nil
}

// finish ends one input: in the one-input path it writes the progress and commits; inside a batch
// it only releases the input's savepoint and the batch writes the progress before its commit.
func (p *fireProgress) finish(ctx context.Context, tx pgx.Tx) error {
	if p.single {
		if e := p.flush(ctx, tx); e != nil {
			return e
		}
	}
	return commit(ctx, tx, "prepare_fire")
}

// park is parkInput for the wildfire path: the same rows, with the frontier kept in memory.
func (p *fireProgress) park(ctx context.Context, tx pgx.Tx, i Input, reason string) error {
	if e := parkRows(ctx, tx, i, reason); e != nil {
		return e
	}
	p.done(i.Position)
	return p.finish(ctx, tx)
}

// superseded is finishSupersededFire with the frontier kept in memory.
func (p *fireProgress) superseded(ctx context.Context, tx pgx.Tx, i Input, holder string) error {
	if _, e := tx.Exec(ctx, `UPDATE raw_inputs SET state='applied',reason='superseded by a newer source offset' WHERE message_id=$1`, i.ID); e != nil {
		return e
	}
	if holder == i.ID {
		if e := releaseEntity(ctx, tx, i); e != nil {
			return e
		}
	}
	p.done(i.Position)
	return p.finish(ctx, tx)
}

// queue is queueCanonical for the wildfire path: the output counter is kept in memory and the
// input's metadata and state are written in one statement. The resulting rows are the same.
func (p *fireProgress) queue(ctx context.Context, tx pgx.Tx, i Input, b store.CanonicalBatch) error {
	metadata, e := json.Marshal(traceMetadata(ctx, i.Metadata))
	if e != nil {
		return e
	}
	if len(b.Changes) == 0 {
		_, e = tx.Exec(ctx, `UPDATE raw_inputs SET metadata=$2,state='applied' WHERE message_id=$1`, i.ID, metadata)
		return e
	}
	if i.Source != "SATELIT" {
		return errors.New("wildfire output requires the satellite source")
	}
	p.output++
	b.Previous = p.output - 1
	b.Position = p.output
	b.Namespace = i.Namespace + ":wildfire-output"
	raw, e := json.Marshal(b)
	if e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO canonical_outbox(operation_id,message_id,source,namespace,position,payload) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, b.ID, i.ID, i.Source, b.Namespace, b.Position, raw); e != nil {
		return e
	}
	_, e = tx.Exec(ctx, `UPDATE raw_inputs SET metadata=$2,state='prepared' WHERE message_id=$1`, i.ID, metadata)
	return e
}
