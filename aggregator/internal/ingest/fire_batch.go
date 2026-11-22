package ingest

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/ownership"
	"aggregator/internal/platform"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/trace"
)

// Inputs loads several immutable raw records with one query.
func (s *Store) Inputs(ctx context.Context, ids []string) (map[string]Input, error) {
	rows, e := s.Pool.Query(ctx, `SELECT `+inputColumns+` FROM raw_inputs i WHERE message_id=ANY($1::uuid[])`, ids)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := make(map[string]Input, len(ids))
	for rows.Next() {
		i, e := scanInput(rows)
		if e != nil {
			return nil, e
		}
		out[i.ID] = i
	}
	return out, rows.Err()
}

// NoteAttempts records, in its own short transaction and before any processing, that these
// records are about to be processed. It has the meaning NoteAttempt always had: the count is
// durable even if the processing transaction later rolls back or the process crashes, and a
// recorded attempt does not mean processing finished.
func (s *Store) NoteAttempts(ctx context.Context, ids []string) error {
	tx, e := s.begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if e = ownership.Guard(ctx, tx); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `UPDATE raw_inputs SET attempts=attempts+1 WHERE message_id=ANY($1::uuid[])`, ids); e != nil {
		return e
	}
	return commit(ctx, tx, "attempts")
}

type fireWork struct {
	msg   jetstream.Msg
	input Input
	ctx   context.Context
	span  trace.Span
}

// fireBatcher prepares consecutive wildfire inputs, in source order, within one transaction.
//
// Per fetch it loads the records with one query, records the attempts (own transaction),
// prepares each input under its own savepoint, commits once, and acknowledges only after the
// commit. An input that fails rolls back to its savepoint; it is an ordering barrier, because
// later positions cannot be prepared before it, so it and everything after it are redelivered
// while the inputs before it stay committed. After a failure of the batch as a whole the batch
// size drops to one and then doubles back on success, so one bad record cannot make the same
// large batch fail again and again.
type fireBatcher struct {
	s          *Store
	max, size  int
	ackWorkers int
	// confirm replaces the broker confirmation in tests that inject lost acknowledgements.
	confirm func(jetstream.Msg) error
}

func newFireBatcher(s *Store) *fireBatcher {
	limit := max(1, platform.Int("WILDFIRE_BATCH_RECORDS", 25))
	return &fireBatcher{s: s, max: limit, size: limit, ackWorkers: max(1, platform.Int("WILDFIRE_ACK_CONCURRENCY", limit))}
}

func (b *fireBatcher) fail(msgs []jetstream.Msg, reason error, id, correlation string) {
	for _, msg := range msgs {
		_ = msg.NakWithDelay(time.Second)
	}
	platform.Add("wildfire_worker_retries_total", 1)
	slog.Warn("worker retry", "service", "wildfire-processor", "correlation_id", correlation, "message_id", id, "error", reason.Error())
}

// ack confirms messages after their commit, with bounded concurrency. A failed confirmation
// only causes redelivery, which the idempotent state check turns into a no-op.
func (b *fireBatcher) ack(ctx context.Context, work []fireWork) {
	started := time.Now()
	sem := make(chan struct{}, b.ackWorkers)
	var wg sync.WaitGroup
	for _, w := range work {
		sem <- struct{}{}
		wg.Add(1)
		go func(w fireWork) {
			defer func() { <-sem; wg.Done() }()
			at := time.Now()
			var e error
			if b.confirm != nil {
				e = b.confirm(w.msg)
			} else {
				e = w.msg.DoubleAck(ctx)
			}
			platform.Observe("ingest_ack_seconds", time.Since(at).Seconds())
			if e != nil {
				platform.Add("ingest_ack_failures_total", 1)
				return
			}
			inputLog(w.ctx, w.input, "raw input processed")
		}(w)
	}
	wg.Wait()
	platform.Observe("ingest_stage_seconds_fire_batch_ack", time.Since(started).Seconds())
}

var errMissingInput = errors.New("raw input record not found")

// isBatchLevel reports errors that invalidate the whole transaction rather than one input:
// a lost connection or an expired ownership token cannot be isolated to a savepoint.
func isBatchLevel(e error) bool {
	return errors.Is(e, pgx.ErrTxClosed) || errors.Is(e, ownership.ErrFenced) || errors.Is(e, context.DeadlineExceeded) || errors.Is(e, context.Canceled)
}

func (b *fireBatcher) step(owned context.Context, consumer jetstream.Consumer, registry *avrowire.Client, lateness time.Duration) error {
	at := time.Now()
	fetched, e := consumer.Fetch(b.size, jetstream.FetchMaxWait(time.Second))
	if e != nil {
		return e
	}
	var messages []jetstream.Msg
	for msg := range fetched.Messages() {
		messages = append(messages, msg)
	}
	if len(messages) == 0 {
		return fetched.Error()
	}
	platform.Observe("ingest_stage_seconds_fire_batch_fetch", time.Since(at).Seconds())

	call, cancel := context.WithTimeout(owned, 30*time.Second)
	defer cancel()
	at = time.Now()
	ids := make([]string, 0, len(messages))
	var decodeFailure error
	for _, msg := range messages {
		i, e := decodeInput(call, registry, msg.Data())
		if e != nil {
			decodeFailure = e
			break
		}
		ids = append(ids, i.ID)
	}
	loaded, e := b.s.Inputs(call, ids)
	if e != nil {
		b.fail(messages, e, "", "")
		b.adapt(true)
		return e
	}
	var work []fireWork
	payloadBytes := 0
	for index, id := range ids {
		input, ok := loaded[id]
		if !ok {
			decodeFailure = errMissingInput
			break
		}
		mctx, span := platform.Start(platform.Extract(call, http.Header(messages[index].Headers())), "process raw input")
		work = append(work, fireWork{messages[index], input, mctx, span})
		payloadBytes += len(input.Payload)
	}
	if len(work) < len(messages) {
		// Everything from the first unreadable message on goes back, in order.
		b.fail(messages[len(work):], decodeFailure, "", "")
	}
	platform.Observe("ingest_stage_seconds_fire_batch_load", time.Since(at).Seconds())
	if len(work) == 0 {
		return nil
	}
	endSpans := func() {
		for _, w := range work {
			w.span.End()
		}
	}
	// A failure of the batch as a whole sends it back, shrinks the next batch to one input and
	// pauses briefly; only a lost ownership token ends the lane.
	whole := func(e error) error {
		endSpans()
		b.fail(messages[:len(work)], e, "", "")
		b.adapt(true)
		if errors.Is(e, ownership.ErrFenced) {
			return e
		}
		Sleep(owned, 200*time.Millisecond)
		return nil
	}

	items := make([]chunkItem, len(work))
	for index, w := range work {
		items[index] = chunkItem{input: w.input, ctx: w.ctx}
	}
	done, failed, e := b.s.prepareChunkFilled(call, items, lateness)
	endSpans()
	if e != nil {
		return whole(e)
	}
	batchStats("prepare_fire", done, payloadBytes)
	b.ack(call, work[:done])
	if failed != nil {
		b.fail(messages[done:len(work)], failed.err, failed.id, failed.correlation)
	}
	b.adapt(false)
	return nil
}

// adapt sizes the next fetch: one input after a failure of a whole batch, doubling back to the
// configured size after each success.
func (b *fireBatcher) adapt(wholeFailure bool) {
	if wholeFailure {
		b.size = 1
		return
	}
	b.size = min(b.max, max(1, b.size*2))
}

type chunkItem struct {
	input Input
	ctx   context.Context
}

type chunkFailure struct {
	err             error
	id, correlation string
}

// gapInputs returns the durable inputs between the prepared frontier and next, in position order,
// when next arrived ahead of them. The broker only notifies; the inputs themselves are already
// committed in raw_inputs, so waiting for their (possibly delayed or still-pending) redelivery is
// unnecessary. Nothing is returned unless the whole gap is present and pending, so the existing
// contiguity check and ordering barrier still decide; replays keep their own path.
func (s *Store) gapInputs(ctx context.Context, next Input) ([]Input, error) {
	if next.Source != "SATELIT" || replayGeneration(next) > 0 {
		return nil, nil
	}
	var prepared int64
	if e := s.Pool.QueryRow(ctx, `SELECT prepared FROM source_progress WHERE source=$1 AND namespace=$2`, next.Source, next.Namespace).Scan(&prepared); e != nil {
		return nil, e
	}
	gap := next.Position - prepared - 1
	if gap <= 0 || gap > int64(max(0, platform.Int("WILDFIRE_GAP_FILL_MAX", 500))) {
		return nil, nil
	}
	rows, e := s.Pool.Query(ctx, `SELECT `+inputColumns+` FROM raw_inputs i WHERE source=$1 AND namespace=$2 AND position>$3 AND position<$4 AND state='pending' ORDER BY position`, next.Source, next.Namespace, prepared, next.Position)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []Input
	for rows.Next() {
		i, e := scanInput(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, i)
	}
	if e = rows.Err(); e != nil {
		return nil, e
	}
	if int64(len(out)) != gap {
		return nil, nil
	}
	return out, nil
}

// prepareChunkFilled is prepareChunk preceded, when the first input is ahead of the prepared
// frontier, by the missing inputs read from raw_inputs. They go through the same attempt
// accounting, savepoints and ordering barrier. done counts only the given items; a failure among
// the filled inputs leaves done at zero, so every given item is retried in order.
func (s *Store) prepareChunkFilled(call context.Context, items []chunkItem, lateness time.Duration) (int, *chunkFailure, error) {
	if len(items) == 0 {
		return 0, nil, nil
	}
	fill, e := s.gapInputs(call, items[0].input)
	if e != nil {
		return 0, nil, e
	}
	all := items
	if len(fill) > 0 {
		all = make([]chunkItem, 0, len(fill)+len(items))
		for _, i := range fill {
			all = append(all, chunkItem{input: i, ctx: items[0].ctx})
		}
		all = append(all, items...)
	}
	done, failed, e := s.prepareChunk(call, all, lateness)
	if e != nil {
		return 0, nil, e
	}
	platform.Add("wildfire_gap_fill_records_total", float64(min(done, len(fill))))
	return max(0, done-len(fill)), failed, nil
}

// prepareChunk prepares consecutive inputs in order. It returns how many inputs are committed;
// failed describes the first input that failed on its own (rolled back to its savepoint, and an
// ordering barrier for the rest, which are not attempted); err is set when the batch as a whole
// failed and nothing was committed. Attempts are recorded first, in their own transaction.
func (s *Store) prepareChunk(call context.Context, items []chunkItem, lateness time.Duration) (done int, failed *chunkFailure, err error) {
	at := time.Now()
	ids := make([]string, len(items))
	for index, item := range items {
		ids[index] = item.input.ID
	}
	if err = s.NoteAttempts(call, ids); err != nil {
		return 0, nil, err
	}
	platform.Observe("ingest_stage_seconds_fire_batch_attempts", time.Since(at).Seconds())
	at = time.Now()
	tx, err := s.begin(call)
	if err != nil {
		return 0, nil, err
	}
	defer tx.Rollback(call)
	if err = ownership.Guard(call, tx); err != nil {
		return 0, nil, err
	}
	progress := &fireProgress{}
	for _, item := range items {
		// An input that fails rolls back its savepoint, so its progress changes are undone too.
		before := *progress
		savepoint, e := tx.Begin(item.ctx)
		if e == nil {
			e = s.prepareFireIn(item.ctx, savepoint, item.input, lateness, progress)
			if e != nil {
				_ = savepoint.Rollback(item.ctx)
				*progress = before
			}
		}
		if e != nil {
			if isBatchLevel(e) {
				return 0, nil, e
			}
			failed = &chunkFailure{e, item.input.ID, item.input.CorrelationID}
			break
		}
		done++
	}
	// The progress row is written (and locked) only now, just before the commit.
	if err = progress.flush(call, tx); err != nil {
		return 0, nil, err
	}
	platform.Observe("ingest_stage_seconds_fire_batch_process", time.Since(at).Seconds())
	if err = commit(call, tx, "prepare_fire_batch"); err != nil {
		return 0, nil, err
	}
	return done, failed, nil
}
