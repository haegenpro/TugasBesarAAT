package ingest

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/ownership"
	"aggregator/internal/platform"
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Raw publication moves records from the durable raw outbox to JetStream in three separate steps,
// none of which holds a database transaction while the broker is called:
//
//  1. claim: a short transaction stamps a bounded set of due entries with this owner's identity,
//     lease epoch and an expiry;
//  2. publish: the broker is called with stable message identities (record id plus replay
//     generation), so a repeat is a broker duplicate, and beyond the broker's duplicate window the
//     consumers' idempotent state check makes it a business no-op;
//  3. complete: one short transaction deletes exactly the entries whose publication was acknowledged
//     and whose claim, generation and ownership are still the ones that were published.
//
// An entry that is not completed is simply claimable again: immediately after a failure, after its
// claim expires, or at once when the lease changed hands, because only the current lease holder can
// legitimately hold a claim.

// PublishConfig bounds one publication cycle.
type PublishConfig struct {
	Batch         int           // bulk entries per cycle (satellite and ordinary coastal)
	ReservedBatch int           // critical coastal and Field entries per cycle
	Lease         time.Duration // how long a claim is exclusive
	AckTimeout    time.Duration // how long a cycle waits for broker acknowledgements
	Lanes         int           // 1: one ordered loop; 2: critical coastal and Field get their own loop
}

func PublishConfigFromEnv() PublishConfig {
	return PublishConfig{
		Batch: max(1, platform.Int("RAW_PUBLISH_BATCH", 100)), ReservedBatch: max(1, platform.Int("RAW_PUBLISH_RESERVED_BATCH", 50)),
		Lease: platform.Duration("RAW_PUBLISH_CLAIM_LEASE", 30*time.Second), AckTimeout: platform.Duration("RAW_PUBLISH_ACK_TIMEOUT", 10*time.Second),
		Lanes: min(2, max(1, platform.Int("RAW_PUBLISH_LANES", 1))),
	}
}

func (c PublishConfig) normalized() PublishConfig {
	d := PublishConfigFromEnv()
	if c.Batch <= 0 {
		c.Batch = d.Batch
	}
	if c.ReservedBatch <= 0 {
		c.ReservedBatch = d.ReservedBatch
	}
	if c.Lease <= 0 {
		c.Lease = d.Lease
	}
	if c.AckTimeout <= 0 {
		c.AckTimeout = d.AckTimeout
	}
	if c.Lanes <= 0 {
		c.Lanes = d.Lanes
	}
	return c
}

type outboxClaim struct {
	Input
	Subject    string
	Generation int64
}

func subjectPriority(subject string) int {
	switch subject {
	case "raw.coastal.critical":
		return 0
	case "raw.field":
		return 1
	case "raw.coastal.normal":
		return 2
	}
	return 3
}

// claimOutbox claims up to limit due entries whose priority lies in [low, high], in delivery order.
func (s *Store) claimOutbox(ctx context.Context, limit int, lease time.Duration, low, high int) ([]outboxClaim, error) {
	defer stage("publish_claim")()
	token, ok := ownership.Current(ctx)
	if !ok {
		return nil, ownership.ErrFenced
	}
	tx, e := s.begin(ctx)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback(ctx)
	if e = ownership.Guard(ctx, tx); e != nil {
		return nil, e
	}
	rows, e := tx.Query(ctx, `WITH picked AS (
 SELECT message_id FROM raw_outbox
 WHERE priority BETWEEN $5 AND $6
   AND (available_at<=clock_timestamp() OR claim_holder IS DISTINCT FROM $3 OR claim_epoch IS DISTINCT FROM $4)
 ORDER BY priority,received_at,position,message_id LIMIT $1 FOR UPDATE SKIP LOCKED)
UPDATE raw_outbox o SET available_at=clock_timestamp()+$2::interval,claim_holder=$3,claim_epoch=$4
FROM picked WHERE o.message_id=picked.message_id RETURNING o.message_id::text,o.subject,o.generation`,
		limit, fmt.Sprintf("%f seconds", lease.Seconds()), token.Holder, token.Epoch, low, high)
	if e != nil {
		return nil, e
	}
	var ids []string
	meta := map[string]outboxClaim{}
	for rows.Next() {
		var c outboxClaim
		if e = rows.Scan(&c.ID, &c.Subject, &c.Generation); e != nil {
			rows.Close()
			return nil, e
		}
		ids = append(ids, c.ID)
		meta[c.ID] = c
	}
	rows.Close()
	if e = rows.Err(); e != nil {
		return nil, e
	}
	if e = commit(ctx, tx, "publish_claim"); e != nil {
		return nil, e
	}
	if len(ids) == 0 {
		return nil, nil
	}
	inputs, e := s.Inputs(ctx, ids)
	if e != nil {
		return nil, e
	}
	claims := make([]outboxClaim, 0, len(ids))
	for _, id := range ids {
		if i, found := inputs[id]; found {
			c := meta[id]
			c.Input = i
			claims = append(claims, c)
		}
	}
	sort.Slice(claims, func(a, b int) bool {
		x, y := claims[a], claims[b]
		if px, py := subjectPriority(x.Subject), subjectPriority(y.Subject); px != py {
			return px < py
		}
		if !x.ReceivedAt.Equal(y.ReceivedAt) {
			return x.ReceivedAt.Before(y.ReceivedAt)
		}
		if x.Position != y.Position {
			return x.Position < y.Position
		}
		return x.ID < y.ID
	})
	return claims, nil
}

// publishClaims publishes the claimed entries in order without any database transaction. It reports
// per entry whether the broker acknowledged it.
func (s *Store) publishClaims(ctx context.Context, js jetstream.JetStream, registry *avrowire.Client, claims []outboxClaim, timeout time.Duration) ([]bool, error) {
	defer stage("publish_send")()
	schema, e := registry.Register(ctx, "acquired-input-reference-value", RawSchema)
	if e != nil {
		return make([]bool, len(claims)), e
	}
	futures := make([]jetstream.PubAckFuture, len(claims))
	sent := make([]time.Time, len(claims))
	bytes := 0
	var sendErr error
	for k, c := range claims {
		frame, e := avrowire.Encode(schema, referenceValue(c.Input))
		if e != nil {
			sendErr = e
			break
		}
		msg := nats.NewMsg(c.Subject)
		msg.Data = frame
		msg.Header.Set("Nats-Msg-Id", c.ID+":"+fmt.Sprint(c.Generation))
		msg.Header.Set("X-Correlation-ID", c.CorrelationID)
		mctx, span := inputSpan(ctx, c.Input, "raw input publication")
		platform.Inject(mctx, http.Header(msg.Header))
		span.End()
		sent[k] = time.Now()
		if futures[k], e = js.PublishMsgAsync(msg); e != nil {
			sendErr = e
			break
		}
		bytes += len(frame)
	}
	acked := make([]bool, len(claims))
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for k, f := range futures {
		if f == nil {
			break
		}
		select {
		case <-f.Ok():
			acked[k] = true
			platform.Observe("ingest_publish_seconds_raw", time.Since(sent[k]).Seconds())
		case e := <-f.Err():
			platform.Add("ingest_publish_failures_total", 1)
			if sendErr == nil {
				sendErr = e
			}
		case <-deadline.C:
			platform.Add("ingest_publish_failures_total", 1)
			if sendErr == nil {
				sendErr = context.DeadlineExceeded
			}
			return acked, sendErr
		case <-ctx.Done():
			return acked, ctx.Err()
		}
	}
	n := 0
	for _, ok := range acked {
		if ok {
			n++
		}
	}
	batchStats("publish", n, bytes)
	return acked, sendErr
}

// completeOutbox deletes the acknowledged prefix of the batch (order is part of the contract: a
// later entry is never completed ahead of an earlier one that is still unpublished) and releases the
// remaining claims for immediate retry. Only entries whose claim holder, epoch and replay generation
// are unchanged are touched. It returns how many entries were completed.
func (s *Store) completeOutbox(ctx context.Context, claims []outboxClaim, acked []bool) (int, error) {
	defer stage("publish_complete")()
	token, ok := ownership.Current(ctx)
	if !ok {
		return 0, ownership.ErrFenced
	}
	var doneIDs, restIDs []string
	var doneGen, restGen []int64
	prefix := true
	for k, c := range claims {
		prefix = prefix && acked[k]
		if prefix {
			doneIDs, doneGen = append(doneIDs, c.ID), append(doneGen, c.Generation)
		} else {
			restIDs, restGen = append(restIDs, c.ID), append(restGen, c.Generation)
		}
	}
	tx, e := s.begin(ctx)
	if e != nil {
		return 0, e
	}
	defer tx.Rollback(ctx)
	if e = ownership.Guard(ctx, tx); e != nil {
		return 0, e
	}
	completed := 0
	if len(doneIDs) > 0 {
		if e = tx.QueryRow(ctx, `WITH done AS (DELETE FROM raw_outbox o USING unnest($1::text[],$2::bigint[]) AS d(id,generation)
 WHERE o.message_id=d.id::uuid AND o.generation=d.generation AND o.claim_holder=$3 AND o.claim_epoch=$4 RETURNING 1)
 SELECT count(*) FROM done`, doneIDs, doneGen, token.Holder, token.Epoch).Scan(&completed); e != nil {
			return 0, e
		}
	}
	if len(restIDs) > 0 {
		if _, e = tx.Exec(ctx, `UPDATE raw_outbox o SET available_at='-infinity',claim_holder=NULL,claim_epoch=NULL FROM unnest($1::text[],$2::bigint[]) AS d(id,generation)
 WHERE o.message_id=d.id::uuid AND o.generation=d.generation AND o.claim_holder=$3 AND o.claim_epoch=$4`, restIDs, restGen, token.Holder, token.Epoch); e != nil {
			return 0, e
		}
	}
	if e = commit(ctx, tx, "publish_complete"); e != nil {
		return 0, e
	}
	if lost := len(doneIDs) - completed; lost > 0 {
		// Published but no longer ours to complete: a replay re-queued it or another owner claimed it.
		// It stays in the outbox and is published again under its (new) identity.
		platform.Add("ingest_publish_completion_lost_total", float64(lost))
	}
	return completed, nil
}

// publishCycle runs one claim, publish, complete round for a priority range. It reports how many
// entries it claimed.
func (s *Store) publishCycle(ctx context.Context, js jetstream.JetStream, registry *avrowire.Client, cfg PublishConfig, limit, low, high int) (int, error) {
	claims, e := s.claimOutbox(ctx, limit, cfg.Lease, low, high)
	if e != nil || len(claims) == 0 {
		return 0, e
	}
	acked, publishErr := s.publishClaims(ctx, js, registry, claims, cfg.AckTimeout)
	completed, e := s.completeOutbox(ctx, claims, acked)
	if e != nil {
		return len(claims), e
	}
	for k := 0; k < completed && k < len(claims); k++ {
		inputLog(ctx, claims[k].Input, "raw input published")
	}
	return len(claims), publishErr
}

// PublishOnce publishes one bounded batch of everything due, critical first. It is the batch-size-one
// comparison mode when cfg.Batch is 1.
func (s *Store) PublishOnce(ctx context.Context, js jetstream.JetStream, registry *avrowire.Client, cfg PublishConfig) (int, error) {
	cfg = cfg.normalized()
	return s.publishCycle(ctx, js, registry, cfg, cfg.Batch, 0, 3)
}

func (s *Store) RunPublisher(ctx context.Context, url string, registry *avrowire.Client) {
	cfg := PublishConfigFromEnv()
	for ctx.Err() == nil {
		err := s.Leases.Run(ctx, "raw-outbox", func(c context.Context) error {
			nc, e := nats.Connect(url, nats.Timeout(time.Second))
			if e != nil {
				return e
			}
			defer nc.Close()
			js, e := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(2*(cfg.Batch+cfg.ReservedBatch)))
			if e != nil {
				return e
			}
			type lane struct{ limit, low, high int }
			lanes := []lane{{cfg.Batch, 0, 3}}
			if cfg.Lanes > 1 {
				// Critical coastal and Field traffic never queue behind a bulk batch.
				lanes = []lane{{cfg.ReservedBatch, 0, 1}, {cfg.Batch, 2, 3}}
			}
			laneCtx, cancel := context.WithCancel(c)
			defer cancel()
			var wg sync.WaitGroup
			errs := make(chan error, len(lanes))
			for _, l := range lanes {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for laneCtx.Err() == nil {
						n, e := s.publishCycle(laneCtx, js, registry, cfg, l.limit, l.low, l.high)
						if e != nil {
							errs <- e
							cancel()
							return
						}
						if n == 0 && !Sleep(laneCtx, 50*time.Millisecond) {
							return
						}
					}
				}()
			}
			wg.Wait()
			select {
			case e := <-errs:
				return e
			default:
				return c.Err()
			}
		})
		if err != nil {
			platform.Add("raw_publish_retries_total", 1)
		}
		Sleep(ctx, time.Second)
	}
}
