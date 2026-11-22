package ingest

import (
	"aggregator/internal/ownership"
	"aggregator/internal/platform"
	"aggregator/internal/store"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

// Canonical delivery uses the same pattern as raw publication: a short claim, the external call
// outside any database transaction, and a fenced completion.
//
//  1. claim: one short transaction stamps the next entries, in canonical namespace and position order,
//     with this owner's identity, lease epoch and an expiry;
//  2. deliver: each entry is POSTed to the canonical owner with its original operation identity and
//     content, strictly one after the other (the owner applies a namespace in predecessor order). An
//     ambiguous outcome (timeout, lost response) is retried unchanged, which the owner answers
//     idempotently;
//  3. complete: after the owner confirmed a run of entries, one short transaction marks their inputs
//     applied, deletes the outbox entries and advances the frontier once per source namespace. It
//     touches only entries whose claim holder and epoch are still ours.
//
// Entries that were not confirmed are released at once, so the next cycle starts at the first
// unconfirmed entry and nothing is ever sent ahead of an earlier one.

// DeliveryConfig bounds one delivery cycle.
type DeliveryConfig struct {
	Batch int           // entries claimed per cycle; 1 is the batch-size-one comparison mode
	Lease time.Duration // how long a claim is exclusive
}

func DeliveryConfigFromEnv() DeliveryConfig {
	return DeliveryConfig{Batch: max(1, platform.Int("CANONICAL_DELIVERY_BATCH", 50)), Lease: platform.Duration("CANONICAL_DELIVERY_CLAIM_LEASE", 60*time.Second)}
}

func (c DeliveryConfig) normalized() DeliveryConfig {
	d := DeliveryConfigFromEnv()
	if c.Batch <= 0 {
		c.Batch = d.Batch
	}
	if c.Lease <= 0 {
		c.Lease = d.Lease
	}
	return c
}

type canonicalClaim struct {
	Operation, Message, Namespace string
	Position                      int64
	Payload                       []byte
}

func canonicalSource(cfg WorkerConfig) string {
	if cfg.Kind == "coastal" {
		return "SENSOR_PANTAI"
	}
	return "SATELIT"
}

// releaseCanonicalClaims frees every claim this incarnation holds. It runs when a delivery loop starts
// and after any failed cycle, which keeps the loop from skipping an earlier claimed entry.
func (s *Store) releaseCanonicalClaims(ctx context.Context) error {
	if _, ok := ownership.Current(ctx); !ok {
		return ownership.ErrFenced
	}
	return s.releaseCanonicalClaimsFor(ctx, ctx)
}

func (s *Store) claimCanonical(ctx context.Context, source string, limit int, lease time.Duration) ([]canonicalClaim, error) {
	defer stage("canonical_claim")()
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
 SELECT operation_id FROM canonical_outbox
 WHERE source=$1 AND (available_at<=clock_timestamp() OR claim_holder IS DISTINCT FROM $4 OR claim_epoch IS DISTINCT FROM $5)
 ORDER BY namespace,position LIMIT $2 FOR UPDATE SKIP LOCKED)
UPDATE canonical_outbox o SET available_at=clock_timestamp()+$3::interval,claim_holder=$4,claim_epoch=$5
FROM picked WHERE o.operation_id=picked.operation_id RETURNING o.operation_id,o.message_id::text,o.namespace,o.position,o.payload::text`,
		source, limit, fmt.Sprintf("%f seconds", lease.Seconds()), token.Holder, token.Epoch)
	if e != nil {
		return nil, e
	}
	var claims []canonicalClaim
	for rows.Next() {
		var c canonicalClaim
		var payload string
		if e = rows.Scan(&c.Operation, &c.Message, &c.Namespace, &c.Position, &payload); e != nil {
			rows.Close()
			return nil, e
		}
		c.Payload = []byte(payload)
		claims = append(claims, c)
	}
	rows.Close()
	if e = rows.Err(); e != nil {
		return nil, e
	}
	if e = commit(ctx, tx, "canonical_claim"); e != nil {
		return nil, e
	}
	sortClaims(claims)
	return claims, nil
}

func sortClaims(claims []canonicalClaim) {
	// RETURNING order is not guaranteed; delivery order is namespace then position.
	sort.Slice(claims, func(x, y int) bool {
		if claims[x].Namespace != claims[y].Namespace {
			return claims[x].Namespace < claims[y].Namespace
		}
		return claims[x].Position < claims[y].Position
	})
}

// deliverClaims posts claims in order and returns how many were confirmed by the owner before the
// first failure.
func (s *Store) deliverClaims(ctx context.Context, client *http.Client, cfg WorkerConfig, claims []canonicalClaim) (int, error) {
	defer stage("canonical_delivery")()
	inputs := map[string]Input{}
	ids := make([]string, len(claims))
	for k, c := range claims {
		ids[k] = c.Message
	}
	if loaded, e := s.Inputs(ctx, ids); e == nil {
		inputs = loaded
	}
	limit := cfg.CanonicalSequence
	if limit == 0 {
		limit = platform.Int("CANONICAL_SEQUENCE_MAX", 50)
	}
	limit = min(max(1, limit), store.MaxCanonicalSequence)
	done := 0
	for done < len(claims) {
		// A run is consecutive claims of one namespace; claims are already in namespace and
		// position order, so each run is in predecessor order.
		end := done + 1
		for end < len(claims) && end-done < limit && claims[end].Namespace == claims[done].Namespace {
			end++
		}
		if end-done > 1 && !s.noSequence.Load() {
			run := claims[done:end]
			body := bytes.NewBufferString(`{"batches":[`)
			for k, c := range run {
				if k > 0 {
					body.WriteByte(',')
				}
				body.Write(c.Payload)
			}
			body.WriteString(`]}`)
			status, e := s.postOwner(ctx, client, cfg, "/internal/canonical/sequences", body.Bytes(), inputs[run[0].Message], "canonical sequence delivery")
			if e == nil {
				platform.Add("ingest_canonical_sequence_calls_total", 1)
				platform.Add("ingest_canonical_sequence_operations_total", float64(len(run)))
				done = end
				continue
			}
			if status == http.StatusNotFound {
				s.noSequence.Store(true)
			} else if status != http.StatusConflict {
				// Ambiguous or unavailable: the whole run is retried unchanged next cycle.
				return done, e
			}
			// A rejection (or an owner without the sequence API) is resolved one operation at a
			// time, so the exact rejected operation is found and everything before it completes.
		}
		for k := done; k < end; k++ {
			if _, e := s.postOwner(ctx, client, cfg, "/internal/canonical/batches", claims[k].Payload, inputs[claims[k].Message], "canonical batch delivery"); e != nil {
				return k, e
			}
		}
		done = end
	}
	return len(claims), nil
}

// postOwner sends one owner call and returns its HTTP status (0 when there was no response).
func (s *Store) postOwner(ctx context.Context, client *http.Client, cfg WorkerConfig, path string, payload []byte, i Input, name string) (int, error) {
	cctx, span := inputSpan(ctx, i, name)
	defer span.End()
	req, e := http.NewRequestWithContext(cctx, "POST", strings.TrimRight(cfg.CanonicalURL, "/")+path, bytes.NewReader(payload))
	if e != nil {
		return 0, e
	}
	req.Header.Set("Authorization", "Bearer "+cfg.CanonicalToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Correlation-ID", i.CorrelationID)
	platform.Inject(cctx, req.Header)
	sent := time.Now()
	response, e := client.Do(req)
	platform.Observe("ingest_canonical_owner_call_seconds", time.Since(sent).Seconds())
	if e != nil {
		return 0, e
	}
	_, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
	response.Body.Close()
	if readErr != nil {
		return response.StatusCode, readErr
	}
	if response.StatusCode != 200 {
		platform.Add(fmt.Sprintf("ingest_canonical_owner_rejections_total_%dxx", response.StatusCode/100), 1)
		return response.StatusCode, fmt.Errorf("canonical owner returned %d", response.StatusCode)
	}
	return 200, nil
}

// completeCanonical records owner-confirmed entries in one transaction. Claims that are no longer ours
// (the lease changed hands) are left alone; the new owner delivers them again unchanged.
func (s *Store) completeCanonical(ctx context.Context, confirmed []canonicalClaim) (int, error) {
	defer stage("canonical_complete")()
	if len(confirmed) == 0 {
		return 0, nil
	}
	token, ok := ownership.Current(ctx)
	if !ok {
		return 0, ownership.ErrFenced
	}
	operations := make([]string, len(confirmed))
	for k, c := range confirmed {
		operations[k] = c.Operation
	}
	tx, e := s.begin(ctx)
	if e != nil {
		return 0, e
	}
	defer tx.Rollback(ctx)
	if e = ownership.Guard(ctx, tx); e != nil {
		return 0, e
	}
	rows, e := tx.Query(ctx, `WITH gone AS (DELETE FROM canonical_outbox c USING unnest($1::text[]) AS d(id)
 WHERE c.operation_id=d.id AND c.claim_holder=$2 AND c.claim_epoch=$3 RETURNING c.message_id)
 UPDATE raw_inputs SET state='applied' WHERE message_id IN (SELECT message_id FROM gone) RETURNING source,namespace,received_at`, operations, token.Holder, token.Epoch)
	if e != nil {
		return 0, e
	}
	type frontier struct{ source, namespace string }
	seen := map[frontier]bool{}
	var received []time.Time
	for rows.Next() {
		var f frontier
		var at time.Time
		if e = rows.Scan(&f.source, &f.namespace, &at); e != nil {
			rows.Close()
			return 0, e
		}
		seen[f] = true
		received = append(received, at)
	}
	rows.Close()
	if e = rows.Err(); e != nil {
		return 0, e
	}
	for f := range seen {
		if e = frontiers(ctx, tx, f.source, f.namespace); e != nil {
			return 0, e
		}
	}
	if e = commit(ctx, tx, "canonical_complete"); e != nil {
		return 0, e
	}
	for _, at := range received {
		platform.Observe("canonical_apply_seconds", time.Since(at).Seconds())
	}
	if lost := len(confirmed) - len(received); lost > 0 {
		platform.Add("ingest_canonical_completion_lost_total", float64(lost))
	}
	return len(received), nil
}

// deliverCycle runs one claim, deliver, complete round and reports how many entries it claimed.
func (s *Store) deliverCycle(ctx context.Context, client *http.Client, cfg WorkerConfig, dc DeliveryConfig) (int, error) {
	dc = dc.normalized()
	claims, e := s.claimCanonical(ctx, canonicalSource(cfg), dc.Batch, dc.Lease)
	if e != nil || len(claims) == 0 {
		return 0, e
	}
	confirmed, deliverErr := s.deliverClaims(ctx, client, cfg, claims)
	completed, e := s.completeCanonical(ctx, claims[:confirmed])
	if e == nil && confirmed < len(claims) {
		// Release the unconfirmed tail so it is retried first and nothing jumps ahead of it.
		_, e = s.Pool.Exec(ctx, `UPDATE canonical_outbox SET available_at='-infinity',claim_holder=NULL,claim_epoch=NULL WHERE claim_holder=$1 AND operation_id=ANY($2)`, mustHolder(ctx), operationsOf(claims[confirmed:]))
	}
	batchStats("canonical", completed, 0)
	if e != nil {
		return len(claims), e
	}
	return len(claims), deliverErr
}

func mustHolder(ctx context.Context) string {
	token, _ := ownership.Current(ctx)
	return token.Holder
}

func operationsOf(claims []canonicalClaim) []string {
	out := make([]string, len(claims))
	for k, c := range claims {
		out[k] = c.Operation
	}
	return out
}

func (s *Store) RunCanonicalDelivery(ctx context.Context, cfg WorkerConfig) {
	client := platform.Client(3 * time.Second)
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	dc := DeliveryConfigFromEnv()
	for ctx.Err() == nil {
		_ = s.Leases.Run(ctx, "canonical-delivery:"+cfg.Kind, func(owned context.Context) error {
			if e := s.releaseCanonicalClaims(owned); e != nil {
				return e
			}
			for owned.Err() == nil {
				n, e := s.deliverCycle(owned, client, cfg, dc)
				if e != nil {
					// Free whatever this cycle still holds before the loop restarts.
					rctx, cancel := context.WithTimeout(context.WithoutCancel(owned), 5*time.Second)
					_ = s.releaseCanonicalClaimsFor(rctx, owned)
					cancel()
					return e
				}
				if n == 0 {
					Sleep(owned, 100*time.Millisecond)
				}
			}
			return owned.Err()
		})
		Sleep(ctx, time.Second)
	}
}

// releaseCanonicalClaimsFor releases using the ownership token of owned on a separate context.
func (s *Store) releaseCanonicalClaimsFor(ctx, owned context.Context) error {
	_, e := s.Pool.Exec(ctx, `UPDATE canonical_outbox SET available_at='-infinity',claim_holder=NULL,claim_epoch=NULL WHERE claim_holder=$1`, mustHolder(owned))
	return e
}
