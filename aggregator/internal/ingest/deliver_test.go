package ingest

import (
	"aggregator/internal/platform"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeOwner behaves like the canonical owner API for one namespace: a batch is applied only when
// its predecessor is the last applied position, and a repeat of an applied operation with the same
// content is answered 200 without applying it again.
type fakeOwner struct {
	mu       sync.Mutex
	server   *httptest.Server
	last     map[string]int64
	applied  map[string]string
	order    []string
	posts    map[string]int
	dropOnce map[string]bool // apply, then lose the response
	reject   bool
	// noSequences makes the owner answer 404 to the multi-operation call, like an older owner.
	noSequences bool
	sequences   int // multi-operation calls that committed
}

type fakeOp struct {
	Operation string `json:"operation_id"`
	Namespace string `json:"namespace"`
	Position  int64  `json:"position"`
	Previous  int64  `json:"previous_position"`
}

// check reports whether op can be applied on top of last/applied: ok, already applied (skip), or a conflict.
func (o *fakeOwner) check(op fakeOp, body []byte, last map[string]int64, applied map[string]string) (skip bool, ok bool) {
	if digest, done := applied[op.Operation]; done {
		return true, digest == Hash(body)
	}
	at, known := last[op.Namespace]
	if !known {
		at = op.Previous // the first operation of a namespace defines its start
	}
	return false, op.Previous == at
}

func newFakeOwner(t *testing.T) *fakeOwner {
	o := &fakeOwner{last: map[string]int64{}, applied: map[string]string{}, posts: map[string]int{}, dropOnce: map[string]bool{}}
	o.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var bodies []json.RawMessage
		if strings.HasSuffix(r.URL.Path, "/sequences") {
			if o.noSequences {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			var seq struct {
				Batches []json.RawMessage `json:"batches"`
			}
			_ = json.Unmarshal(body, &seq)
			bodies = seq.Batches
		} else {
			bodies = []json.RawMessage{body}
		}
		o.mu.Lock()
		defer o.mu.Unlock()
		ops := make([]fakeOp, len(bodies))
		for k, raw := range bodies {
			_ = json.Unmarshal(raw, &ops[k])
			o.posts[ops[k].Operation]++
		}
		if o.reject {
			w.WriteHeader(http.StatusConflict)
			return
		}
		// All or nothing, like one owner transaction: check every operation on a scratch copy first.
		last := map[string]int64{}
		for k, v := range o.last {
			last[k] = v
		}
		applied := map[string]string{}
		for k, v := range o.applied {
			applied[k] = v
		}
		for k, op := range ops {
			skip, ok := o.check(op, bodies[k], last, applied)
			if !ok {
				w.WriteHeader(http.StatusConflict)
				return
			}
			if !skip {
				last[op.Namespace] = op.Position
				applied[op.Operation] = Hash(bodies[k])
			}
		}
		drop := false
		for k, op := range ops {
			if _, done := o.applied[op.Operation]; done {
				continue
			}
			o.last[op.Namespace] = op.Position
			o.applied[op.Operation] = Hash(bodies[k])
			o.order = append(o.order, op.Operation)
			if o.dropOnce[op.Operation] {
				delete(o.dropOnce, op.Operation)
				drop = true
			}
		}
		if len(bodies) > 1 || strings.HasSuffix(r.URL.Path, "/sequences") {
			o.sequences++
		}
		if drop {
			hijacker, _ := w.(http.Hijacker)
			conn, _, _ := hijacker.Hijack()
			_ = conn.Close() // the owner committed, the caller never hears back
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(o.server.Close)
	return o
}

func (o *fakeOwner) appliedCount() int { o.mu.Lock(); defer o.mu.Unlock(); return len(o.applied) }

type deliveryFixture struct {
	s       *Store
	ns      string
	inputs  []Input
	owner   *fakeOwner
	cfg     WorkerConfig
	client  *http.Client
	outputs int
	base    int // inputs that were already applied because they produced no output
}

func newDelivery(t *testing.T) *deliveryFixture {
	s := testStore(t)
	ns, inputs := fireFixture(t, s, "mixed-merge-split.jsonl")
	runChunks(t, s, ns, inputs, 3)
	owner := newFakeOwner(t)
	f := &deliveryFixture{s: s, ns: ns, inputs: inputs, owner: owner, client: &http.Client{Timeout: 2 * time.Second},
		cfg: WorkerConfig{Kind: "wildfire", CanonicalURL: owner.server.URL, CanonicalToken: "t", CanonicalSequence: 1}}
	require.NoError(t, s.Pool.QueryRow(context.Background(), `SELECT count(*) FROM canonical_outbox WHERE namespace LIKE $1||'%'`, ns).Scan(&f.outputs))
	require.Greater(t, f.outputs, 3)
	_, _, _, states := f.state(t)
	f.base = states["applied"]
	return f
}

func (f *deliveryFixture) own(t *testing.T, s *Store, fn func(context.Context)) {
	t.Helper()
	require.NoError(t, s.Leases.Run(context.Background(), "canonical-delivery:wildfire", func(c context.Context) error { fn(c); return nil }))
}

func (f *deliveryFixture) state(t *testing.T) (outbox int, applied, firstPrepared int64, states map[string]int) {
	ctx := context.Background()
	require.NoError(t, f.s.Pool.QueryRow(ctx, `SELECT count(*) FROM canonical_outbox WHERE namespace LIKE $1||'%'`, f.ns).Scan(&outbox))
	require.NoError(t, f.s.Pool.QueryRow(ctx, `SELECT applied FROM source_progress WHERE source='SATELIT' AND namespace=$1`, f.ns).Scan(&applied))
	require.NoError(t, f.s.Pool.QueryRow(ctx, `SELECT coalesce(min(position),-1) FROM raw_inputs WHERE namespace=$1 AND state IN ('pending','prepared')`, f.ns).Scan(&firstPrepared))
	states = map[string]int{}
	rows, e := f.s.Pool.Query(ctx, `SELECT state,count(*) FROM raw_inputs WHERE namespace=$1 GROUP BY 1`, f.ns)
	require.NoError(t, e)
	defer rows.Close()
	for rows.Next() {
		var state string
		var n int
		require.NoError(t, rows.Scan(&state, &n))
		states[state] = n
	}
	return
}

func TestM2CanonicalDeliveryIsOrderedBatchedAndAdvancesTheAppliedFrontierContiguously(t *testing.T) {
	f := newDelivery(t)
	f.own(t, f.s, func(c context.Context) {
		n, e := f.s.deliverCycle(c, f.client, f.cfg, DeliveryConfig{Batch: 2})
		require.NoError(t, e)
		require.Equal(t, 2, n)
		outbox, applied, firstOpen, states := f.state(t)
		require.Equal(t, f.outputs-2, outbox)
		require.Equal(t, f.base+2, states["applied"])
		require.EqualValues(t, firstOpen-1, applied, "applied never passes an input that is not applied")
		for {
			n, e := f.s.deliverCycle(c, f.client, f.cfg, DeliveryConfig{Batch: 2})
			require.NoError(t, e)
			if n == 0 {
				break
			}
		}
	})
	outbox, applied, _, states := f.state(t)
	require.Zero(t, outbox)
	require.Zero(t, states["prepared"])
	require.EqualValues(t, f.inputs[len(f.inputs)-1].Position, applied)
	require.Equal(t, f.outputs, f.owner.appliedCount())
	require.Len(t, f.owner.order, f.outputs)
	for _, posts := range f.owner.posts {
		require.Equal(t, 1, posts, "no operation is sent twice when nothing went wrong")
	}
}

func TestM2CanonicalDeliveryRetriesALostResponseWithTheOriginalOperation(t *testing.T) {
	f := newDelivery(t)
	var operations []string
	rows, e := f.s.Pool.Query(context.Background(), `SELECT operation_id FROM canonical_outbox WHERE namespace LIKE $1||'%' ORDER BY position`, f.ns)
	require.NoError(t, e)
	for rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		operations = append(operations, id)
	}
	rows.Close()
	f.owner.dropOnce[operations[2]] = true // the owner commits the third, the response is lost
	f.own(t, f.s, func(c context.Context) {
		n, e := f.s.deliverCycle(c, f.client, f.cfg, DeliveryConfig{Batch: 50})
		require.Equal(t, f.outputs, n)
		require.Error(t, e, "an ambiguous outcome is a failed cycle")
		outbox, _, _, states := f.state(t)
		require.Equal(t, f.outputs-2, outbox, "only the two confirmed entries are completed")
		require.Equal(t, f.base+2, states["applied"])
		claimed := 0
		require.NoError(t, f.s.Pool.QueryRow(context.Background(), `SELECT count(*) FROM canonical_outbox WHERE claim_holder IS NOT NULL`).Scan(&claimed))
		require.Zero(t, claimed, "unconfirmed entries are released at once")
		n, e = f.s.deliverCycle(c, f.client, f.cfg, DeliveryConfig{Batch: 50})
		require.NoError(t, e)
		require.Equal(t, f.outputs-2, n)
	})
	outbox, _, _, _ := f.state(t)
	require.Zero(t, outbox)
	require.Equal(t, f.outputs, f.owner.appliedCount(), "the owner applied each operation exactly once")
	require.Equal(t, 2, f.owner.posts[operations[2]], "the lost one was re-sent with the same identity and content")
}

func TestM2CanonicalDeliveryPredecessorRejectionCompletesNothingAndRecovers(t *testing.T) {
	f := newDelivery(t)
	f.owner.reject = true
	f.own(t, f.s, func(c context.Context) {
		n, e := f.s.deliverCycle(c, f.client, f.cfg, DeliveryConfig{Batch: 10})
		require.Error(t, e)
		require.Equal(t, f.outputs, n, "everything was claimed and nothing was confirmed")
		outbox, applied, _, states := f.state(t)
		require.Equal(t, f.outputs, outbox)
		require.Equal(t, f.base, states["applied"], "no output was completed")
		_ = applied
		f.owner.mu.Lock()
		f.owner.reject = false
		f.owner.mu.Unlock()
		for {
			n, e := f.s.deliverCycle(c, f.client, f.cfg, DeliveryConfig{Batch: 10})
			require.NoError(t, e)
			if n == 0 {
				break
			}
		}
	})
	outbox, _, _, _ := f.state(t)
	require.Zero(t, outbox)
}

func TestM2CanonicalCrashAfterOwnerCommitIsRepeatedIdempotently(t *testing.T) {
	f := newDelivery(t)
	// The first incarnation delivers everything, then dies before it can record any of it.
	f.own(t, f.s, func(c context.Context) {
		claims, e := f.s.claimCanonical(c, "SATELIT", 100, time.Hour)
		require.NoError(t, e)
		confirmed, e := f.s.deliverClaims(c, f.client, f.cfg, claims)
		require.NoError(t, e)
		require.Equal(t, f.outputs, confirmed)
	})
	require.Equal(t, f.outputs, f.owner.appliedCount())
	expireLeaseScope(t, f.s, "canonical-delivery:wildfire")
	next := otherOwner(f.s)
	f.own(t, next, func(c context.Context) {
		n, e := next.deliverCycle(c, f.client, f.cfg, DeliveryConfig{Batch: 100})
		require.NoError(t, e, "the new incarnation takes over claims that are still marked with the dead one")
		require.Equal(t, f.outputs, n)
	})
	outbox, _, _, states := f.state(t)
	require.Zero(t, outbox)
	require.Zero(t, states["prepared"])
	require.Equal(t, f.outputs, f.owner.appliedCount(), "the owner did not apply anything twice")
}

func TestM2CanonicalStaleClaimIsFencedAtCompletion(t *testing.T) {
	f := newDelivery(t)
	stalled := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- f.s.Leases.Run(context.Background(), "canonical-delivery:wildfire", func(c context.Context) error {
			claims, e := f.s.claimCanonical(c, "SATELIT", 100, time.Hour)
			if e != nil {
				return e
			}
			if _, e = f.s.deliverClaims(c, f.client, f.cfg, claims); e != nil {
				return e
			}
			close(stalled)
			<-release // paused after the owner confirmed, before completing
			_, e = f.s.completeCanonical(c, claims)
			return e
		})
	}()
	<-stalled
	expireLeaseScope(t, f.s, "canonical-delivery:wildfire")
	next := otherOwner(f.s)
	f.own(t, next, func(c context.Context) {
		n, e := next.deliverCycle(c, f.client, f.cfg, DeliveryConfig{Batch: 100})
		require.NoError(t, e)
		require.Equal(t, f.outputs, n)
	})
	close(release)
	select {
	case e := <-done:
		require.Error(t, e, "the stale owner cannot complete anything")
	case <-time.After(5 * time.Second):
		t.Fatal("stale owner did not return")
	}
	outbox, _, _, states := f.state(t)
	require.Zero(t, outbox)
	require.Zero(t, states["prepared"])
}

func TestM2CanonicalClaimsAreExclusiveAndExpire(t *testing.T) {
	f := newDelivery(t)
	f.own(t, f.s, func(c context.Context) {
		first, e := f.s.claimCanonical(c, "SATELIT", 3, 250*time.Millisecond)
		require.NoError(t, e)
		require.Len(t, first, 3)
		require.Less(t, first[0].Position, first[1].Position)
		next, e := f.s.claimCanonical(c, "SATELIT", 3, time.Minute)
		require.NoError(t, e)
		require.Len(t, next, 3)
		require.Greater(t, next[0].Position, first[2].Position, "a claimed entry is not claimed again while its claim lives")
		time.Sleep(400 * time.Millisecond)
		again, e := f.s.claimCanonical(c, "SATELIT", 3, time.Minute)
		require.NoError(t, e)
		require.Equal(t, first[0].Operation, again[0].Operation, "an expired claim is recovered, in order")
	})
}

func TestM2DeliveryMetricsAreExported(t *testing.T) {
	f := newDelivery(t)
	f.own(t, f.s, func(c context.Context) {
		_, e := f.s.deliverCycle(c, f.client, f.cfg, DeliveryConfig{Batch: 4})
		require.NoError(t, e)
	})
	out := scrape()
	for _, name := range []string{"app_ingest_stage_seconds_canonical_claim_count", "app_ingest_stage_seconds_canonical_complete_count",
		"app_ingest_commit_seconds_canonical_complete_count", "app_ingest_canonical_owner_call_seconds_count", "app_ingest_batches_total_canonical "} {
		require.Contains(t, out, name)
	}
	_ = platform.Duration
}

func expireLeaseScope(t *testing.T, s *Store, scope string) {
	_, e := s.Pool.Exec(context.Background(), `UPDATE source_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE scope=$1`, scope)
	require.NoError(t, e)
}

func (f *deliveryFixture) operations(t *testing.T) []string {
	var out []string
	rows, e := f.s.Pool.Query(context.Background(), `SELECT operation_id FROM canonical_outbox WHERE namespace LIKE $1||'%' ORDER BY position`, f.ns)
	require.NoError(t, e)
	defer rows.Close()
	for rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		out = append(out, id)
	}
	return out
}

func (f *deliveryFixture) drain(t *testing.T, s *Store, batch int) {
	f.own(t, s, func(c context.Context) {
		for {
			n, e := s.deliverCycle(c, f.client, f.cfg, DeliveryConfig{Batch: batch})
			require.NoError(t, e)
			if n == 0 {
				return
			}
		}
	})
}

func TestM2CanonicalSequencesDeliverInOrderWithFewOwnerCalls(t *testing.T) {
	f := newDelivery(t)
	f.cfg.CanonicalSequence = 4
	f.drain(t, f.s, 50)
	outbox, applied, _, states := f.state(t)
	require.Zero(t, outbox)
	require.Zero(t, states["prepared"])
	require.EqualValues(t, f.inputs[len(f.inputs)-1].Position, applied)
	require.Equal(t, f.outputs, f.owner.appliedCount())
	require.Len(t, f.owner.order, f.outputs)
	require.Equal(t, (f.outputs+3)/4, f.owner.sequences, "runs of at most four operations per owner call")
	for _, posts := range f.owner.posts {
		require.Equal(t, 1, posts)
	}
}

func TestM2CanonicalSequenceLostResponseIsResentUnchangedAndAppliedOnce(t *testing.T) {
	f := newDelivery(t)
	f.cfg.CanonicalSequence = 50
	operations := f.operations(t)
	f.owner.dropOnce[operations[2]] = true // the owner commits the whole sequence; the response is lost
	f.own(t, f.s, func(c context.Context) {
		_, e := f.s.deliverCycle(c, f.client, f.cfg, DeliveryConfig{Batch: 50})
		require.Error(t, e)
		outbox, _, _, states := f.state(t)
		require.Equal(t, f.outputs, outbox, "nothing is completed locally without the owner's answer")
		require.Equal(t, f.base, states["applied"])
	})
	f.drain(t, f.s, 50)
	outbox, _, _, _ := f.state(t)
	require.Zero(t, outbox)
	require.Equal(t, f.outputs, f.owner.appliedCount(), "the owner applied each operation exactly once")
	require.Len(t, f.owner.order, f.outputs)
	require.Equal(t, 2, f.owner.posts[operations[0]], "the same identities and content were sent again")
}

func TestM2CanonicalSequenceRejectionFallsBackAndCompletesOnlyThePrefix(t *testing.T) {
	f := newDelivery(t)
	f.cfg.CanonicalSequence = 50
	operations := f.operations(t)
	f.owner.applied[operations[3]] = "different content" // the fourth reuses an identity: a real rejection
	f.own(t, f.s, func(c context.Context) {
		_, e := f.s.deliverCycle(c, f.client, f.cfg, DeliveryConfig{Batch: 50})
		require.Error(t, e)
	})
	outbox, applied, firstOpen, states := f.state(t)
	require.Equal(t, f.outputs-3, outbox, "the three operations before the rejected one complete")
	require.Equal(t, f.base+3, states["applied"])
	require.EqualValues(t, firstOpen-1, applied)
	require.Equal(t, 1, f.owner.posts[operations[4]], "nothing after the rejected operation is sent alone")
}

func TestM2CanonicalSequenceFallsBackToSingleCallsForAnOlderOwner(t *testing.T) {
	f := newDelivery(t)
	f.cfg.CanonicalSequence = 50
	f.owner.noSequences = true
	f.drain(t, f.s, 50)
	outbox, _, _, _ := f.state(t)
	require.Zero(t, outbox)
	require.Equal(t, f.outputs, f.owner.appliedCount())
	require.Zero(t, f.owner.sequences)
	require.True(t, f.s.noSequence.Load())
}
