package ingest

import (
	"aggregator/internal/satellite"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// feed is a fake satellite service: it serves offsets from the requested one,
// optionally disconnects after a number of events, and otherwise holds the
// connection open like a live feed that is waiting for its next event.
type feed struct {
	mu        sync.Mutex
	events    []satellite.Event
	requested []int64
	stopAfter int
	server    *httptest.Server
}

func newFeed(t *testing.T, n int) *feed {
	f := &feed{}
	for pos := 0; pos < n; pos++ {
		f.events = append(f.events, satellite.Event{ID: strconv.Itoa(pos), Type: "detection", Data: []byte(fmt.Sprintf(
			`{"offset":%d,"detection":{"satellite":"N21","acq_date":"2026-11-18","acq_time":%d,"latitude":0,"longitude":%s,"confidence":"n","frp":2}}`, pos, pos%2300, strconv.FormatFloat(float64(pos)*0.5, 'f', -1, 64)))})
	}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		from, _ := strconv.Atoi(r.URL.Query().Get("from"))
		f.mu.Lock()
		f.requested = append(f.requested, int64(from))
		limit := f.stopAfter
		f.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		sent := 0
		for _, e := range f.events {
			if e.ID == "" {
				continue
			}
			pos, _ := strconv.Atoi(e.ID)
			if pos < from {
				continue
			}
			if limit > 0 && sent >= limit {
				return
			}
			fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", e.ID, e.Type, e.Data)
			w.(http.Flusher).Flush()
			sent++
		}
		<-r.Context().Done()
	}))
	t.Cleanup(f.server.Close)
	return f
}

func (f *feed) config(ns string, batch BatchLimits) SatelliteConfig {
	return SatelliteConfig{URL: f.server.URL, Token: "t", Namespace: ns, Initial: 0, Batch: batch}
}

func runSession(t *testing.T, s *Store, cfg SatelliteConfig) (cancel func(), done chan error) {
	ctx, stop := context.WithCancel(context.Background())
	done = make(chan error, 1)
	go func() {
		done <- s.Leases.Run(ctx, "acquire:SATELIT:"+cfg.Namespace, func(c context.Context) error { return s.satelliteSession(c, cfg) })
	}()
	return stop, done
}

func durable(t *testing.T, s *Store, ns string) (received int64, rows, outbox int) {
	// One statement, so the three figures come from the same snapshot even while batches commit.
	require.NoError(t, s.Pool.QueryRow(context.Background(), `SELECT
 coalesce((SELECT received FROM source_progress WHERE source='SATELIT' AND namespace=$1),-1),
 (SELECT count(*) FROM raw_inputs WHERE source='SATELIT' AND namespace=$1),
 (SELECT count(*) FROM raw_outbox o JOIN raw_inputs i USING(message_id) WHERE i.namespace=$1)`, ns).Scan(&received, &rows, &outbox))
	return
}

func requireContiguous(t *testing.T, s *Store, ns string) {
	var minimum, maximum, distinct int
	require.NoError(t, s.Pool.QueryRow(context.Background(), `SELECT coalesce(min(position),0),coalesce(max(position),-1),count(DISTINCT position) FROM raw_inputs WHERE source='SATELIT' AND namespace=$1`, ns).Scan(&minimum, &maximum, &distinct))
	require.Equal(t, 0, minimum)
	require.Equal(t, distinct-1, maximum, "stored positions must have no gap")
}

func TestM2SatelliteTimerFlushesWhileTheReaderWaitsForTheNextEvent(t *testing.T) {
	s := testStore(t)
	ns := "timer:" + uuid.NewString()
	f := newFeed(t, 5)
	stop, done := runSession(t, s, f.config(ns, BatchLimits{Records: 100, Bytes: 1 << 20, Wait: 150 * time.Millisecond}))
	defer stop()
	require.Eventually(t, func() bool { r, rows, _ := durable(t, s, ns); return r == 4 && rows == 5 }, 3*time.Second, 25*time.Millisecond,
		"five records below every size limit must still become durable once the wait elapses, while the feed stays silent")
	select {
	case e := <-done:
		t.Fatalf("session ended although the feed is still open: %v", e)
	default:
	}
}

func TestM2SatelliteBatchesStopAtCountAndBytesAndKeepOutboxInStep(t *testing.T) {
	s := testStore(t)
	ns := "limits:" + uuid.NewString()
	f := newFeed(t, 25)
	stop, _ := runSession(t, s, f.config(ns, BatchLimits{Records: 10, Bytes: 1 << 20, Wait: time.Hour}))
	// Two full batches commit at once; the remaining five wait for a limit that never comes.
	require.Eventually(t, func() bool { r, _, _ := durable(t, s, ns); return r == 19 }, 3*time.Second, 25*time.Millisecond)
	time.Sleep(300 * time.Millisecond)
	received, rows, outbox := durable(t, s, ns)
	require.EqualValues(t, 19, received)
	require.Equal(t, 20, rows)
	require.Equal(t, 20, outbox, "every durable record has its outbox entry in the same commit")
	stop()

	ns = "bytes:" + uuid.NewString()
	stop, _ = runSession(t, s, f.config(ns, BatchLimits{Records: 1000, Bytes: 600, Wait: time.Hour}))
	defer stop()
	require.Eventually(t, func() bool { r, _, _ := durable(t, s, ns); return r >= 3 }, 3*time.Second, 25*time.Millisecond, "the byte limit alone must flush")
	received, rows, outbox = durable(t, s, ns)
	require.Equal(t, int(received)+1, rows)
	require.Equal(t, rows, outbox)
	requireContiguous(t, s, ns)
}

func TestM2SatelliteInterruptionResumesFromDurableProgressWithoutLossOrDuplicates(t *testing.T) {
	s := testStore(t)
	ns := "resume:" + uuid.NewString()
	f := newFeed(t, 60)
	f.stopAfter = 23 // the feed disconnects in the middle of a batch
	limits := BatchLimits{Records: 10, Bytes: 1 << 20, Wait: 100 * time.Millisecond}
	stop, done := runSession(t, s, f.config(ns, limits))
	select {
	case e := <-done:
		require.Error(t, e, "a dropped connection ends the session with an error so the supervisor reconnects")
	case <-time.After(5 * time.Second):
		t.Fatal("session did not end after the disconnect")
	}
	stop()
	received, rows, _ := durable(t, s, ns)
	require.EqualValues(t, 22, received, "records buffered when the connection dropped are flushed before the session ends")
	require.Equal(t, 23, rows)

	f.mu.Lock()
	f.stopAfter = 0
	f.mu.Unlock()
	stop, _ = runSession(t, s, f.config(ns, limits))
	defer stop()
	require.Eventually(t, func() bool { r, _, _ := durable(t, s, ns); return r == 59 }, 5*time.Second, 25*time.Millisecond)
	_, rows, outbox := durable(t, s, ns)
	require.Equal(t, 60, rows)
	require.Equal(t, 60, outbox)
	requireContiguous(t, s, ns)
	f.mu.Lock()
	require.Equal(t, []int64{0, 23}, f.requested, "the second connection resumes from the durable position, not from the reader position")
	f.mu.Unlock()
}

func TestM2SatelliteShutdownMidStreamLeavesOnlyCommittedWholeBatches(t *testing.T) {
	s := testStore(t)
	ns := "shutdown:" + uuid.NewString()
	f := newFeed(t, 45)
	stop, done := runSession(t, s, f.config(ns, BatchLimits{Records: 20, Bytes: 1 << 20, Wait: time.Hour}))
	require.Eventually(t, func() bool { r, _, _ := durable(t, s, ns); return r == 39 }, 3*time.Second, 25*time.Millisecond)
	stop()
	<-done
	received, rows, outbox := durable(t, s, ns)
	require.EqualValues(t, 39, received, "the unfinished batch of five is not half-written")
	require.Equal(t, 40, rows)
	require.Equal(t, 40, outbox)
	requireContiguous(t, s, ns)
}

func TestM2AppendBatchIsAtomicIdempotentAndRejectsGapsAndConflicts(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	ns := "batch:" + uuid.NewString()
	mk := func(from, to int) []Input {
		var out []Input
		for p := from; p <= to; p++ {
			out = append(out, Input{Source: "SATELIT", Namespace: ns, Position: int64(p), Payload: []byte(fmt.Sprintf(`{"p":%d}`, p)), Metadata: map[string]any{"event_type": "detection"}, CorrelationID: "c"})
		}
		return out
	}
	run := func(inputs []Input) error {
		return s.Leases.Run(ctx, "acquire:"+ns, func(c context.Context) error { return s.AppendBatch(c, inputs, 0, "raw.satellite") })
	}
	require.NoError(t, run(mk(0, 9)))
	received, rows, outbox := durable(t, s, ns)
	require.EqualValues(t, 9, received)
	require.Equal(t, 10, rows)
	require.Equal(t, 10, outbox)

	// A retry after an ambiguous commit repeats the same records: nothing changes.
	require.NoError(t, run(mk(0, 9)))
	// An overlapping retry stores only the new tail.
	require.NoError(t, run(mk(5, 14)))
	received, rows, outbox = durable(t, s, ns)
	require.EqualValues(t, 14, received)
	require.Equal(t, 15, rows)
	require.Equal(t, 15, outbox)

	// A gap, inside the batch or before it, stores nothing.
	require.ErrorIs(t, run(mk(20, 25)), ErrGap)
	broken := mk(15, 20)
	broken[3].Position = 30
	require.ErrorIs(t, run(broken), ErrGap)
	// Same position with different content is a conflict and changes nothing.
	changed := mk(10, 12)
	changed[1].Payload = []byte(`{"p":"other"}`)
	require.ErrorIs(t, run(changed), ErrConflict)
	received, rows, _ = durable(t, s, ns)
	require.EqualValues(t, 14, received)
	require.Equal(t, 15, rows)

	// A failure in the middle of the write rolls the whole batch back: the third
	// record reuses the message id of a record of another namespace.
	other := "other:" + uuid.NewString()
	taken := uuid.NewString()
	require.NoError(t, s.Leases.Run(ctx, "acquire:"+other, func(c context.Context) error {
		return s.AppendBatch(c, []Input{{ID: taken, Source: "SATELIT", Namespace: other, Position: 0, Payload: []byte("x"), Metadata: map[string]any{}, CorrelationID: "c"}}, 0, "raw.satellite")
	}))
	failing := mk(15, 19)
	failing[2].ID = taken
	require.Error(t, run(failing))
	received, rows, outbox = durable(t, s, ns)
	require.EqualValues(t, 14, received, "the frontier does not move when the batch fails")
	require.Equal(t, 15, rows)
	require.Equal(t, 15, outbox)
	require.NoError(t, run(mk(15, 19)), "the same records succeed once the cause is gone")
	requireContiguous(t, s, ns)

	// Without a valid ownership token nothing is written.
	require.Error(t, s.AppendBatch(ctx, mk(20, 21), 0, "raw.satellite"))
	_, rows, _ = durable(t, s, ns)
	require.Equal(t, 20, rows)
}

func TestM2AppendBatchResolvesRetainedResponseFailuresAndRecordsBatchMetrics(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	ns := "resolve:" + uuid.NewString()
	require.NoError(t, s.Leases.Run(ctx, "acquire:"+ns, func(c context.Context) error {
		if e := s.RetainResponse(c, "SATELIT", ns, "stream:3", strings.NewReader("broken frame"), "oversized"); e != nil {
			return e
		}
		var inputs []Input
		for p := 0; p < 6; p++ {
			inputs = append(inputs, Input{Source: "SATELIT", Namespace: ns, Position: int64(p), Payload: []byte("{}"), Metadata: map[string]any{}, CorrelationID: "c"})
		}
		return s.AppendBatch(c, inputs, 0, "raw.satellite")
	}))
	var unresolved int
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM response_failures WHERE namespace=$1 AND NOT resolved`, ns).Scan(&unresolved))
	require.Zero(t, unresolved, "re-acquiring the position resolves its retained failure")
	out := scrape()
	require.Contains(t, out, "app_ingest_batch_records_total_append")
}

func TestM2SatelliteFeedThatNeverAnswersIsCutAtTheIdleLimit(t *testing.T) {
	s := testStore(t)
	t.Setenv("SATELLITE_IDLE_TIMEOUT_MS", "300")
	// Accepts the request and never sends response headers, like a stuck proxy.
	silent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	t.Cleanup(silent.Close)
	ns := "sat:" + uuid.NewString()
	started := time.Now()
	stop, done := runSession(t, s, SatelliteConfig{URL: silent.URL, Token: "t", Namespace: ns, Batch: BatchLimits{Records: 10}})
	defer stop()
	select {
	case e := <-done:
		require.Error(t, e)
		require.Less(t, time.Since(started), 3*time.Second, "waiting for headers is bounded by the idle limit")
	case <-time.After(5 * time.Second):
		t.Fatal("session still waiting for response headers")
	}
	received, rows, _ := durable(t, s, ns)
	require.EqualValues(t, -1, received)
	require.Zero(t, rows)
}

func TestM2SatelliteRefusedConnectionReturnsItsStatusWithoutTouchingProgress(t *testing.T) {
	s := testStore(t)
	refusing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(refusing.Close)
	ns := "sat:" + uuid.NewString()
	stop, done := runSession(t, s, SatelliteConfig{URL: refusing.URL, Token: "t", Namespace: ns, Batch: BatchLimits{Records: 10}})
	defer stop()
	e := <-done
	var status feedStatusError
	require.ErrorAs(t, e, &status)
	require.Equal(t, http.StatusServiceUnavailable, status.code)
	received, rows, _ := durable(t, s, ns)
	require.EqualValues(t, -1, received)
	require.Zero(t, rows)
}

func TestSatelliteRetryDelayBacksOffOnlyForRefusals(t *testing.T) {
	t.Setenv("SATELLITE_RETRY_MAX_MS", "10000")
	for _, transient := range []error{nil, feedStatusError{503}, feedStatusError{500}, context.Canceled, fmt.Errorf("connection reset")} {
		delay, refused := satelliteRetryDelay(transient, 4)
		require.Equal(t, time.Second, delay, "%v reconnects quickly", transient)
		require.Zero(t, refused, "%v resets the refusal count", transient)
	}
	refused := 0
	var delays []time.Duration
	for range 5 {
		var delay time.Duration
		delay, refused = satelliteRetryDelay(feedStatusError{429}, refused)
		delays = append(delays, delay)
	}
	require.Equal(t, []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second, 10 * time.Second}, delays)
	delay, _ := satelliteRetryDelay(feedStatusError{401}, 0)
	require.Equal(t, 2*time.Second, delay)
	delay, refused = satelliteRetryDelay(feedStatusError{503}, refused)
	require.Equal(t, time.Second, delay)
	require.Zero(t, refused)
}
