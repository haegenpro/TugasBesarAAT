package store

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"aggregator/internal/acquisition"
	"aggregator/internal/canonical"
	"aggregator/internal/clients"
	"aggregator/internal/ownership"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestDispatchCompletedResponseReusedAfterTakeover(t *testing.T) {
	s, a := dispatchLab(t)
	old := dispatchOwner(t, a, "PVMBG")
	var count atomic.Int64
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { count.Add(1); _, _ = w.Write([]byte("saved")) }))
	defer source.Close()
	client, err := acquisition.Client(a, "PVMBG", source.URL, 0, 4096)
	require.NoError(t, err)
	_, err = dispatchGet(client, old, source.URL+"/volcanic-reports", "live:0")
	require.NoError(t, err)
	expireDispatchOwner(t, s, "PVMBG")
	next := dispatchOwner(t, a, "PVMBG")
	body, err := dispatchGet(client, next, source.URL+"/volcanic-reports", "live:0")
	require.NoError(t, err)
	require.Equal(t, "saved", string(body))
	require.EqualValues(t, 1, count.Load())
}

func TestDispatchSupersededNonceCannotOverwriteRecoveredReceipt(t *testing.T) {
	s, a := dispatchLab(t)
	oldCtx := dispatchOwner(t, a, "PVMBG")
	old, err := acquisition.Token(oldCtx, "PVMBG")
	require.NoError(t, err)
	request := acquisition.Dispatch{Owner: old, Lane: "/volcanic-reports", Key: string(bytes.Repeat([]byte("a"), 64)), Fingerprint: string(bytes.Repeat([]byte("b"), 64)), ID: uuid.NewString()}
	_, err = a.ReserveDispatch(oldCtx, request)
	require.NoError(t, err)
	require.NoError(t, a.InterruptDispatch(context.Background(), request, "transport"))
	expireDispatchOwner(t, s, "PVMBG")
	nextCtx := dispatchOwner(t, a, "PVMBG")
	next, err := acquisition.Token(nextCtx, "PVMBG")
	require.NoError(t, err)
	replacement := request
	replacement.ID = uuid.NewString()
	replacement.Owner = next
	var receipt acquisition.Receipt
	require.Eventually(t, func() bool { receipt, err = a.ReserveDispatch(nextCtx, replacement); return err == nil }, 3*time.Second, 100*time.Millisecond)
	require.True(t, receipt.Recovered)
	require.NoError(t, a.CompleteDispatch(context.Background(), replacement, acquisition.Receipt{Status: 200, Headers: http.Header{}, Body: []byte("replacement")}))
	require.ErrorIs(t, a.CompleteDispatch(context.Background(), request, acquisition.Receipt{Status: 200, Headers: http.Header{}, Body: []byte("obsolete")}), acquisition.ErrNotOwner)
	require.NoError(t, a.InterruptDispatch(context.Background(), request, "body"))
	var body []byte
	var state string
	require.NoError(t, s.Pool.QueryRow(context.Background(), `SELECT body,state FROM acquisition_dispatch WHERE scope='acquisition:PVMBG'`).Scan(&body, &state))
	require.Equal(t, "replacement", string(body))
	require.Equal(t, "complete", state)
}

func TestDispatchStreamingRecoversWithoutOldBodyClose(t *testing.T) {
	s, a := dispatchLab(t)
	_, err := s.Pool.Exec(context.Background(), `INSERT INTO canonical_ownership(scope,source)VALUES('acquisition:SATELIT','SATELIT')`)
	require.NoError(t, err)
	old := dispatchOwner(t, a, "SATELIT")
	var count atomic.Int64
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(":ready\n\n"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer source.Close()
	client, err := acquisition.Client(a, "SATELIT", source.URL, 0, 4096)
	require.NoError(t, err)
	request, err := http.NewRequestWithContext(acquisition.WithRequestKey(old, "stream:first"), "GET", source.URL+"/v1/stream?from=0", nil)
	require.NoError(t, err)
	oldResponse, err := client.Do(request)
	require.NoError(t, err)
	defer oldResponse.Body.Close()
	// The lease is expired deliberately; the dispatch grace period is real time.
	expireDispatchOwner(t, s, "SATELIT")
	next := dispatchOwner(t, a, "SATELIT")
	var response *http.Response
	started := time.Now()
	require.Eventually(t, func() bool {
		req, err := http.NewRequestWithContext(acquisition.WithRequestKey(next, "stream:resume"), "GET", source.URL+"/v1/stream?from=5", nil)
		if err != nil {
			return false
		}
		response, err = client.Do(req)
		return err == nil
	}, 15*time.Second, 250*time.Millisecond)
	defer response.Body.Close()
	require.Less(t, time.Since(started), 60*time.Second)
	require.EqualValues(t, 2, count.Load())
	require.ErrorIs(t, oldResponse.Body.Close(), acquisition.ErrNotOwner, "closing an obsolete stream must not close its replacement's ledger entry")
	var open bool
	require.NoError(t, s.Pool.QueryRow(context.Background(), `SELECT stream_open FROM acquisition_dispatch WHERE scope='acquisition:SATELIT'`).Scan(&open))
	require.True(t, open)
	require.NoError(t, response.Body.Close())
}

// An actual subprocess is killed after its source accepts a GET but before any
// durable response exists. This proof never edits lease timestamps and measures
// kill-to-first-committed-event using the normal 15-second production lease.
// Three replacement contenders exercise election rather than calling Acquire
// manually. It is a local process/HTTP/SQL proof, not four-node HA acceptance.
func TestDispatchProcessCrashTakeoverWithin60Seconds(t *testing.T) {
	if os.Getenv("AAT_DISPATCH_CRASH_CHILD") == "1" {
		ctx := context.Background()
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		db, err := New(ctx, os.Getenv("AAT_DISPATCH_CHILD_DSN"), logger)
		require.NoError(t, err)
		defer db.Close()
		authority, err := ownership.NewAcquisition(db.Pool, 15*time.Second, canonical.Deadline)
		require.NoError(t, err)
		coordinator, err := acquisition.New(authority, logger)
		require.NoError(t, err)
		client, err := acquisition.Client(authority, "BMKG", os.Getenv("AAT_DISPATCH_CHILD_SOURCE"), 0, 4096)
		require.NoError(t, err)
		coordinator.Run(ctx, "BMKG", func(owned context.Context) {
			_, _ = dispatchGet(client, owned, os.Getenv("AAT_DISPATCH_CHILD_SOURCE")+"/seismic-events", "live:0")
			<-owned.Done()
		})
		return
	}
	s, a := dispatchLab(t)
	event := clients.SeismicEvent{SourceSequence: 7, EventID: "crash-takeover-event", Magnitude: 5, DepthKM: 10, EpicenterLat: -6, EpicenterLon: 107, RegionName: "fixture", OccurredAt: time.Now().UTC()}
	var requests atomic.Int64
	accepted := make(chan struct{})
	var mu sync.Mutex
	ids := []string{}
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		ids = append(ids, r.Header.Get("X-Acquisition-Request-ID"))
		mu.Unlock()
		if requests.Add(1) == 1 {
			connection, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = connection.Close()
			}
			close(accepted)
			return
		}
		_ = json.NewEncoder(w).Encode(event)
	}))
	defer source.Close()
	binary, err := os.Executable()
	require.NoError(t, err)
	command := exec.Command(binary, "-test.run=^TestDispatchProcessCrashTakeoverWithin60Seconds$", "-test.timeout=90s")
	command.Env = append(os.Environ(), "AAT_DISPATCH_CRASH_CHILD=1", "AAT_DISPATCH_CHILD_DSN="+s.Pool.Config().ConnString(), "AAT_DISPATCH_CHILD_SOURCE="+source.URL)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	require.NoError(t, command.Start())
	waited := false
	defer func() {
		if !waited {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()
	select {
	case <-accepted:
	case <-time.After(6 * time.Second):
		t.Fatal("child never reached the upstream")
	}
	started := time.Now()
	require.NoError(t, command.Process.Kill())
	_ = command.Wait()
	waited = true
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	t.Cleanup(func() { cancel(); workers.Wait() })
	completed := make(chan error, 3)
	for range 3 {
		coordinator, err := acquisition.New(a, slog.New(slog.NewTextHandler(io.Discard, nil)))
		require.NoError(t, err)
		client, err := acquisition.Client(a, "BMKG", source.URL, 0, 4096)
		require.NoError(t, err)
		workers.Add(1)
		go func() {
			defer workers.Done()
			coordinator.Run(ctx, "BMKG", func(owned context.Context) {
				for owned.Err() == nil {
					body, err := dispatchGet(client, owned, source.URL+"/seismic-events", "live:0")
					if _, waiting := acquisition.WaitDelay(err); waiting {
						if !dispatchTestSleep(owned, acquisition.DispatchRetryInterval) {
							return
						}
						continue
					}
					if err != nil {
						completed <- err
						return
					}
					var received clients.SeismicEvent
					if err = json.Unmarshal(body, &received); err == nil {
						err = s.CommitSeismic(owned, []clients.SeismicEvent{received}, "takeover", 7)
					}
					if err == nil {
						err = s.CommitSeismic(owned, []clients.SeismicEvent{received}, "replay", 7)
					}
					if err == nil {
						err = s.RecordAttempt(owned, "BMKG", "takeover", true, time.Second, time.Minute)
					}
					completed <- err
					<-owned.Done()
					return
				}
			})
		}()
	}
	select {
	case err = <-completed:
		require.NoError(t, err)
	case <-time.After(60 * time.Second):
		t.Fatal("no successful protected acquisition within 60 seconds")
	}
	elapsed := time.Since(started)
	require.Less(t, elapsed, 60*time.Second)
	var count int
	require.NoError(t, s.Pool.QueryRow(context.Background(), `SELECT count(*) FROM hazard_events WHERE source_ref_id='crash-takeover-event'`).Scan(&count))
	require.Equal(t, 1, count)
	cursor, err := s.HistoryCursor(context.Background(), "BMKG", "seismic-events")
	require.NoError(t, err)
	require.EqualValues(t, 7, cursor)
	require.EqualValues(t, 2, requests.Load(), "one lost-response request and one recovery request, not one request per contender")
	mu.Lock()
	require.Len(t, ids, 2)
	require.NotEmpty(t, ids[0])
	require.Equal(t, ids[0], ids[1])
	mu.Unlock()
	select {
	case <-completed:
		t.Fatal("multiple replacement contenders made progress")
	case <-time.After(time.Second):
	}
	cancel()
	workers.Wait()
	measured, _ := json.Marshal(map[string]any{"elapsed_ms": elapsed.Milliseconds(), "upstream_http_requests": requests.Load(), "recovery_retries": 1, "hazard_events": count, "committed_cursor": cursor, "contenders": 3, "lease_ttl_ms": 15000, "lease_clock_modified": false})
	t.Log("DISPATCH_TAKEOVER_RESULT=" + string(measured))
}
func dispatchTestSleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
