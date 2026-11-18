package store

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"aggregator/internal/acquisition"
	"aggregator/internal/canonical"
	"aggregator/internal/ownership"
	"github.com/stretchr/testify/require"
)

type dispatchPause struct {
	*ownership.Authority
	before, after func()
}

func (p dispatchPause) ReserveDispatch(ctx context.Context, d acquisition.Dispatch) (acquisition.Receipt, error) {
	if p.before != nil {
		p.before()
	}
	r, err := p.Authority.ReserveDispatch(ctx, d)
	if err == nil && p.after != nil {
		p.after()
	}
	return r, err
}
func dispatchLab(t *testing.T) (*Store, *ownership.Authority) {
	t.Helper()
	dsn, admin := m2Database(t, false)
	m2Schema(t, admin, "postgres", false)
	s := m2Store(t, dsn)
	a, err := ownership.NewAcquisition(s.Pool, 15*time.Second, canonical.Deadline)
	require.NoError(t, err)
	s.Acquisition = a
	return s, a
}
func dispatchOwner(t *testing.T, a *ownership.Authority, source string) context.Context {
	t.Helper()
	c, err := acquisition.New(a, slog.Default())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan context.Context, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx, source, func(ctx context.Context) { ready <- ctx; <-ctx.Done() })
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("owner failed to drain")
		}
	})
	select {
	case owned := <-ready:
		return owned
	case <-time.After(4 * time.Second):
		t.Fatal("source owner did not start")
	}
	return nil
}
func expireDispatchOwner(t *testing.T, s *Store, source string) {
	t.Helper()
	_, err := s.Pool.Exec(context.Background(), `UPDATE canonical_ownership SET expires_at=clock_timestamp()-interval '1 second' WHERE scope=$1`, "acquisition:"+source)
	require.NoError(t, err)
}
func dispatchGet(client *http.Client, ctx context.Context, target, key string) ([]byte, error) {
	req, err := http.NewRequestWithContext(acquisition.WithRequestKey(ctx, key), "GET", target, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}
func TestDispatchFreezeAfterPermitReusesReceiptAcrossTakeover(t *testing.T) {
	s, a := dispatchLab(t)
	old := dispatchOwner(t, a, "PVMBG")
	var requests atomic.Int64
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"snapshot":"first"}`))
	}))
	defer source.Close()
	paused, resume := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-resume:
		default:
			close(resume)
		}
	}()
	var once atomic.Bool
	gate := dispatchPause{Authority: a, after: func() {
		if once.CompareAndSwap(false, true) {
			close(paused)
			<-resume
		}
	}}
	first, err := acquisition.Client(gate, "PVMBG", source.URL, 0, 4096)
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() {
		_, err := dispatchGet(first, old, source.URL+"/volcanic-reports", "live:initial")
		result <- err
	}()
	select {
	case <-paused:
	case <-time.After(3 * time.Second):
		t.Fatal("dispatch was not paused after its committed permit")
	}
	expireDispatchOwner(t, s, "PVMBG")
	next := dispatchOwner(t, a, "PVMBG")
	second, err := acquisition.Client(a, "PVMBG", source.URL, 0, 4096)
	require.NoError(t, err)
	_, err = dispatchGet(second, next, source.URL+"/volcanic-reports", "live:initial")
	require.ErrorIs(t, err, acquisition.ErrDispatchPending)
	_, err = dispatchGet(second, next, source.URL+"/history?entity=volcanic-reports&after=0", "history:initial")
	require.ErrorIs(t, err, acquisition.ErrDispatchPending, "different lanes must not bypass an old sender")
	require.EqualValues(t, 0, requests.Load())
	close(resume)
	select {
	case err = <-result:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("resumed request did not finish")
	}
	require.EqualValues(t, 1, requests.Load())
	// Old ownership can preserve its HTTP receipt but still cannot commit data.
	require.Error(t, s.CommitVolcanic(old, nil, nil, "stale", 0))
	body, err := dispatchGet(second, next, source.URL+"/volcanic-reports", "live:initial")
	require.NoError(t, err)
	require.JSONEq(t, `{"snapshot":"first"}`, string(body))
	require.EqualValues(t, 1, requests.Load(), "takeover must read the receipt, not fetch it again")
	_, err = dispatchGet(first, old, source.URL+"/volcanic-reports", "live:later")
	require.Error(t, err)
	require.EqualValues(t, 1, requests.Load())
	// A new durable polling generation legitimately opens a new request.
	_, err = dispatchGet(second, next, source.URL+"/volcanic-reports", "live:committed-next")
	require.NoError(t, err)
	require.EqualValues(t, 2, requests.Load())
}
func TestDispatchFreezeBeforePermitRejectsStaleSender(t *testing.T) {
	s, a := dispatchLab(t)
	old := dispatchOwner(t, a, "BMKG")
	var requests atomic.Int64
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); _, _ = w.Write([]byte("snapshot")) }))
	defer source.Close()
	paused, resume := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-resume:
		default:
			close(resume)
		}
	}()
	gate := dispatchPause{Authority: a, before: func() { close(paused); <-resume }}
	first, err := acquisition.Client(gate, "BMKG", source.URL, 0, 4096)
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() { _, err := dispatchGet(first, old, source.URL+"/seismic-events", "namespace:7"); result <- err }()
	select {
	case <-paused:
	case <-time.After(3 * time.Second):
		t.Fatal("sender did not reach final admission")
	}
	expireDispatchOwner(t, s, "BMKG")
	next := dispatchOwner(t, a, "BMKG")
	second, err := acquisition.Client(a, "BMKG", source.URL, 0, 4096)
	require.NoError(t, err)
	_, err = dispatchGet(second, next, source.URL+"/seismic-events", "namespace:7")
	require.NoError(t, err)
	close(resume)
	require.Error(t, <-result)
	require.EqualValues(t, 1, requests.Load())
}
func TestDispatchLostResponseRemainsBlockedAfterRestart(t *testing.T) {
	s, a := dispatchLab(t)
	old := dispatchOwner(t, a, "PVMBG")
	var requests atomic.Int64
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		connection, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = connection.Close()
		}
	}))
	defer source.Close()
	client, err := acquisition.Client(a, "PVMBG", source.URL, 0, 4096)
	require.NoError(t, err)
	_, err = dispatchGet(client, old, source.URL+"/volcanic-reports", "initial")
	require.ErrorIs(t, err, acquisition.ErrDispatchPending)
	require.EqualValues(t, 1, requests.Load())
	expireDispatchOwner(t, s, "PVMBG")
	restarted, err := ownership.NewAcquisition(s.Pool, 15*time.Second, canonical.Deadline)
	require.NoError(t, err)
	next := dispatchOwner(t, restarted, "PVMBG")
	client, err = acquisition.Client(restarted, "PVMBG", source.URL, 0, 4096)
	require.NoError(t, err)
	for _, key := range []string{"initial", "different", "force-retry"} {
		_, err = dispatchGet(client, next, source.URL+"/volcanic-reports", key)
		require.ErrorIs(t, err, acquisition.ErrDispatchPending)
	}
	require.EqualValues(t, 1, requests.Load(), "timeouts, lease changes and restarts must not authorize another send")
	var state string
	require.NoError(t, s.Pool.QueryRow(context.Background(), `SELECT state FROM acquisition_dispatch WHERE scope='acquisition:PVMBG'`).Scan(&state))
	require.Equal(t, "pending", state)
}
func TestDispatchStreamingHandoffWaitsForBodyClose(t *testing.T) {
	s, a := dispatchLab(t)
	// Canonical deployments do not own satellite; this fixture provisions its
	// acquisition scope only to exercise the same shared dispatch implementation.
	_, err := s.Pool.Exec(context.Background(), `INSERT INTO canonical_ownership(scope,source)VALUES('acquisition:SATELIT','SATELIT')`)
	require.NoError(t, err)
	old := dispatchOwner(t, a, "SATELIT")
	var requests atomic.Int64
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
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
	resp, err := client.Do(request)
	require.NoError(t, err)
	defer resp.Body.Close()
	expireDispatchOwner(t, s, "SATELIT")
	next := dispatchOwner(t, a, "SATELIT")
	_, err = dispatchGet(client, next, source.URL+"/v1/stream?from=1", "stream:next")
	require.ErrorIs(t, err, acquisition.ErrDispatchPending)
	require.EqualValues(t, 1, requests.Load())
	require.NoError(t, resp.Body.Close())
	request, err = http.NewRequestWithContext(acquisition.WithRequestKey(next, "stream:next"), "GET", source.URL+"/v1/stream?from=1", nil)
	require.NoError(t, err)
	resp, err = client.Do(request)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.EqualValues(t, 2, requests.Load())
}
func TestDispatchEnforcesRequestIdentityLimitsAndNoRedirect(t *testing.T) {
	_, a := dispatchLab(t)
	owned := dispatchOwner(t, a, "BMKG")
	var requests atomic.Int64
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		switch r.URL.Query().Get("mode") {
		case "redirect":
			http.Redirect(w, r, "/must-not-fetch", 302)
		case "large":
			_, _ = w.Write([]byte(strings.Repeat("x", 5000)))
		default:
			_, _ = w.Write([]byte("okay"))
		}
	}))
	defer source.Close()
	client, err := acquisition.Client(a, "BMKG", source.URL, 0, 4096)
	require.NoError(t, err)
	_, err = dispatchGet(client, context.Background(), source.URL+"/seismic-events", "initial")
	require.Error(t, err)
	require.EqualValues(t, 0, requests.Load())
	request, err := http.NewRequestWithContext(owned, "GET", source.URL+"/seismic-events", nil)
	require.NoError(t, err)
	_, err = client.Do(request)
	require.ErrorIs(t, err, acquisition.ErrDispatchIdentity)
	_, err = dispatchGet(client, owned, source.URL+"/seismic-events", "initial")
	require.NoError(t, err)
	request, err = http.NewRequestWithContext(acquisition.WithRequestKey(owned, "initial"), "GET", source.URL+"/seismic-events", nil)
	require.NoError(t, err)
	request.Header.Set("X-BMKG-Key", "changed")
	_, err = client.Do(request)
	require.ErrorIs(t, err, acquisition.ErrDispatchPending)
	require.EqualValues(t, 1, requests.Load())
	_, err = dispatchGet(client, owned, source.URL+"/tsunami-warnings?mode=redirect", "redirect")
	require.NoError(t, err)
	require.EqualValues(t, 2, requests.Load(), "HTTP client must not follow unjournalled redirects")
	_, err = dispatchGet(client, owned, source.URL+"/history?entity=seismic-events&mode=large", "large")
	require.ErrorIs(t, err, acquisition.ErrDispatchPending)
	_, err = dispatchGet(client, owned, source.URL+"/history?entity=seismic-events&mode=large", "large-again")
	require.True(t, errors.Is(err, acquisition.ErrDispatchPending))
	require.EqualValues(t, 3, requests.Load())
}

func TestDispatchFailedWorkKeepsReceiptGenerationAndSnapshotBoundary(t *testing.T) {
	s, a := dispatchLab(t)
	owned := dispatchOwner(t, a, "PVMBG")
	var requests atomic.Int64
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); _, _ = w.Write([]byte("snapshot")) }))
	defer source.Close()
	client, err := acquisition.Client(a, "PVMBG", source.URL, 0, 4096)
	require.NoError(t, err)
	firstCtx := acquisition.WithRequestKey(owned, "PVMBG:0")
	req, err := http.NewRequestWithContext(firstCtx, "GET", source.URL+"/volcanic-reports", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	original := acquisition.RequestStarted(firstCtx, time.Time{})
	require.False(t, original.IsZero())
	require.NoError(t, s.RecordAttempt(owned, "PVMBG", "failed-database-step", false, time.Second, time.Minute))
	status, err := s.PollStatus(owned, "PVMBG")
	require.NoError(t, err)
	require.EqualValues(t, 0, status.DispatchGeneration)
	retryCtx := acquisition.WithRequestKey(owned, "PVMBG:0")
	req, err = http.NewRequestWithContext(retryCtx, "GET", source.URL+"/volcanic-reports", nil)
	require.NoError(t, err)
	resp, err = client.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, original, acquisition.RequestStarted(retryCtx, time.Now()), "handoff must retain the source snapshot's original polling boundary")
	require.EqualValues(t, 1, requests.Load())
	require.NoError(t, s.RecordAttempt(retryCtx, "PVMBG", "committed", true, time.Second, time.Minute))
	status, err = s.PollStatus(owned, "PVMBG")
	require.NoError(t, err)
	require.EqualValues(t, 1, status.DispatchGeneration)
	_, err = dispatchGet(client, owned, source.URL+"/volcanic-reports", "PVMBG:1")
	require.NoError(t, err)
	require.EqualValues(t, 2, requests.Load())
}
