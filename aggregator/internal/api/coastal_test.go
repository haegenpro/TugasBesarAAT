package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCoastalStatusIsServedFromTheLastObservation(t *testing.T) {
	var reply atomic.Value
	reply.Store(`{"state":"stale","last_success":"2026-11-16T03:00:00Z","stale_since":"2026-11-16T03:01:00Z"}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer token" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		body := reply.Load().(string)
		if body == "" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()
	s := &CoastalStatus{URL: server.URL, Token: "token", Client: http.DefaultClient, Interval: 20 * time.Millisecond, MaxAge: 150 * time.Millisecond}
	require.Equal(t, "unavailable", s.Get().State)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	require.Eventually(t, func() bool { return s.Get().State == "stale" }, time.Second, 10*time.Millisecond)
	require.Equal(t, "2026-11-16T03:01:00.000Z", s.Get().StaleSince)
	reply.Store(`{"state":"fresh","last_success":"2026-11-16T03:02:00Z"}`)
	require.Eventually(t, func() bool { return s.Get().State == "fresh" }, time.Second, 10*time.Millisecond)
	reply.Store("")
	require.Eventually(t, func() bool { return s.Get().State == "unavailable" }, time.Second, 10*time.Millisecond)
	started := time.Now()
	for range 1000 {
		s.Get()
	}
	require.Less(t, time.Since(started), 50*time.Millisecond)
}
