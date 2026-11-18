package store

import (
	"aggregator/internal/acquisition"
	"aggregator/internal/clients"
	"aggregator/internal/rawinput"
	"bytes"
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestReassessmentDispatchOverflowMustReachFailureCapture(t *testing.T) {
	_, authority := dispatchLab(t)
	owned := dispatchOwner(t, authority, "PVMBG")
	body := []byte(strings.Repeat("x", 5000))
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(body) }))
	defer source.Close()
	sourceHTTP, err := acquisition.Client(authority, "PVMBG", source.URL, 0, 4096)
	require.NoError(t, err)
	client := clients.NewPVMBGClient(source.URL, "review-key", slog.New(slog.NewTextHandler(io.Discard, nil)))
	client.HTTP = sourceHTTP
	client.BeforeFetch = acquisition.RequireRequest
	captured := 0
	kept := 0
	client.CaptureFailure = func(_ context.Context, f clients.Failure) error {
		captured++
		kept = len(f.Payload)
		if f.PayloadStream != nil {
			b, err := io.ReadAll(f.PayloadStream)
			kept = len(b)
			return err
		}
		return nil
	}
	_, err = client.FetchVolcanicReports(acquisition.WithRequestKey(owned, "PVMBG:0"), "review-overflow", time.Time{})
	require.Error(t, err)
	t.Logf("REVIEW_OBSERVATION: capture_calls=%d retained_bytes=%d source_bytes=%d error=%v", captured, kept, len(body), err)
	require.Equal(t, 1, captured, "oversized failed input must reach the existing failure capture path")
	require.Equal(t, len(body), kept)
}

func TestReassessmentDispatchOverflowRetainsCompletePayloadInOwnedStore(t *testing.T) {
	t.Setenv("MAX_PAYLOAD_BYTES", "1024")
	s, authority := dispatchLab(t)
	owned := dispatchOwner(t, authority, "PVMBG")
	original := append(bytes.Repeat([]byte("0123456789abcdef"), 10000), []byte("END\xff")...)
	var calls atomic.Int64
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			_, _ = w.Write(original)
			return
		}
		_, _ = w.Write([]byte(`[]`))
	}))
	defer source.Close()
	sourceHTTP, err := acquisition.Client(authority, "PVMBG", source.URL, 0, 4096)
	require.NoError(t, err)
	client := clients.NewPVMBGClient(source.URL, "fixture-key", slog.New(slog.NewTextHandler(io.Discard, nil)))
	client.HTTP = sourceHTTP
	client.BeforeFetch = acquisition.RequireRequest
	client.CaptureFailure = s.RecordFailure
	_, err = client.FetchVolcanicReports(acquisition.WithRequestKey(owned, "PVMBG:0"), "oversize", time.Time{})
	require.Error(t, err)
	var id, disposition string
	var truncated bool
	require.NoError(t, s.Pool.QueryRow(owned, `SELECT message_id::text,truncated FROM failed_ingestions`).Scan(&id, &truncated))
	require.False(t, truncated)
	var kept bytes.Buffer
	size, digest, _, err := s.WriteFailurePayload(owned, id, &kept)
	require.NoError(t, err)
	require.EqualValues(t, len(original), size)
	require.Equal(t, original, kept.Bytes())
	require.Equal(t, rawinput.Hash(original), digest)
	require.NoError(t, s.Pool.QueryRow(owned, `SELECT state FROM acquisition_dispatch`).Scan(&disposition))
	require.Equal(t, "quarantined", disposition)
	reports, err := client.FetchVolcanicReports(acquisition.WithRequestKey(owned, "PVMBG:0"), "recovered", time.Time{})
	require.NoError(t, err)
	require.Empty(t, reports)
	require.EqualValues(t, 2, calls.Load())
}
func TestReassessmentDispatchOverflowDoesNotRetireOnRecorderFailure(t *testing.T) {
	t.Setenv("MAX_PAYLOAD_BYTES", "32")
	s, authority := dispatchLab(t)
	owned := dispatchOwner(t, authority, "PVMBG")
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(bytes.Repeat([]byte("x"), 5000)) }))
	defer source.Close()
	sourceHTTP, err := acquisition.Client(authority, "PVMBG", source.URL, 0, 4096)
	require.NoError(t, err)
	client := clients.NewPVMBGClient(source.URL, "fixture-key", slog.New(slog.NewTextHandler(io.Discard, nil)))
	client.HTTP = sourceHTTP
	client.BeforeFetch = acquisition.RequireRequest
	client.CaptureFailure = func(context.Context, clients.Failure) error { return errors.New("retention unavailable") }
	_, err = client.FetchVolcanicReports(acquisition.WithRequestKey(owned, "PVMBG:0"), "retention-failure", time.Time{})
	require.Error(t, err)
	var state string
	require.NoError(t, s.Pool.QueryRow(owned, `SELECT state FROM acquisition_dispatch`).Scan(&state))
	require.Equal(t, "pending", state)
	client.CaptureFailure = func(context.Context, clients.Failure) error { return nil } // intentionally fails to consume the stream
	time.Sleep(550 * time.Millisecond)
	_, err = client.FetchVolcanicReports(acquisition.WithRequestKey(owned, "PVMBG:0"), "incomplete-retention", time.Time{})
	require.ErrorContains(t, err, "complete response")
	require.NoError(t, s.Pool.QueryRow(owned, `SELECT state FROM acquisition_dispatch`).Scan(&state))
	require.Equal(t, "pending", state)
}
