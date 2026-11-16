package store

import (
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
	"strconv"
	"testing"
	"time"
)

func TestLegacyFailureStreamIsCompleteBoundedAndReplayable(t *testing.T) {
	dsn, c := m2Database(t, false)
	m2Schema(t, c, "postgres", false)
	s := m2Store(t, dsn)
	ctx := context.Background()
	t.Setenv("MAX_PAYLOAD_BYTES", "64")
	raw := append([]byte(`[{"report_id":"oversized","volcano_id":"merapi","alert_level":"Siaga","eruption_count_24h":2,"ash_column_height_m":800,"reported_at":"2026-11-16T00:00:00Z"}]`), bytes.Repeat([]byte(" "), 180000)...)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(raw)
	}))
	defer srv.Close()
	client := clients.NewPVMBGClient(srv.URL, "key", slog.New(slog.NewTextHandler(io.Discard, nil)))
	client.CaptureFailure = s.RecordFailure
	for range 2 {
		_, err := client.FetchVolcanicReports(ctx, "complete-capture", time.Time{})
		require.ErrorIs(t, err, clients.ErrContractViolation)
	}
	var id, digest string
	var truncated bool
	var size int64
	var count, attempts int
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT f.message_id::text,f.digest,f.truncated,p.size,f.attempts,(SELECT count(*) FROM failed_ingestion_payloads) FROM failed_ingestions f JOIN failed_ingestion_payloads p USING(payload_id)`).Scan(&id, &digest, &truncated, &size, &attempts, &count))
	require.False(t, truncated)
	require.Equal(t, int64(len(raw)), size)
	require.Equal(t, 2, attempts)
	require.Equal(t, 1, count)
	require.Equal(t, rawinput.Hash(raw), digest)
	var largest int
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT max(octet_length(data)) FROM failed_ingestion_payload_chunks`).Scan(&largest))
	require.LessOrEqual(t, largest, 64<<10)
	var kept bytes.Buffer
	_, _, _, err := s.WriteFailurePayload(ctx, id, &kept)
	require.NoError(t, err)
	require.Equal(t, raw, kept.Bytes())
	_, err = s.RequestLegacyReplay(ctx, []string{id}, "pvmbg-adapter-v1")
	require.NoError(t, err)
	parked, err := s.NextLegacyReplay(ctx)
	require.NoError(t, err)
	require.Contains(t, parked.Rejection, "raise MAX_PAYLOAD_BYTES")
	t.Setenv("MAX_PAYLOAD_BYTES", strconv.Itoa(len(raw)+1))
	parked, err = s.NextLegacyReplay(ctx)
	require.NoError(t, err)
	require.Equal(t, raw, parked.Payload)
	require.Empty(t, parked.Rejection)
	reports, err := client.Reprocess(ctx, *parked)
	require.NoError(t, err)
	require.Len(t, reports, 1)
}

type brokenPayload struct{ sent bool }

func (r *brokenPayload) Read(p []byte) (int, error) {
	if r.sent {
		return 0, errors.New("connection reset")
	}
	r.sent = true
	return copy(p, []byte("partial")), nil
}

func TestLegacyFailureStreamRollsBackOnQuotaAndReadFailure(t *testing.T) {
	dsn, c := m2Database(t, false)
	m2Schema(t, c, "postgres", false)
	s := m2Store(t, dsn)
	ctx := context.Background()
	t.Setenv("FAILED_INPUT_QUOTA_BYTES", "32")
	for _, body := range []io.Reader{bytes.NewReader(bytes.Repeat([]byte("x"), 64)), &brokenPayload{}} {
		err := s.RecordFailure(ctx, clients.Failure{Source: "PVMBG", Entity: "/volcanic-reports", PayloadStream: body, Reason: "oversized"})
		require.Error(t, err)
		var count int
		require.NoError(t, s.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM failed_ingestions)+(SELECT count(*) FROM failed_ingestion_payloads)+(SELECT count(*) FROM failed_ingestion_payload_chunks)`).Scan(&count))
		require.Zero(t, count)
	}
}
