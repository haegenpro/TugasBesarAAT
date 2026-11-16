package ingestion

import (
	"aggregator/internal/canonical"
	"aggregator/internal/ownership"
	"aggregator/internal/rawinput"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func sourceLab(t *testing.T, source string) (*SourceStore, canonical.Ownership) {
	t.Helper()
	s, _ := ingestionLab(t)
	a, err := ownership.NewAcquisition(s.DB.Pool, 15*time.Second, canonical.Deadline)
	require.NoError(t, err)
	lease, err := a.Acquire(context.Background(), canonical.AcquireOwnership{Scope: "acquisition:" + source, Holder: "source-fixture"})
	require.NoError(t, err)
	return &SourceStore{DB: s.DB, Durability: s.Durability, Fence: a, Source: source, Namespace: "session-a"}, lease.Ownership
}

func TestSourceProgressIsFencedAndMonotonic(t *testing.T) {
	s, owner := sourceLab(t, "SATELIT")
	ctx := context.Background()
	require.NoError(t, s.Check(ctx))
	p, err := s.Progress(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), p.Position)
	require.NoError(t, s.Advance(ctx, owner, Progress{Position: 40}))
	require.NoError(t, s.Advance(ctx, owner, Progress{Position: 12}))
	p, err = s.Progress(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, int64(40), p.Position)
	stale := owner
	stale.Epoch++
	require.Error(t, s.Advance(ctx, stale, Progress{Position: 41}))
	cursor := "opaque"
	coastal, coastalOwner := sourceLab(t, "SENSOR_PANTAI")
	require.NoError(t, coastal.Advance(ctx, coastalOwner, Progress{Position: 3, Cursor: &cursor}))
	p, err = coastal.Progress(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, "opaque", *p.Cursor)
	require.Error(t, coastal.Advance(ctx, owner, Progress{Position: 4}))
}

func TestQuarantineKeepsOriginalBytesOnce(t *testing.T) {
	s, owner := sourceLab(t, "SENSOR_PANTAI")
	ctx := context.Background()
	payload := []byte(`{"observation_id":"obs-1","critical":"yes"}`)
	position := int64(5)
	ref := "obs-1"
	e := rawinput.Envelope{Version: 1, Source: "SENSOR_PANTAI", Namespace: s.Namespace, Ref: &ref, Position: &position, Operation: "create", ReceivedAt: "2026-11-16T00:00:00Z", Payload: payload, Size: int64(len(payload)), Digest: rawinput.Hash(payload), ContentType: "application/json", CorrelationID: "fixture", OwnershipScope: owner.Scope, OwnershipEpoch: owner.Epoch, OwnershipHolder: owner.Holder}
	e.MessageID = rawinput.ID(e)
	require.NoError(t, rawinput.Validate(e))
	require.NoError(t, s.Quarantine(ctx, e, payload, "critical must be boolean"))
	require.NoError(t, s.Quarantine(ctx, e, payload, "critical must be boolean"))
	var status, classification string
	var attempts int
	require.NoError(t, s.DB.Pool.QueryRow(ctx, `SELECT status,classification,attempts FROM ingestion_failures WHERE message_id=$1`, e.MessageID).Scan(&status, &classification, &attempts))
	require.Equal(t, "parked", status)
	require.Equal(t, "incompatible", classification)
	require.Equal(t, 1, attempts)
	var kept []byte
	require.NoError(t, s.DB.Pool.QueryRow(ctx, `SELECT payload FROM ingestion_failure_payloads WHERE message_id=$1`, e.MessageID).Scan(&kept))
	require.Equal(t, payload, kept)
}
