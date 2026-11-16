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

func TestSourceReceiptSurvivesReacquisitionUntilCursorCommit(t *testing.T) {
	s, owner := sourceLab(t, "SATELIT")
	ctx := context.Background()
	position := int64(5)
	payload := []byte(`{"offset":5,"kind":"heartbeat"}`)
	e := rawinput.Envelope{Version: 1, Source: s.Source, Namespace: s.Namespace, Position: &position, Operation: "heartbeat", ReceivedAt: "2026-11-16T00:00:00Z", Payload: payload, Size: int64(len(payload)), Digest: rawinput.Hash(payload), ContentType: "application/json", CorrelationID: "first-attempt", OwnershipScope: owner.Scope, OwnershipEpoch: owner.Epoch, OwnershipHolder: owner.Holder}
	e.MessageID = rawinput.ID(e)
	first, err := s.Preserve(ctx, owner, e)
	require.NoError(t, err)
	// Model a publish that committed but whose reply/cursor commit was lost,
	// followed by a different instance taking over after the dedup window.
	a := s.Fence.(*ownership.Authority)
	require.NoError(t, a.Release(ctx, owner))
	lease, err := a.Acquire(ctx, canonical.AcquireOwnership{Scope: owner.Scope, Holder: "replacement"})
	require.NoError(t, err)
	retry := e
	retry.ReceivedAt = "2026-11-16T00:05:00Z"
	retry.CorrelationID = "reconnect"
	retry.OwnershipEpoch, retry.OwnershipHolder = lease.Ownership.Epoch, lease.Ownership.Holder
	kept, err := s.Preserve(ctx, lease.Ownership, retry)
	require.NoError(t, err)
	require.Equal(t, rawinput.Fingerprint(first), rawinput.Fingerprint(kept))
	_, err = s.Preserve(ctx, owner, retry)
	require.Error(t, err, "a stale instance must not reserve receipts")
	changed := retry
	changed.Payload = []byte(`{"offset":5,"changed":true}`)
	changed.Size, changed.Digest = int64(len(changed.Payload)), rawinput.Hash(changed.Payload)
	_, err = s.Preserve(ctx, lease.Ownership, changed)
	require.ErrorContains(t, err, "identity_conflict")
	require.NoError(t, s.Advance(ctx, lease.Ownership, Progress{Position: 6}))
	var pending int
	require.NoError(t, s.DB.Pool.QueryRow(ctx, `SELECT count(*) FROM source_envelopes`).Scan(&pending))
	require.Zero(t, pending, "confirmed offsets must not retain an unbounded receipt ledger")
}
