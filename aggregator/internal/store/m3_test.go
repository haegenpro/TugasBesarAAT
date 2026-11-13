package store

import (
	"aggregator/internal/canonical"
	"aggregator/internal/ownership"
	"aggregator/internal/rawinput"
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"os"
	"sync"
	"testing"
	"time"
)

// Only in _test.go: proves local storage semantics, never replicated durability.
type m3LabDurability struct{ pool *pgxpool.Pool }

func (d m3LabDurability) Prepare(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, "SET LOCAL synchronous_commit=on")
	return err
}
func (d m3LabDurability) Confirm(context.Context, string) error { return nil }
func (d m3LabDurability) ConfirmRecord(ctx context.Context, q string, args ...any) error {
	var exists bool
	err := d.pool.QueryRow(ctx, q, args...).Scan(&exists)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("record not committed")
	}
	return nil
}
func testInput(pos int64) rawinput.Envelope {
	e := rawinput.Envelope{Version: 1, Source: "SATELIT", Namespace: "fixture|é", Operation: "heartbeat", Position: &pos, ReceivedAt: "2026-11-13T00:00:00Z", Payload: []byte{0xff, 0, 0xfe}, Size: 3, ContentType: "application/octet-stream", CorrelationID: "fixture-trace", OwnershipScope: "acquisition:SATELIT", OwnershipEpoch: 1, OwnershipHolder: "old-producer"}
	e.Digest = rawinput.Hash(e.Payload)
	e.MessageID = rawinput.ID(e)
	return e
}
func TestM3HistoryReceiptReplayAndDuplicateRace(t *testing.T) {
	for _, target := range []string{"postgres", "citus"} {
		t.Run(target, func(t *testing.T) {
			if target == "citus" && os.Getenv("M2_WORKER1_URL") == "" {
				t.Skip("isolated Citus workers unavailable")
			}
			dsn, admin := m2Database(t, target == "citus")
			m2Schema(t, admin, target, false)
			s := m2Store(t, dsn)
			s.Canonical.Durability = m3LabDurability{pool: s.Pool}
			a, err := ownership.NewProcessor(s.Pool, 15*time.Second)
			require.NoError(t, err)
			s.Processor = a
			ctx := context.Background()
			lease, err := a.Acquire(ctx, canonical.AcquireOwnership{Scope: "processor:SATELIT", Holder: "receiver"})
			require.NoError(t, err)
			s.Inputs.Starts = map[string]int64{InputSession("SATELIT", "fixture|é"): 7}
			input := testInput(7)
			request := rawinput.AppendRequest{Version: 1, Ownership: lease.Ownership, Messages: []rawinput.Envelope{input}}
			var wg sync.WaitGroup
			errs := make(chan error, 8)
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); _, err := s.AppendHistory(ctx, request); errs <- err }()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				require.NoError(t, err)
			}
			var count int
			require.NoError(t, admin.QueryRow(ctx, `SELECT count(*) FROM input_history`).Scan(&count))
			require.Equal(t, 1, count)
			page, err := s.ReadHistory(ctx, "fixture|é", nil, 100)
			require.NoError(t, err)
			require.Len(t, page.Items, 1)
			require.Equal(t, input, page.Items[0])
			require.Nil(t, page.NextAfter)
			// Acquiring a successor delivery token does not rewrite acquisition provenance.
			require.NoError(t, a.Release(ctx, lease.Ownership))
			successor, err := a.Acquire(ctx, canonical.AcquireOwnership{Scope: "processor:SATELIT", Holder: "successor"})
			require.NoError(t, err)
			request.Ownership = successor.Ownership
			receipt, err := s.AppendHistory(ctx, request)
			require.NoError(t, err)
			require.Equal(t, "duplicate", receipt.Items[0].Outcome)
			request.Messages[0] = testInput(9)
			_, err = s.AppendHistory(ctx, request)
			m2Error(t, err, "source_gap")
			request.Messages[0] = input
			request.Messages[0].Payload = []byte("different")
			request.Messages[0].Size = 9
			request.Messages[0].Digest = rawinput.Hash(request.Messages[0].Payload)
			_, err = s.AppendHistory(ctx, request)
			m2Error(t, err, "identity_conflict")
			request.Messages[0] = testInput(8)
			request.Ownership = lease.Ownership
			_, err = s.AppendHistory(ctx, request)
			m2Error(t, err, "stale_owner")
			var received, prepared, applied int64
			require.NoError(t, admin.QueryRow(ctx, `SELECT received_through,prepared_through,applied_through FROM input_sessions`).Scan(&received, &prepared, &applied))
			require.Equal(t, int64(7), received)
			require.Equal(t, int64(6), prepared)
			require.Equal(t, int64(6), applied)
			if target == "citus" {
				var n int
				require.NoError(t, admin.QueryRow(ctx, `SELECT count(*) FROM pg_dist_partition WHERE logicalrelid IN ('input_history'::regclass,'input_outputs'::regclass,'input_bmkg_observations'::regclass)`).Scan(&n))
				require.Equal(t, 3, n)
			}
		})
	}
}
func TestM3HistoryWholeBatchRollback(t *testing.T) {
	dsn, admin := m2Database(t, false)
	m2Schema(t, admin, "postgres", false)
	s := m2Store(t, dsn)
	s.Canonical.Durability = m3LabDurability{pool: s.Pool}
	a, err := ownership.NewProcessor(s.Pool, 15*time.Second)
	require.NoError(t, err)
	s.Processor = a
	lease, err := a.Acquire(context.Background(), canonical.AcquireOwnership{Scope: "processor:SATELIT", Holder: "x"})
	require.NoError(t, err)
	s.Inputs.Starts = map[string]int64{InputSession("SATELIT", "fixture|é"): 7}
	_, err = s.AppendHistory(context.Background(), rawinput.AppendRequest{Version: 1, Ownership: lease.Ownership, Messages: []rawinput.Envelope{testInput(7), testInput(9)}})
	m2Error(t, err, "source_gap")
	var count int
	require.NoError(t, admin.QueryRow(context.Background(), `SELECT count(*) FROM input_history`).Scan(&count))
	require.Zero(t, count)
}
