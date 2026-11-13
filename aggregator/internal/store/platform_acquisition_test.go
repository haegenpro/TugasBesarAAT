package store

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"aggregator/internal/acquisition"
	"aggregator/internal/canonical"
	"aggregator/internal/clients"
	"aggregator/internal/ownership"
	"github.com/stretchr/testify/require"
)

// Deferred single-database proof: this uses the isolated M2 lab helper, not the
// replicated adapter. It must not be counted as a VM/HA or takeover-SLA result.
func TestPlatformAcquisitionFencesProgress(t *testing.T) {
	dsn, admin := m2Database(t, false)
	m2Schema(t, admin, "postgres", false)
	s := m2Store(t, dsn)
	a, err := ownership.NewAcquisition(s.Pool, 15*time.Second, canonical.Deadline)
	require.NoError(t, err)
	s.Acquisition = a
	public, err := ownership.New(s.Pool, 15*time.Second, canonical.Deadline)
	require.NoError(t, err)
	_, err = public.Acquire(context.Background(), canonical.AcquireOwnership{Scope: "acquisition:BMKG", Holder: "public-caller"})
	m2Error(t, err, "invalid_scope")
	_, err = a.Acquire(context.Background(), canonical.AcquireOwnership{Scope: "canonical:BMKG", Holder: "poller"})
	m2Error(t, err, "invalid_scope")

	err = s.CommitSeismic(context.Background(), nil, "unguarded", 1)
	require.ErrorIs(t, err, acquisition.ErrNotOwner)
	coordinator, err := acquisition.New(a, slog.Default())
	require.NoError(t, err)
	type results struct{ setup, valid, status, staleCursor, staleStatus, staleFailure error }
	out := make(chan results, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	coordinator.Run(ctx, "BMKG", func(owned context.Context) {
		var result results
		result.valid = s.CommitSeismic(owned, nil, "owned", 7)
		result.status = s.RecordAttempt(owned, "BMKG", "owned", true, time.Second, time.Minute)
		// Expire the original lease on the database clock, then acquire a real
		// successor while the old process still has its original local context.
		_, result.setup = admin.Exec(ctx, "UPDATE canonical_ownership SET expires_at=clock_timestamp()-interval '1 second' WHERE scope='acquisition:BMKG'")
		if result.setup == nil {
			_, result.setup = a.Acquire(ctx, canonical.AcquireOwnership{Scope: "acquisition:BMKG", Holder: "successor"})
		}
		result.staleCursor = s.CommitSeismic(owned, nil, "stale", 8)
		result.staleStatus = s.RecordAttempt(owned, "BMKG", "stale", false, time.Second, time.Minute)
		result.staleFailure = s.RecordFailure(context.WithoutCancel(owned), clients.Failure{Source: "BMKG", Entity: "history:seismic-events", Payload: []byte("stale fixture"), Reason: "fixture"})
		out <- result
	})
	var result results
	select {
	case result = <-out:
	default:
		t.Fatal("source acquisition callback did not execute")
	}
	require.NoError(t, result.setup)
	require.NoError(t, result.valid)
	require.NoError(t, result.status)
	require.Error(t, result.staleCursor)
	require.Error(t, result.staleStatus)
	require.Error(t, result.staleFailure)
	cursor, err := s.HistoryCursor(context.Background(), "BMKG", "seismic-events")
	require.NoError(t, err)
	require.EqualValues(t, 7, cursor)
	status, err := s.PollStatus(context.Background(), "BMKG")
	require.NoError(t, err)
	require.Equal(t, "fresh", status.State)
	require.Zero(t, status.Failures)
	var failures int
	require.NoError(t, admin.QueryRow(context.Background(), "SELECT count(*) FROM failed_ingestions").Scan(&failures))
	require.Zero(t, failures)
}

type acquisitionContender struct {
	*ownership.Authority
	contended chan struct{}
}

func (r acquisitionContender) Acquire(ctx context.Context, request canonical.AcquireOwnership) (canonical.Lease, error) {
	lease, err := r.Authority.Acquire(ctx, request)
	var problem *canonical.Error
	if errors.As(err, &problem) && problem.Status == 409 {
		select {
		case r.contended <- struct{}{}:
		default:
		}
	}
	return lease, err
}

func TestPlatformAcquisitionContenderTakesOverAfterDrain(t *testing.T) {
	dsn, admin := m2Database(t, false)
	m2Schema(t, admin, "postgres", false)
	s := m2Store(t, dsn)
	a, err := ownership.NewAcquisition(s.Pool, 15*time.Second, canonical.Deadline)
	require.NoError(t, err)
	first, err := acquisition.New(a, slog.Default())
	require.NoError(t, err)
	repo := acquisitionContender{Authority: a, contended: make(chan struct{}, 1)}
	second, err := acquisition.New(repo, slog.Default())
	require.NoError(t, err)
	root, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	ctx, cancelFirst := context.WithCancel(root)
	defer cancelFirst()
	owners := make(chan canonical.Ownership, 2)
	workErrors := make(chan error, 2)
	doneFirst, doneSecond := make(chan struct{}), make(chan struct{})
	work := func(owned context.Context) {
		owner, err := acquisition.Token(owned, "BMKG")
		workErrors <- err
		owners <- owner
		<-owned.Done()
	}
	go func() { defer close(doneFirst); first.Run(ctx, "BMKG", work) }()
	var before, after canonical.Ownership
	select {
	case before = <-owners:
	case <-root.Done():
		t.Fatal("first owner did not start")
	}
	require.NoError(t, <-workErrors)
	go func() { defer close(doneSecond); second.Run(root, "BMKG", work) }()
	select {
	case <-repo.contended:
	case <-root.Done():
		t.Fatal("standby did not observe the active owner's lease")
	}
	select {
	case <-owners:
		t.Fatal("two source workers started before the first owner drained")
	default:
	}
	cancelFirst()
	select {
	case <-doneFirst:
	case <-root.Done():
		t.Fatal("first owner failed to drain/release")
	}
	select {
	case after = <-owners:
	case <-root.Done():
		t.Fatal("standby failed to acquire after release")
	}
	require.NoError(t, <-workErrors)
	require.Greater(t, after.Epoch, before.Epoch)
	require.NotEqual(t, before.Holder, after.Holder)
	stop()
	select {
	case <-doneSecond:
	case <-time.After(3 * time.Second):
		t.Fatal("successor failed to drain")
	}
}
