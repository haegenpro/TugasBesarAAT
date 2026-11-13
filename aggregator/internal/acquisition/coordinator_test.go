package acquisition

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"aggregator/internal/canonical"
	"github.com/stretchr/testify/require"
)

type repositoryFixture struct {
	mu              sync.Mutex
	validationError error
	renewals        int
	failRenewal     bool
	stopped         <-chan struct{}
	releasedEarly   bool
}

func (r *repositoryFixture) Acquire(_ context.Context, req canonical.AcquireOwnership) (canonical.Lease, error) {
	return canonical.Lease{Ownership: canonical.Ownership{Scope: req.Scope, Holder: req.Holder, Epoch: 1}, RenewAfterMS: 500}, nil
}
func (r *repositoryFixture) Renew(_ context.Context, owner canonical.Ownership) (canonical.Lease, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.renewals++
	if r.failRenewal && r.renewals > 1 {
		return canonical.Lease{}, canonical.Unavailable()
	}
	return canonical.Lease{Ownership: owner, RenewAfterMS: 500}, nil
}
func (r *repositoryFixture) Release(context.Context, canonical.Ownership) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped != nil {
		select {
		case <-r.stopped:
		default:
			r.releasedEarly = true
		}
	}
	return nil
}
func (r *repositoryFixture) Validate(context.Context, canonical.Ownership) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.validationError
}

func fixtureCoordinator(t *testing.T, repo *repositoryFixture) *Coordinator {
	t.Helper()
	c, err := New(repo, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	c.budget = 10 * time.Millisecond
	return c
}

func TestAcquisitionCancellationSurvivesWithoutCancel(t *testing.T) {
	repo := &repositoryFixture{}
	c := fixtureCoordinator(t, repo)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, stopped, done := make(chan context.Context, 1), make(chan struct{}), make(chan struct{})
	repo.stopped = stopped
	go func() {
		defer close(done)
		c.Run(ctx, "BMKG", func(owned context.Context) { started <- owned; <-owned.Done(); close(stopped); cancel() })
	}()
	var owned context.Context
	select {
	case owned = <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("owner work did not start")
	}
	require.NoError(t, RequireRequest(owned, "BMKG"))
	require.ErrorIs(t, RequireRequest(owned, "PVMBG"), ErrNotOwner)
	repo.mu.Lock()
	repo.validationError = canonical.Fail(403, "stale_owner", "successor owns source")
	repo.mu.Unlock()
	require.Error(t, RequireRequest(owned, "BMKG"))
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("lease loss did not cancel source work")
	}
	_, err := Token(context.WithoutCancel(owned), "BMKG")
	require.ErrorIs(t, err, ErrNotOwner)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("coordinator did not stop")
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	require.False(t, repo.releasedEarly, "lease released before local source work drained")
}

func TestAcquisitionRenewalFailureStopsWork(t *testing.T) {
	repo := &repositoryFixture{failRenewal: true}
	c := fixtureCoordinator(t, repo)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopped, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx, "PVMBG", func(owned context.Context) { <-owned.Done(); close(stopped); cancel() })
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("failed renewal did not stop source work")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("coordinator did not drain")
	}
}

func TestAcquisitionContextAndSourcePolicy(t *testing.T) {
	_, err := Token(context.Background(), "BMKG")
	require.ErrorIs(t, err, ErrNotOwner)
	require.ErrorIs(t, RequireRequest(context.Background(), "BMKG"), ErrNotOwner)
	for _, endpoint := range []string{"BMKG", "BMKG_TSUNAMI", "history:seismic-events", "history:tsunami-warnings", "poll:BMKG_TSUNAMI"} {
		source, ok := Source(endpoint)
		require.True(t, ok)
		require.Equal(t, "BMKG", source)
	}
	for _, endpoint := range []string{"PVMBG", "history:volcanic-reports", "mapping:volcanic"} {
		source, ok := Source(endpoint)
		require.True(t, ok)
		require.Equal(t, "PVMBG", source)
	}
	_, ok := Source("canonical:BMKG")
	require.False(t, ok)
}
