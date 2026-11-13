package ownership

import (
	"aggregator/internal/canonical"
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type runRepository struct{ released chan struct{} }

func (r runRepository) Acquire(_ context.Context, q canonical.AcquireOwnership) (canonical.Lease, error) {
	return canonical.Lease{Ownership: canonical.Ownership{Scope: q.Scope, Epoch: 1, Holder: q.Holder}}, nil
}
func (r runRepository) Renew(_ context.Context, o canonical.Ownership) (canonical.Lease, error) {
	return canonical.Lease{Ownership: o, RenewAfterMS: 5000}, nil
}
func (r runRepository) Release(context.Context, canonical.Ownership) error {
	select {
	case r.released <- struct{}{}:
	default:
	}
	return nil
}
func TestRunCancellationDrainsBeforeRelease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started, drained, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	repo := runRepository{released: make(chan struct{}, 1)}
	go func() {
		defer close(done)
		Run(ctx, repo, "processor:BMKG", func(work context.Context, o canonical.Ownership) { close(started); <-work.Done(); close(drained) })
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("owner did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("owner did not drain")
	}
	select {
	case <-drained:
	default:
		t.Fatal("worker still active")
	}
	require.Len(t, repo.released, 1)
}
