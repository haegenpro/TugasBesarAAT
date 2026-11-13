package ownership

import (
	"aggregator/internal/canonical"
	"context"
	"github.com/google/uuid"
	"time"
)

// Run keeps a single process-incarnation authority live and cancels/drains its
// work before release. It always renews after Acquire, which may return an old
// expiry. Local safety uses request-start monotonic deadlines, never DB UTC.
func Run(ctx context.Context, repo canonical.OwnershipRepository, scope string, work func(context.Context, canonical.Ownership)) {
	holder := "worker-" + uuid.NewString()
	for ctx.Err() == nil {
		call, cancel := context.WithTimeout(ctx, canonical.Deadline)
		lease, err := repo.Acquire(call, canonical.AcquireOwnership{Scope: scope, Holder: holder})
		cancel()
		if err == nil && lease.Ownership.Scope == scope && lease.Ownership.Holder == holder {
			lead(ctx, repo, lease.Ownership, work)
			cleanup, stop := context.WithTimeout(context.Background(), canonical.Deadline)
			_ = repo.Release(cleanup, lease.Ownership)
			stop()
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
func lead(ctx context.Context, repo canonical.OwnershipRepository, o canonical.Ownership, work func(context.Context, canonical.Ownership)) {
	renew := func() (canonical.Lease, time.Time, error) {
		started := time.Now()
		call, cancel := context.WithTimeout(ctx, canonical.Deadline)
		defer cancel()
		l, err := repo.Renew(call, o)
		return l, started, err
	}
	l, started, err := renew()
	interval := time.Duration(l.RenewAfterMS) * time.Millisecond
	if err != nil || l.Ownership != o || interval < canonical.Deadline || interval > 10*time.Second {
		return
	}
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); work(child, o) }()
	tick := time.NewTimer(interval)
	expiry := time.NewTimer(time.Until(started.Add(3*interval - canonical.Deadline)))
	defer tick.Stop()
	defer expiry.Stop()
	defer func() { cancel(); <-done }()
	for {
		select {
		case <-child.Done():
			return
		case <-done:
			return
		case <-expiry.C:
			return
		case <-tick.C:
			l, started, err = renew()
			next := time.Duration(l.RenewAfterMS) * time.Millisecond
			if err != nil || l.Ownership != o || next != interval || !time.Now().Before(started.Add(3*interval-canonical.Deadline)) {
				return
			}
			if !expiry.Stop() {
				return
			}
			expiry.Reset(time.Until(started.Add(3*interval - canonical.Deadline)))
			tick.Reset(interval)
		}
	}
}
