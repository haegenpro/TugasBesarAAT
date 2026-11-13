// Package acquisition coordinates in-process pollers. Lease tokens travel with
// work contexts; databases must still fence their protected effects at COMMIT.
package acquisition

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"aggregator/internal/canonical"
	"github.com/google/uuid"
)

var ErrNotOwner = errors.New("active source acquisition ownership required")

type Repository interface {
	canonical.OwnershipRepository
	Validate(context.Context, canonical.Ownership) error
}

type Coordinator struct {
	repository Repository
	holder     string
	logger     *slog.Logger
	budget     time.Duration
	Ready      func() bool
}

func New(repository Repository, logger *slog.Logger) (*Coordinator, error) {
	if repository == nil {
		return nil, errors.New("acquisition repository required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Coordinator{repository: repository, holder: "acq-" + uuid.NewString(), logger: logger, budget: canonical.Deadline}, nil
}

// Source groups live and archive endpoints together. Unknown endpoints fail
// closed when acquisition protection is enabled, instead of creating scopes.
func Source(endpoint string) (string, bool) {
	endpoint = strings.TrimPrefix(endpoint, "poll:")
	switch endpoint {
	case "BMKG", "BMKG_TSUNAMI", "history:seismic-events", "history:tsunami-warnings":
		return "BMKG", true
	case "PVMBG", "history:volcanic-reports", "mapping:volcanic":
		return "PVMBG", true
	default:
		return "", false
	}
}

type guardKey struct{}

type guard struct {
	mu         sync.RWMutex
	ctx        context.Context
	cancel     context.CancelCauseFunc
	owner      canonical.Ownership
	until      time.Time
	repository Repository
	ready      func() bool
	budget     time.Duration
}

func HasGuard(ctx context.Context) bool {
	g, _ := ctx.Value(guardKey{}).(*guard)
	return g != nil
}

func (g *guard) token(source string) (canonical.Ownership, error) {
	g.mu.RLock()
	valid := g.owner.Scope == "acquisition:"+source && time.Now().Before(g.until)
	owner := g.owner
	g.mu.RUnlock()
	// g.ctx is the original leadership context. WithoutCancel on a child (used
	// for failure retention) must not revive a cancelled source generation.
	if !valid || g.ctx.Err() != nil || (g.ready != nil && !g.ready()) {
		return canonical.Ownership{}, ErrNotOwner
	}
	return owner, nil
}

func Token(ctx context.Context, source string) (canonical.Ownership, error) {
	g, _ := ctx.Value(guardKey{}).(*guard)
	if g == nil {
		return canonical.Ownership{}, ErrNotOwner
	}
	return g.token(source)
}

// RequireRequest is installed on both live and history clients. It refreshes
// authoritative evidence immediately before dispatch; it is not an upstream
// fencing protocol and does not make HTTP+database work one atomic operation.
func RequireRequest(ctx context.Context, source string) error {
	g, _ := ctx.Value(guardKey{}).(*guard)
	owner, err := Token(ctx, source)
	if err != nil || ctx.Err() != nil {
		return ErrNotOwner
	}
	check, cancel := context.WithTimeout(ctx, g.budget)
	defer cancel()
	if err = g.repository.Validate(check, owner); err != nil {
		g.cancel(err)
		return err
	}
	_, err = g.token(source)
	return err
}

// Run elects one owner for this source and drains its work before releasing or
// reacquiring. Other sources use independent Run calls and can have other owners.
func (c *Coordinator) Run(ctx context.Context, source string, work func(context.Context)) {
	if source != "BMKG" && source != "PVMBG" {
		c.logger.Error("unsupported acquisition source", "source", source)
		return
	}
	for ctx.Err() == nil {
		if c.Ready == nil || c.Ready() {
			call, cancel := context.WithTimeout(ctx, c.budget)
			lease, err := c.repository.Acquire(call, canonical.AcquireOwnership{Scope: "acquisition:" + source, Holder: c.holder})
			cancel()
			if err == nil {
				completed := c.lead(ctx, source, lease, work)
				cleanup, stop := context.WithTimeout(context.Background(), c.budget)
				_ = c.repository.Release(cleanup, lease.Ownership)
				stop()
				if completed {
					return
				}
			} else {
				var problem *canonical.Error
				if !errors.As(err, &problem) || problem.Status != 409 {
					c.logger.Warn("acquisition unavailable; retrying", "source", source, "error", err)
				}
			}
		}
		if !pause(ctx, time.Second+time.Duration(rand.IntN(250))*time.Millisecond) {
			return
		}
	}
}

func (c *Coordinator) renew(ctx context.Context, owner canonical.Ownership) (canonical.Lease, time.Time, error) {
	started := time.Now()
	call, cancel := context.WithTimeout(ctx, c.budget)
	defer cancel()
	lease, err := c.repository.Renew(call, owner)
	if err != nil {
		return lease, time.Time{}, err
	}
	// Always renew after acquisition: an idempotent Acquire can return an old
	// expiry. Use request-start monotonic time, not a possibly skewed wall clock.
	if lease.RenewAfterMS <= 0 || lease.RenewAfterMS > 10000 {
		return lease, time.Time{}, ErrNotOwner
	}
	interval := time.Duration(lease.RenewAfterMS) * time.Millisecond
	if lease.Ownership != owner || interval < c.budget {
		return lease, time.Time{}, ErrNotOwner
	}
	until := started.Add(3*interval - c.budget)
	if !time.Now().Before(until) {
		return lease, until, ErrNotOwner
	}
	return lease, until, nil
}

func (c *Coordinator) lead(ctx context.Context, source string, acquired canonical.Lease, work func(context.Context)) bool {
	owner := acquired.Ownership
	if owner.Scope != "acquisition:"+source || owner.Holder != c.holder || owner.Epoch <= 0 {
		return false
	}
	lease, until, err := c.renew(ctx, owner)
	if err != nil || ctx.Err() != nil {
		return false
	}
	leaseCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(ErrNotOwner)
	g := &guard{ctx: leaseCtx, cancel: cancel, owner: owner, until: until,
		repository: c.repository, ready: c.Ready, budget: c.budget}
	workCtx := context.WithValue(leaseCtx, guardKey{}, g)
	done := make(chan struct{})
	go func() { defer close(done); work(workCtx) }()
	c.logger.Info("source acquisition acquired", "source", source, "epoch", owner.Epoch, "holder", owner.Holder)
	timer := time.NewTimer(time.Duration(lease.RenewAfterMS) * time.Millisecond)
	expiry := time.NewTimer(time.Until(until))
	defer timer.Stop()
	defer expiry.Stop()
	completed := false
running:
	for {
		select {
		case <-leaseCtx.Done():
			break running
		case <-done:
			completed = leaseCtx.Err() == nil
			break running
		case <-expiry.C:
			break running
		case <-timer.C:
			if _, err = g.token(source); err != nil {
				break running
			}
			lease, until, err = c.renew(leaseCtx, owner)
			if err != nil {
				break running
			}
			g.mu.Lock()
			// A delayed renewal must not revive a locally expired generation.
			valid := time.Now().Before(g.until)
			if valid {
				g.until = until
			}
			g.mu.Unlock()
			if !valid {
				break running
			}
			timer.Reset(time.Duration(lease.RenewAfterMS) * time.Millisecond)
			expiry.Reset(time.Until(until))
		}
	}
	cancel(ErrNotOwner)
	<-done
	c.logger.Info("source acquisition stopped", "source", source, "epoch", owner.Epoch)
	return completed
}

func pause(ctx context.Context, delay time.Duration) bool {
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
