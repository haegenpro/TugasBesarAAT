// Package ownership implements source-scoped canonical authority in the owning
// coordinator database. Its safety depends on a single authoritative database
// primary; HA/durability verification is a separate deployment gate.
package ownership

import (
	"aggregator/internal/canonical"
	"aggregator/internal/platform"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Authority struct {
	pool       *pgxpool.Pool
	ttl        time.Duration
	budget     time.Duration
	prefix     string
	Durability Durability
}

// Durability is optional for the original local ownership development path.
// The replicated server installs it for authority writes as well as batches.
type Durability interface {
	Prepare(context.Context, pgx.Tx) error
	ConfirmOwnership(context.Context, canonical.Ownership, bool) error
}

func New(pool *pgxpool.Pool, ttl, budget time.Duration) (*Authority, error) {
	if pool == nil || budget < time.Millisecond || budget > canonical.Deadline || ttl < 3*budget || ttl > time.Minute {
		return nil, fmt.Errorf("ownership requires a pool, a 1ms..2s request budget, and a lease of 3 budgets..1 minute")
	}
	return &Authority{pool: pool, ttl: ttl, budget: budget, prefix: "canonical:"}, nil
}

// NewAcquisition keeps acquisition scopes private to the poller coordinator.
// The canonical authority cannot issue or accept these tokens.
func NewAcquisition(pool *pgxpool.Pool, ttl, budget time.Duration) (*Authority, error) {
	if ttl < 12*time.Second || ttl > 30*time.Second {
		return nil, fmt.Errorf("acquisition lease TTL must be between 12s and 30s")
	}
	a, err := New(pool, ttl, budget)
	if err == nil {
		a.prefix = "acquisition:"
	}
	return a, err
}

func (a *Authority) accepts(scope string) bool {
	if a.prefix == "processor:" {
		return scope == "processor:SATELIT" || scope == "processor:BMKG"
	}
	if a.prefix == "acquisition:" {
		return scope == "acquisition:BMKG" || scope == "acquisition:PVMBG"
	}
	return scope == "canonical:BMKG" || scope == "canonical:PVMBG" || scope == "canonical:SATELIT" || scope == "canonical:SENSOR_PANTAI"
}

func valid(v string) bool {
	return v != "" && len(v) <= 256 && strings.TrimSpace(v) == v && utf8.ValidString(v) && !strings.ContainsRune(v, 0)
}

func validOwner(o canonical.Ownership) bool { return valid(o.Scope) && valid(o.Holder) && o.Epoch > 0 }
func stale() error {
	platform.Add("stale_owner_rejections_total", 1)
	return canonical.Fail(403, "stale_owner", "current live ownership required")
}

type record struct {
	source  string
	epoch   int64
	holder  *string
	expires *time.Time
}

func lock(ctx context.Context, tx pgx.Tx, scope, mode string) (record, time.Time, error) {
	var r record
	err := tx.QueryRow(ctx, "SELECT source,epoch,holder,expires_at FROM canonical_ownership WHERE scope=$1 FOR "+mode, scope).Scan(&r.source, &r.epoch, &r.holder, &r.expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, time.Time{}, canonical.Fail(400, "invalid_scope", "scope is not provisioned")
	}
	if err != nil {
		return r, time.Time{}, canonical.Unavailable()
	}
	var now time.Time
	// A separate statement ensures the timestamp is sampled after the row lock.
	err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now)
	if err != nil {
		return r, now, canonical.Unavailable()
	}
	return r, now, nil
}

func (a *Authority) configure(ctx context.Context, tx pgx.Tx) error {
	ms := fmt.Sprintf("%dms", a.budget.Milliseconds())
	_, err := tx.Exec(ctx, "SELECT set_config('statement_timeout',$1,true),set_config('lock_timeout',$1,true),set_config('idle_in_transaction_session_timeout',$1,true),set_config('synchronous_commit','on',true)", ms)
	return err
}

func (a *Authority) begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return nil, canonical.Unavailable()
	}
	if err = a.configure(ctx, tx); err != nil {
		_ = tx.Rollback(context.Background())
		return nil, canonical.Unavailable()
	}
	if a.Durability != nil {
		if err = a.Durability.Prepare(ctx, tx); err != nil {
			_ = tx.Rollback(context.Background())
			return nil, canonical.Unavailable()
		}
	}
	return tx, nil
}

func (a *Authority) lease(scope string, r record) canonical.Lease {
	return canonical.Lease{Ownership: canonical.Ownership{Scope: scope, Epoch: r.epoch, Holder: *r.holder}, ExpiresAt: r.expires.UTC(), RenewAfterMS: (a.ttl / 3).Milliseconds()}
}

func live(r record, now time.Time) bool {
	return r.holder != nil && r.expires != nil && now.Before(*r.expires)
}
func matches(r record, o canonical.Ownership) bool {
	return r.epoch == o.Epoch && r.holder != nil && *r.holder == o.Holder
}

// Acquire is idempotent for a live holder; retrying does not extend its lease.
// A holder must be unique per worker process incarnation, including restarts.
func (a *Authority) Acquire(ctx context.Context, request canonical.AcquireOwnership) (canonical.Lease, error) {
	if !valid(request.Scope) || !valid(request.Holder) {
		return canonical.Lease{}, canonical.Fail(400, "invalid_request", "scope and unique process holder required")
	}
	if !a.accepts(request.Scope) {
		return canonical.Lease{}, canonical.Fail(400, "invalid_scope", "scope is not authorized by this authority")
	}
	ctx, cancel := context.WithTimeout(ctx, a.budget)
	defer cancel()
	tx, err := a.begin(ctx)
	if err != nil {
		return canonical.Lease{}, err
	}
	defer tx.Rollback(context.Background())
	r, now, err := lock(ctx, tx, request.Scope, "UPDATE")
	if err != nil {
		return canonical.Lease{}, err
	}
	if live(r, now) {
		if *r.holder != request.Holder {
			return canonical.Lease{}, canonical.Fail(409, "ownership_conflict", "scope already has a live owner")
		}
	} else {
		if r.epoch == math.MaxInt64 {
			return canonical.Lease{}, canonical.Unavailable()
		}
		r.epoch++
		r.holder = &request.Holder
		expires := now.Add(a.ttl)
		r.expires = &expires
		if _, err = tx.Exec(ctx, "UPDATE canonical_ownership SET epoch=$2,holder=$3,expires_at=$4 WHERE scope=$1", request.Scope, r.epoch, r.holder, r.expires); err != nil {
			return canonical.Lease{}, canonical.Unavailable()
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return canonical.Lease{}, canonical.Unavailable()
	}
	lease := a.lease(request.Scope, r)
	if a.Durability != nil && a.Durability.ConfirmOwnership(ctx, lease.Ownership, false) != nil {
		return canonical.Lease{}, canonical.Unavailable()
	}
	return lease, nil
}

func (a *Authority) Renew(ctx context.Context, o canonical.Ownership) (canonical.Lease, error) {
	if !validOwner(o) || !a.accepts(o.Scope) {
		return canonical.Lease{}, canonical.Fail(400, "invalid_request", "scope, positive epoch and holder required")
	}
	ctx, cancel := context.WithTimeout(ctx, a.budget)
	defer cancel()
	tx, err := a.begin(ctx)
	if err != nil {
		return canonical.Lease{}, err
	}
	defer tx.Rollback(context.Background())
	r, now, err := lock(ctx, tx, o.Scope, "UPDATE")
	if err != nil {
		return canonical.Lease{}, err
	}
	if !matches(r, o) || !live(r, now) {
		return canonical.Lease{}, stale()
	}
	expires := now.Add(a.ttl)
	r.expires = &expires
	if _, err = tx.Exec(ctx, "UPDATE canonical_ownership SET expires_at=$2 WHERE scope=$1", o.Scope, expires); err != nil {
		return canonical.Lease{}, canonical.Unavailable()
	}
	if err = tx.Commit(ctx); err != nil {
		return canonical.Lease{}, canonical.Unavailable()
	}
	if a.Durability != nil && a.Durability.ConfirmOwnership(ctx, o, false) != nil {
		return canonical.Lease{}, canonical.Unavailable()
	}
	return a.lease(o.Scope, r), nil
}

func (a *Authority) Release(ctx context.Context, o canonical.Ownership) error {
	if !validOwner(o) || !a.accepts(o.Scope) {
		return canonical.Fail(400, "invalid_request", "scope, positive epoch and holder required")
	}
	ctx, cancel := context.WithTimeout(ctx, a.budget)
	defer cancel()
	tx, err := a.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	r, _, err := lock(ctx, tx, o.Scope, "UPDATE")
	if err != nil {
		return err
	}
	// A repeated release is harmless until a newer epoch has been issued.
	if r.epoch != o.Epoch || (r.holder != nil && *r.holder != o.Holder) {
		return stale()
	}
	if _, err = tx.Exec(ctx, "UPDATE canonical_ownership SET holder=NULL,expires_at=NULL WHERE scope=$1", o.Scope); err != nil {
		return canonical.Unavailable()
	}
	if err = tx.Commit(ctx); err != nil {
		return canonical.Unavailable()
	}
	if a.Durability != nil && a.Durability.ConfirmOwnership(ctx, o, true) != nil {
		return canonical.Unavailable()
	}
	return nil
}

// Protect implements store.Fence. The SHARE lock conflicts with takeover,
// renewal and release through the transaction's commit. The deferred check
// rejects a transaction that outlives its lease even without a successor.
func (a *Authority) Protect(ctx context.Context, tx pgx.Tx, o canonical.Ownership, mutations []canonical.Mutation) error {
	if !validOwner(o) || !a.accepts(o.Scope) || len(mutations) == 0 {
		return stale()
	}
	if err := a.configure(ctx, tx); err != nil {
		return canonical.Unavailable()
	}
	r, now, err := lock(ctx, tx, o.Scope, "SHARE")
	if err != nil {
		return err
	}
	if !matches(r, o) || !live(r, now) {
		return stale()
	}
	for _, mutation := range mutations {
		if string(mutation.Source) != r.source {
			return stale()
		}
	}
	_, err = tx.Exec(ctx, "INSERT INTO canonical_ownership_checks(transaction_id,scope,epoch,holder) VALUES(txid_current(),$1,$2,$3) ON CONFLICT DO NOTHING", o.Scope, o.Epoch, o.Holder)
	if err != nil {
		return canonical.Unavailable()
	}
	return nil
}

var _ canonical.OwnershipRepository = (*Authority)(nil)

func NewProcessor(pool *pgxpool.Pool, ttl time.Duration) (*Authority, error) {
	a, err := NewAcquisition(pool, ttl, canonical.Deadline)
	if err == nil {
		a.prefix = "processor:"
	}
	return a, err
}

// Validate preflights an outbound request. Effects still need Protect through
// COMMIT; no database transaction is held across the HTTP request.
func (a *Authority) Validate(ctx context.Context, o canonical.Ownership) error {
	if !validOwner(o) || !a.accepts(o.Scope) {
		return stale()
	}
	ctx, cancel := context.WithTimeout(ctx, a.budget)
	defer cancel()
	tx, err := a.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	r, now, err := lock(ctx, tx, o.Scope, "SHARE")
	if err != nil {
		return err
	}
	if !matches(r, o) || !live(r, now) {
		return stale()
	}
	if err = tx.Commit(ctx); err != nil {
		return canonical.Unavailable()
	}
	return nil
}
