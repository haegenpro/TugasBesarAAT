// Package ownership uses the database clock and a commit-time constraint to
// reject work produced by an expired process incarnation.
package ownership

import (
	"aggregator/internal/platform"
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"sync/atomic"
	"time"
)

var ErrBusy = errors.New("source owned by another incarnation")
var ErrFenced = errors.New("source ownership expired")

type tokenKey struct{}
type Token struct {
	Scope, Holder string
	Epoch         int64
}

// Current returns the ownership token carried by ctx inside Authority.Run.
func Current(ctx context.Context) (Token, bool) { token, ok := ctx.Value(tokenKey{}).(Token); return token, ok }

func Scope(ctx context.Context) string { token, _ := ctx.Value(tokenKey{}).(Token); return token.Scope }

type Authority struct {
	Pool   *pgxpool.Pool
	Holder string
	TTL    time.Duration
}

func New(pool *pgxpool.Pool) *Authority {
	holder := uuid.NewString()
	if pod := os.Getenv("POD_NAME"); pod != "" {
		holder = pod + ":" + holder
	}
	return &Authority{Pool: pool, Holder: holder, TTL: 15 * time.Second}
}

// Run keeps a lease alive only while an attempt is active. An expired holder
// receives a new epoch; an old request cannot borrow that new epoch.
func (a *Authority) Run(ctx context.Context, scope string, fn func(context.Context) error) error {
	t := Token{Scope: scope, Holder: a.Holder}
	err := a.Pool.QueryRow(ctx, `INSERT INTO source_leases(scope,holder,epoch,expires_at)
 VALUES($1,$2,1,clock_timestamp()+$3::interval) ON CONFLICT(scope) DO UPDATE SET
 holder=excluded.holder,epoch=CASE WHEN source_leases.holder=$2 AND source_leases.expires_at>clock_timestamp()
 THEN source_leases.epoch ELSE source_leases.epoch+1 END,expires_at=excluded.expires_at
 WHERE source_leases.holder=$2 OR source_leases.expires_at<=clock_timestamp() RETURNING epoch`, scope, a.Holder, fmt.Sprintf("%f seconds", a.TTL.Seconds())).Scan(&t.Epoch)
	if errors.Is(err, pgx.ErrNoRows) {
		platform.Add("ingest_ownership_failures_total_busy", 1)
		return ErrBusy
	}
	if err != nil {
		return err
	}
	c, cancel := context.WithCancel(context.WithValue(ctx, tokenKey{}, t))
	defer cancel()
	done := make(chan struct{})
	var lost atomic.Bool
	go func() {
		defer close(done)
		tick := time.NewTicker(a.TTL / 3)
		defer tick.Stop()
		for {
			select {
			case <-c.Done():
				return
			case <-tick.C:
				r, e := a.Pool.Exec(c, `UPDATE source_leases SET expires_at=clock_timestamp()+$4::interval WHERE scope=$1 AND holder=$2 AND epoch=$3 AND expires_at>clock_timestamp()`, scope, t.Holder, t.Epoch, fmt.Sprintf("%f seconds", a.TTL.Seconds()))
				if e != nil || r.RowsAffected() != 1 {
					platform.Add("ingest_ownership_failures_total_lease_lost", 1)
					lost.Store(true)
					cancel()
					return
				}
			}
		}
	}()
	err = fn(c)
	cancel()
	<-done
	if lost.Load() {
		return errors.Join(err, ErrFenced)
	}
	return err
}

// Guard locks the authoritative lease until this transaction finishes. The
// deferred constraint repeats expiry validation at COMMIT using the DB clock.
func Guard(ctx context.Context, tx pgx.Tx) error {
	t, ok := ctx.Value(tokenKey{}).(Token)
	if !ok {
		return ErrFenced
	}
	var epoch int64
	locked := time.Now()
	err := tx.QueryRow(ctx, `SELECT epoch FROM source_leases WHERE scope=$1 AND holder=$2 AND epoch=$3 AND expires_at>clock_timestamp() FOR UPDATE`, t.Scope, t.Holder, t.Epoch).Scan(&epoch)
	platform.Observe("ingest_lock_wait_seconds_lease_guard", time.Since(locked).Seconds())
	if errors.Is(err, pgx.ErrNoRows) {
		platform.Add("ingest_ownership_failures_total_fenced", 1)
		return ErrFenced
	}
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO source_commit_guards(scope,holder,epoch) VALUES($1,$2,$3) ON CONFLICT(scope) DO UPDATE SET holder=excluded.holder,epoch=excluded.epoch`, t.Scope, t.Holder, t.Epoch)
	return err
}
