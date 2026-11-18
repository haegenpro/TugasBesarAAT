package ownership

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"aggregator/internal/acquisition"
	"aggregator/internal/canonical"
	"aggregator/internal/platform"
	"github.com/google/uuid"
)

type recordConfirmer interface {
	ConfirmRecord(context.Context, string, ...any) error
}

func (a *Authority) confirmDispatch(ctx context.Context, id, state string) error {
	if a.Durability == nil {
		return nil
	}
	d, ok := a.Durability.(recordConfirmer)
	if !ok {
		return canonical.Unavailable()
	}
	return d.ConfirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM acquisition_dispatch WHERE dispatch_id=$1 AND state=$2)`, id, state)
}

func (a *Authority) ReserveDispatch(ctx context.Context, d acquisition.Dispatch) (acquisition.Receipt, error) {
	var out acquisition.Receipt
	if a.prefix != "acquisition:" || !a.accepts(d.Owner.Scope) || !validOwner(d.Owner) || !valid(d.Lane) || len(d.Key) != 64 || len(d.Fingerprint) != 64 || uuid.Validate(d.ID) != nil {
		return out, acquisition.ErrDispatchIdentity
	}
	ctx, cancel := context.WithTimeout(ctx, a.budget)
	defer cancel()
	tx, err := a.begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback(context.Background())
	r, now, err := lock(ctx, tx, d.Owner.Scope, "SHARE")
	if err != nil {
		return out, err
	}
	if !matches(r, d.Owner) || !live(r, now) {
		return out, stale()
	}
	// The ownership lock is held only for this short decision, not for network
	// I/O. The durable row, not a row/advisory lock or lease, protects dispatch.
	tag, err := tx.Exec(ctx, `INSERT INTO acquisition_dispatch(scope,lane,request_key,fingerprint,dispatch_id,epoch,holder,state,streaming) VALUES($1,$2,$3,$4,$5,$6,$7,'pending',$8) ON CONFLICT(scope,lane) DO NOTHING`, d.Owner.Scope, d.Lane, d.Key, d.Fingerprint, d.ID, d.Owner.Epoch, d.Owner.Holder, d.Streaming)
	if err != nil {
		return out, canonical.Unavailable()
	}
	inserted := tag.RowsAffected() == 1
	var key, fingerprint string
	var streaming bool
	var status *int
	var headers, body []byte
	err = tx.QueryRow(ctx, `SELECT dispatch_id::text,request_key,fingerprint,state,streaming,started_at,status,headers,body FROM acquisition_dispatch WHERE scope=$1 AND lane=$2 FOR UPDATE`, d.Owner.Scope, d.Lane).Scan(&out.ID, &key, &fingerprint, &out.State, &streaming, &out.StartedAt, &status, &headers, &body)
	if err != nil {
		return out, canonical.Unavailable()
	}
	if !inserted {
		if out.State == "pending" {
			return out, acquisition.ErrDispatchPending
		}
		if out.State == "complete" && key == d.Key {
			if fingerprint != d.Fingerprint || streaming != d.Streaming {
				return out, acquisition.ErrDispatchIdentity
			}
			if status != nil && *status == 200 {
				out.Status = *status
				out.Body = body
				if json.Unmarshal(headers, &out.Headers) != nil {
					return out, canonical.Unavailable()
				}
				if err = tx.Commit(ctx); err != nil {
					return out, canonical.Unavailable()
				}
				if err = a.confirmDispatch(ctx, out.ID, "complete"); err != nil {
					return out, err
				}
				return out, nil
			}
		}
		var oldPending bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM acquisition_dispatch WHERE scope=$1 AND state='pending' AND epoch<>$2)`, d.Owner.Scope, d.Owner.Epoch).Scan(&oldPending); err != nil {
			return out, canonical.Unavailable()
		}
		if oldPending {
			return out, acquisition.ErrDispatchPending
		}
		err = tx.QueryRow(ctx, `UPDATE acquisition_dispatch SET request_key=$3,fingerprint=$4,dispatch_id=$5,epoch=$6,holder=$7,state='pending',streaming=$8,started_at=clock_timestamp(),completed_at=NULL,status=NULL,headers=NULL,body=NULL WHERE scope=$1 AND lane=$2 RETURNING started_at`, d.Owner.Scope, d.Lane, d.Key, d.Fingerprint, d.ID, d.Owner.Epoch, d.Owner.Holder, d.Streaming).Scan(&out.StartedAt)
		if err != nil {
			return out, canonical.Unavailable()
		}
		out.ID, out.State = d.ID, "pending"
	}
	var oldPending bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM acquisition_dispatch WHERE scope=$1 AND state='pending' AND epoch<>$2)`, d.Owner.Scope, d.Owner.Epoch).Scan(&oldPending); err != nil {
		return out, canonical.Unavailable()
	}
	if oldPending {
		return out, acquisition.ErrDispatchPending
	}
	if err = tx.Commit(ctx); err != nil {
		return out, canonical.Unavailable()
	}
	// Do not send unless the intent meets the owning formation's replicated
	// durability contract. Failure to confirm intentionally leaves it pending.
	if err = a.confirmDispatch(ctx, d.ID, "pending"); err != nil {
		return out, err
	}
	return out, nil
}

func (a *Authority) finishDispatch(ctx context.Context, d acquisition.Dispatch, r acquisition.Receipt, stream bool) error {
	if a.prefix != "acquisition:" || !a.accepts(d.Owner.Scope) || !validOwner(d.Owner) || uuid.Validate(d.ID) != nil || d.Streaming != stream {
		return acquisition.ErrDispatchIdentity
	}
	ctx, cancel := context.WithTimeout(ctx, a.budget)
	defer cancel()
	tx, err := a.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	// The dispatch nonce authorizes only completing its existing intent. A stale
	// owner may save bytes already fetched; it still cannot begin another fetch
	// or write input/canonical effects. Otherwise a lost lease would lose bytes.
	state := "complete"
	var rows int64
	if stream {
		state = "closed"
		command, e := tx.Exec(ctx, `UPDATE acquisition_dispatch SET state='closed',completed_at=clock_timestamp() WHERE dispatch_id=$1 AND scope=$2 AND epoch=$3 AND holder=$4 AND state='pending' AND streaming`, d.ID, d.Owner.Scope, d.Owner.Epoch, d.Owner.Holder)
		rows, err = command.RowsAffected(), e
	} else {
		if r.Status < 100 || r.Status > 599 || r.Headers == nil || r.Body == nil {
			return acquisition.ErrDispatchIdentity
		}
		headers, e := json.Marshal(r.Headers)
		if e != nil {
			return e
		}
		command, e := tx.Exec(ctx, `UPDATE acquisition_dispatch SET state='complete',completed_at=clock_timestamp(),status=$5,headers=$6,body=$7 WHERE dispatch_id=$1 AND scope=$2 AND epoch=$3 AND holder=$4 AND state='pending' AND NOT streaming`, d.ID, d.Owner.Scope, d.Owner.Epoch, d.Owner.Holder, r.Status, headers, r.Body)
		rows, err = command.RowsAffected(), e
	}
	if err != nil || rows != 1 {
		return canonical.Unavailable()
	}
	if err = tx.Commit(ctx); err != nil {
		return canonical.Unavailable()
	}
	return a.confirmDispatch(ctx, d.ID, state)
}
func (a *Authority) CompleteDispatch(ctx context.Context, d acquisition.Dispatch, r acquisition.Receipt) error {
	return a.finishDispatch(ctx, d, r, false)
}
func (a *Authority) CloseDispatchStream(ctx context.Context, d acquisition.Dispatch) error {
	return a.finishDispatch(ctx, d, acquisition.Receipt{}, true)
}

// DispatchWindow uses the authority clock for metadata cache generations. It
// never decides lease validity or reclaims a pending request.
func (a *Authority) DispatchWindow(ctx context.Context, width time.Duration) (string, error) {
	if width < time.Second {
		return "", errors.New("metadata window must be at least one second")
	}
	var key string
	err := a.pool.QueryRow(ctx, `SELECT floor(extract(epoch FROM clock_timestamp())/$1)::bigint::text`, width.Seconds()).Scan(&key)
	if err != nil {
		platform.Add("acquisition_dispatch_clock_failures_total", 1)
	}
	return key, err
}
