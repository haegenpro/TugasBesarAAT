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
	// Serialize source-wide retirement with lease changes and other admissions.
	// No network work is performed while this ownership row is locked.
	authority, now, err := lock(ctx, tx, d.Owner.Scope, "UPDATE")
	if err != nil {
		return out, err
	}
	if !matches(authority, d.Owner) || !live(authority, now) {
		return out, stale()
	}
	tag, err := tx.Exec(ctx, `INSERT INTO acquisition_dispatch(scope,lane,request_key,fingerprint,dispatch_id,epoch,holder,state,streaming)VALUES($1,$2,$3,$4,$5,$6,$7,'pending',$8)ON CONFLICT(scope,lane)DO NOTHING`, d.Owner.Scope, d.Lane, d.Key, d.Fingerprint, d.ID, d.Owner.Epoch, d.Owner.Holder, d.Streaming)
	if err != nil {
		return out, canonical.Unavailable()
	}
	inserted := tag.RowsAffected() == 1
	var key, fingerprint, holder string
	var epoch int64
	var streaming, open, interrupted bool
	var status *int
	var headers, body []byte
	var retryAfter time.Time
	err = tx.QueryRow(ctx, `SELECT dispatch_id::text,request_key,fingerprint,state,streaming,started_at,status,headers,body,epoch,holder,retry_after,stream_open,interrupted,recovery_count FROM acquisition_dispatch WHERE scope=$1 AND lane=$2 FOR UPDATE`, d.Owner.Scope, d.Lane).Scan(&out.ID, &key, &fingerprint, &out.State, &streaming, &out.StartedAt, &status, &headers, &body, &epoch, &holder, &retryAfter, &open, &interrupted, &out.RecoveryCount)
	if err != nil {
		return out, canonical.Unavailable()
	}
	if !inserted && out.State == "complete" && key == d.Key {
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
	if !inserted && out.State == "pending" {
		sameOwner := epoch == d.Owner.Epoch && holder == d.Owner.Holder
		if now.Before(retryAfter) || sameOwner && streaming && open && !interrupted {
			return out, &acquisition.DispatchWait{}
		}
	}
	// A replacement gives every old in-flight lane its bounded grace period,
	// including metadata lanes that it might not otherwise request immediately.
	var draining bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM acquisition_dispatch WHERE scope=$1 AND state='pending' AND (epoch<>$2 OR holder<>$3) AND retry_after>$4)`, d.Owner.Scope, d.Owner.Epoch, d.Owner.Holder, now).Scan(&draining)
	if err != nil {
		return out, canonical.Unavailable()
	}
	if draining {
		return out, &acquisition.DispatchWait{}
	}
	_, err = tx.Exec(ctx, `UPDATE acquisition_dispatch SET state='abandoned',stream_open=false,interrupted=true,previous_dispatch_id=dispatch_id,recovery_count=recovery_count+1,last_recovery_at=$4,last_failure='owner_replaced' WHERE scope=$1 AND lane<>$5 AND state='pending' AND (epoch<>$2 OR holder<>$3)`, d.Owner.Scope, d.Owner.Epoch, d.Owner.Holder, now, d.Lane)
	if err != nil {
		return out, canonical.Unavailable()
	}
	if !inserted {
		recover := out.State == "pending"
		out.Recovered = recover || out.State == "abandoned"
		err = tx.QueryRow(ctx, `UPDATE acquisition_dispatch SET
   started_at=CASE WHEN request_key=$3 AND fingerprint=$4 THEN started_at ELSE $9 END,
   previous_dispatch_id=CASE WHEN $10 THEN dispatch_id ELSE previous_dispatch_id END,
   recovery_count=recovery_count+CASE WHEN $10 THEN 1 ELSE 0 END,
   last_recovery_at=CASE WHEN $10 THEN $9 ELSE last_recovery_at END,
   last_failure=CASE WHEN $10 THEN CASE WHEN epoch<>$6 OR holder<>$7 THEN 'owner_replaced' WHEN interrupted THEN last_failure ELSE 'timeout' END ELSE '' END,
   request_key=$3,fingerprint=$4,dispatch_id=$5,epoch=$6,holder=$7,state='pending',streaming=$8,
   attempt_started_at=$9,retry_after=$9+interval '10 seconds',stream_open=false,interrupted=false,
   completed_at=NULL,status=NULL,headers=NULL,body=NULL
   WHERE scope=$1 AND lane=$2 RETURNING started_at,recovery_count`, d.Owner.Scope, d.Lane, d.Key, d.Fingerprint, d.ID, d.Owner.Epoch, d.Owner.Holder, d.Streaming, now, recover).Scan(&out.StartedAt, &out.RecoveryCount)
		if err != nil {
			return out, canonical.Unavailable()
		}
		out.ID, out.State = d.ID, "pending"
	}
	if err = tx.Commit(ctx); err != nil {
		return out, canonical.Unavailable()
	}
	if err = a.confirmDispatch(ctx, d.ID, "pending"); err != nil {
		return out, err
	}
	return out, nil
}

// CheckDispatch revalidates both ownership and the nonce after response caching
// and durable admission, immediately before the actual HTTP transport is entered.
func (a *Authority) CheckDispatch(ctx context.Context, d acquisition.Dispatch) error {
	return a.checkOrOpenDispatch(ctx, d, false)
}
func (a *Authority) OpenDispatchStream(ctx context.Context, d acquisition.Dispatch) error {
	if !d.Streaming {
		return acquisition.ErrDispatchIdentity
	}
	return a.checkOrOpenDispatch(ctx, d, true)
}
func (a *Authority) checkOrOpenDispatch(ctx context.Context, d acquisition.Dispatch, open bool) error {
	if a.prefix != "acquisition:" || !a.accepts(d.Owner.Scope) || !validOwner(d.Owner) || uuid.Validate(d.ID) != nil {
		return acquisition.ErrDispatchIdentity
	}
	ctx, cancel := context.WithTimeout(ctx, a.budget)
	defer cancel()
	tx, err := a.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	r, now, err := lock(ctx, tx, d.Owner.Scope, "SHARE")
	if err != nil {
		return err
	}
	if !matches(r, d.Owner) || !live(r, now) {
		return stale()
	}
	var exists bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM acquisition_dispatch WHERE dispatch_id=$1 AND scope=$2 AND epoch=$3 AND holder=$4 AND state='pending' AND NOT interrupted AND retry_after>$5)`, d.ID, d.Owner.Scope, d.Owner.Epoch, d.Owner.Holder, now).Scan(&exists)
	if err != nil {
		return canonical.Unavailable()
	}
	if !exists {
		return acquisition.ErrNotOwner
	}
	if open {
		tag, e := tx.Exec(ctx, `UPDATE acquisition_dispatch SET stream_open=true WHERE dispatch_id=$1 AND state='pending' AND streaming`, d.ID)
		if e != nil || tag.RowsAffected() != 1 {
			return acquisition.ErrNotOwner
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return canonical.Unavailable()
	}
	if open {
		return a.confirmDispatch(ctx, d.ID, "pending")
	}
	return nil
}

// Interrupt records an unknown outcome; it does not claim the source never saw
// the request. Only a current live owner can later authorize the recovery read.
// LEAST prevents an obsolete sender from extending a replacement's grace period.
func (a *Authority) InterruptDispatch(ctx context.Context, d acquisition.Dispatch, reason string) error {
	if a.prefix != "acquisition:" || !a.accepts(d.Owner.Scope) || !validOwner(d.Owner) || uuid.Validate(d.ID) != nil {
		return acquisition.ErrDispatchIdentity
	}
	switch reason {
	case "preflight", "transport", "body", "receipt":
	default:
		return acquisition.ErrDispatchIdentity
	}
	ctx, cancel := context.WithTimeout(ctx, a.budget)
	defer cancel()
	tx, err := a.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	_, err = tx.Exec(ctx, `UPDATE acquisition_dispatch SET interrupted=true,stream_open=false,last_failure=$5,retry_after=LEAST(retry_after,clock_timestamp()+interval '500 milliseconds') WHERE dispatch_id=$1 AND scope=$2 AND epoch=$3 AND holder=$4 AND state='pending'`, d.ID, d.Owner.Scope, d.Owner.Epoch, d.Owner.Holder, reason)
	if err != nil {
		return canonical.Unavailable()
	}
	return tx.Commit(ctx)
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
		command, e := tx.Exec(ctx, `UPDATE acquisition_dispatch SET state='closed',stream_open=false,completed_at=clock_timestamp() WHERE dispatch_id=$1 AND scope=$2 AND epoch=$3 AND holder=$4 AND state='pending' AND streaming`, d.ID, d.Owner.Scope, d.Owner.Epoch, d.Owner.Holder)
		rows, err = command.RowsAffected(), e
	} else {
		if r.Status < 100 || r.Status > 599 || r.Headers == nil || r.Body == nil {
			return acquisition.ErrDispatchIdentity
		}
		headers, e := json.Marshal(r.Headers)
		if e != nil {
			return e
		}
		command, e := tx.Exec(ctx, `UPDATE acquisition_dispatch SET state='complete',stream_open=false,completed_at=clock_timestamp(),status=$5,headers=$6,body=$7 WHERE dispatch_id=$1 AND scope=$2 AND epoch=$3 AND holder=$4 AND state='pending' AND NOT streaming`, d.ID, d.Owner.Scope, d.Owner.Epoch, d.Owner.Holder, r.Status, headers, r.Body)
		rows, err = command.RowsAffected(), e
	}
	if err != nil || rows != 1 {
		if err != nil {
			return canonical.Unavailable()
		}
		return acquisition.ErrNotOwner
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

// The caller has durably retained the response before this operation. The live
// consumer may quarantine a cached receipt originally fetched by an old epoch;
// only this exact nonce/key can be retired, never its replacement.
func (a *Authority) FinishResponse(ctx context.Context, d acquisition.Dispatch, state string) error {
	if a.prefix != "acquisition:" || !a.accepts(d.Owner.Scope) || !validOwner(d.Owner) || uuid.Validate(d.ID) != nil || (state != "quarantined" && state != "consumed") {
		return acquisition.ErrDispatchIdentity
	}
	ctx, cancel := context.WithTimeout(ctx, a.budget)
	defer cancel()
	tx, err := a.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	r, now, err := lock(ctx, tx, d.Owner.Scope, "SHARE")
	if err != nil {
		return err
	}
	if !matches(r, d.Owner) || !live(r, now) {
		return stale()
	}
	tag, err := tx.Exec(ctx, `UPDATE acquisition_dispatch SET state=$6,stream_open=false,completed_at=clock_timestamp(),body=NULL,headers=NULL,last_failure=CASE WHEN $6='quarantined' THEN 'invalid_response' ELSE '' END WHERE dispatch_id=$1 AND scope=$2 AND lane=$3 AND request_key=$4 AND fingerprint=$5 AND state IN ('pending','complete',$6)`, d.ID, d.Owner.Scope, d.Lane, d.Key, d.Fingerprint, state)
	if err != nil {
		return canonical.Unavailable()
	}
	if tag.RowsAffected() != 1 {
		return acquisition.ErrNotOwner
	}
	if err = tx.Commit(ctx); err != nil {
		return canonical.Unavailable()
	}
	return a.confirmDispatch(ctx, d.ID, state)
}
