package store

import (
	"aggregator/internal/domain"
	"aggregator/internal/middleware"
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"time"
)

func scanStatus(row pgx.Row) (domain.EndpointStatus, error) {
	var p domain.EndpointStatus
	err := row.Scan(&p.Endpoint, &p.State, &p.LastSuccess, &p.LastAttempt, &p.StaleSince, &p.NextAttempt, &p.Failures, &p.Revision)
	return p, err
}

const statusColumns = `endpoint,state,last_success,last_attempt,stale_since,next_attempt,failures,revision`

func (s *Store) PollStatus(ctx context.Context, endpoint string) (domain.EndpointStatus, error) {
	ctx = middleware.EnsureCorrelationID(ctx)
	p, err := scanStatus(s.Pool.QueryRow(ctx, `SELECT `+statusColumns+` FROM source_poll_state WHERE endpoint=$1`, endpoint))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.EndpointStatus{Endpoint: endpoint, State: "unavailable"}, nil
	}
	return p, err
}

// PollStatuses returns every requested endpoint, including unavailable defaults for missing rows.
func (s *Store) PollStatuses(ctx context.Context, endpoints []string) (map[string]domain.EndpointStatus, error) {
	ctx = middleware.EnsureCorrelationID(ctx)
	out := make(map[string]domain.EndpointStatus, len(endpoints))
	for _, endpoint := range endpoints {
		out[endpoint] = domain.EndpointStatus{Endpoint: endpoint, State: "unavailable"}
	}
	rows, err := s.Pool.Query(ctx, `SELECT `+statusColumns+` FROM source_poll_state WHERE endpoint=ANY($1::text[])`, endpoints)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		p, err := scanStatus(rows)
		if err != nil {
			return nil, err
		}
		out[p.Endpoint] = p
	}
	return out, rows.Err()
}

// RecordAttempt replaces the endpoint state. started is the request start time;
// last_success advances only after the caller has committed its response data.
// Omitting started preserves the completion-time behavior for other callers.
func (s *Store) RecordAttempt(ctx context.Context, endpoint, cid string, success bool, interval, maxBackoff time.Duration, started ...time.Time) error {
	if len(started) > 1 {
		return errors.New("at most one poll attempt start is allowed")
	}
	ctx = middleware.WithCorrelationID(ctx, cid)
	tx, err := s.sourceTx(ctx, "poll:"+endpoint)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	p, err := scanStatus(tx.QueryRow(ctx, `SELECT `+statusColumns+` FROM source_poll_state WHERE endpoint=$1 FOR UPDATE`, endpoint))
	if errors.Is(err, pgx.ErrNoRows) {
		p = domain.EndpointStatus{Endpoint: endpoint, State: "unavailable"}
	} else if err != nil {
		return err
	}
	previous := p.State
	now := domain.Now()
	attemptedAt := now
	if len(started) > 0 {
		attemptedAt = started[0].UTC()
		if attemptedAt.IsZero() || attemptedAt.After(now) {
			return errors.New("poll attempt start must be nonzero and not in the future")
		}
	}
	// Concurrent one-shot diagnostics must not let an older request overwrite
	// a newer attempt's health or move its successful polling boundary backward.
	// A future stored timestamp is allowed to recover after a clock correction.
	if p.LastAttempt != nil && !p.LastAttempt.After(now) && attemptedAt.Before(*p.LastAttempt) {
		return tx.Commit(ctx)
	}
	p.LastAttempt = &attemptedAt
	delay := interval
	if success {
		p.State = "fresh"
		p.LastSuccess = &attemptedAt
		p.StaleSince = nil
		p.Failures = 0
	} else {
		p.Failures++
		p.State = "stale"
		if p.LastSuccess == nil {
			p.State = "unavailable"
		}
		if p.StaleSince == nil {
			p.StaleSince = &now
		}
		for i := 1; i < p.Failures && delay < maxBackoff; i++ {
			delay *= 2
		}
		if delay > maxBackoff {
			delay = maxBackoff
		}
	}
	next := now.Add(delay)
	p.NextAttempt = &next
	changed := previous != p.State || p.Revision == 0
	if changed {
		p.Revision++
	}
	_, err = tx.Exec(ctx, `INSERT INTO source_poll_state(endpoint,state,last_success,last_attempt,stale_since,next_attempt,failures,revision) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(endpoint) DO UPDATE SET state=excluded.state,last_success=excluded.last_success,last_attempt=excluded.last_attempt,stale_since=excluded.stale_since,next_attempt=excluded.next_attempt,failures=excluded.failures,revision=excluded.revision`, p.Endpoint, p.State, p.LastSuccess, p.LastAttempt, p.StaleSince, p.NextAttempt, p.Failures, p.Revision)
	if err != nil {
		return err
	}
	if changed {
		if err = queueLifecycle(ctx, tx, domain.LifecycleEvent{EventType: "source_status_changed", RecordID: endpoint, Revision: p.Revision, CorrelationID: cid, Status: &p}); err != nil {
			return err
		}
	}
	if endpoint == "BMKG_TSUNAMI" && !success {
		// A failed assessment preserves known hazards and moves open notices to unavailable.
		rows, err := tx.Query(ctx, `SELECT payload FROM report_notices WHERE source='BMKG' AND state<>'resolved' FOR UPDATE`)
		if err != nil {
			return err
		}
		notices := []domain.Notice{}
		for rows.Next() {
			var b []byte
			var n domain.Notice
			if err = rows.Scan(&b); err != nil {
				rows.Close()
				return err
			}
			if err = json.Unmarshal(b, &n); err != nil {
				rows.Close()
				return err
			}
			notices = append(notices, n)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, n := range notices {
			n.LastCheckedAt = &now
			if n.State != "assessment_unavailable" {
				n.State = "assessment_unavailable"
				n.Revision++
				if err = saveNotice(ctx, tx, n, cid, nil); err != nil {
					return err
				}
			} else {
				b, _ := json.Marshal(n)
				if _, err = tx.Exec(ctx, `UPDATE report_notices SET payload=$1 WHERE notice_id=$2`, b, n.NoticeID); err != nil {
					return err
				}
			}
		}
	}
	return tx.Commit(ctx)
}
func (s *Store) Notices(ctx context.Context, state string) ([]domain.Notice, error) {
	ctx = middleware.EnsureCorrelationID(ctx)
	q := `SELECT payload FROM report_notices`
	args := []any{}
	if state == "" || state == "open" {
		q += ` WHERE state<>'resolved'`
	} else if state != "all" {
		q += ` WHERE state=$1`
		args = append(args, state)
	}
	q += ` ORDER BY payload->>'reported_at' DESC`
	rows, err := s.Pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.Notice{}
	for rows.Next() {
		var b []byte
		var n domain.Notice
		if err = rows.Scan(&b); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(b, &n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}
func (s *Store) Notice(ctx context.Context, id string) (domain.Notice, []domain.LifecycleEvent, error) {
	ctx = middleware.EnsureCorrelationID(ctx)
	var b []byte
	var n domain.Notice
	err := s.Pool.QueryRow(ctx, `SELECT payload FROM report_notices WHERE notice_id=$1`, id).Scan(&b)
	if errors.Is(err, pgx.ErrNoRows) {
		return n, nil, ErrNotFound
	}
	if err != nil {
		return n, nil, err
	}
	if err = json.Unmarshal(b, &n); err != nil {
		return n, nil, err
	}
	rows, err := s.Pool.Query(ctx, `SELECT event_payload FROM report_lifecycle_events WHERE record_id=$1 ORDER BY revision,created_at`, id)
	if err != nil {
		return n, nil, err
	}
	defer rows.Close()
	out := []domain.LifecycleEvent{}
	for rows.Next() {
		var b []byte
		var e domain.LifecycleEvent
		if err = rows.Scan(&b); err != nil {
			return n, nil, err
		}
		if err = json.Unmarshal(b, &e); err != nil {
			return n, nil, err
		}
		out = append(out, e)
	}
	return n, out, rows.Err()
}
func (s *Store) DeliverLifecycle(ctx context.Context, publish func(context.Context, domain.LifecycleEvent) error) (bool, error) {
	ctx = middleware.EnsureCorrelationID(ctx)
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var id string
	var b []byte
	err = tx.QueryRow(ctx, `SELECT o.event_id::text,e.event_payload FROM report_lifecycle_outbox o JOIN report_lifecycle_events e USING(event_id) ORDER BY o.queued_at,o.event_id LIMIT 1 FOR UPDATE OF o SKIP LOCKED`).Scan(&id, &b)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var e domain.LifecycleEvent
	if err = json.Unmarshal(b, &e); err != nil {
		return true, err
	}
	ctx = middleware.WithCorrelationID(ctx, e.CorrelationID)
	if err = publish(ctx, e); err != nil {
		return true, err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM report_lifecycle_outbox WHERE event_id=$1`, id); err != nil {
		return true, err
	}
	return true, tx.Commit(ctx)
}
