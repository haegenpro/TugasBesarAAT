package fieldreport

import (
	"aggregator/internal/canonical"
	"aggregator/internal/ingestion"
	"aggregator/internal/platform"
	"aggregator/internal/rawinput"
	"aggregator/internal/store"
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type Receipt struct {
	ReportID   string    `json:"report_id"`
	ReceiptID  string    `json:"receipt_id"`
	Status     string    `json:"status"`
	ReceivedAt time.Time `json:"received_at"`
}

type Submission struct {
	Submitter     string
	Key           string
	ContentType   string
	Body          []byte
	CorrelationID string
	Traceparent   *string
}

type Ledger struct {
	DB         *store.Store
	Durability ingestion.Durability
	Sender     rawinput.Sender
	Namespace  string
	Holder     string
}

type entry struct {
	Receipt
	submitter, fingerprint, contentType, messageID, correlationID, state string
	traceparent                                                       *string
	payload                                                           []byte
	position                                                          int64
}

var errPending = canonical.Fail(503, "acceptance_pending", "report acceptance is not yet confirmed; retry with the same Idempotency-Key")

func fingerprint(contentType string, body []byte) string {
	return rawinput.Hash(append([]byte(contentType+"\n"), body...))
}

func (l *Ledger) Accept(ctx context.Context, s Submission) (Receipt, error) {
	if s.Submitter == "" || !ValidKey(s.Key) {
		return Receipt{}, invalid("a printable Idempotency-Key of 1..128 characters is required")
	}
	if _, err := Parse(s.Body); err != nil {
		return Receipt{}, err
	}
	e, created, err := l.reserve(ctx, s)
	if err != nil {
		return Receipt{}, err
	}
	if !created && e.state != "pending" {
		platform.Add("field_reports_duplicate_total", 1)
		return e.receipt(), nil
	}
	if err = l.publish(ctx, e); err != nil {
		platform.Add("field_reports_pending_total", 1)
		return Receipt{}, errPending
	}
	if err = l.mark(ctx, e.ReceiptID, "pending", "accepted", ""); err != nil {
		return Receipt{}, errPending
	}
	platform.Add("field_reports_accepted_total", 1)
	return e.receipt(), nil
}

func (l *Ledger) reserve(ctx context.Context, s Submission) (entry, bool, error) {
	fp := fingerprint(s.ContentType, s.Body)
	tx, err := l.DB.Pool.Begin(ctx)
	if err != nil {
		return entry{}, false, canonical.Unavailable()
	}
	defer tx.Rollback(context.Background())
	if err = l.Durability.Prepare(ctx, tx); err != nil {
		return entry{}, false, err
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1||chr(31)||$2,7))`, s.Submitter, s.Key); err != nil {
		return entry{}, false, canonical.Unavailable()
	}
	e, err := scan(tx.QueryRow(ctx, selectEntry+` WHERE submitter=$1 AND report_id=$2`, s.Submitter, s.Key))
	if err == nil {
		if e.fingerprint != fp {
			return entry{}, false, canonical.Fail(409, "identity_conflict", "Idempotency-Key was already used with different content")
		}
		return e, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return entry{}, false, canonical.Unavailable()
	}
	e = entry{Receipt: Receipt{ReportID: s.Key, ReceiptID: uuid.NewString(), Status: "accepted", ReceivedAt: time.Now().UTC().Truncate(time.Microsecond)}, submitter: s.Submitter, fingerprint: fp, contentType: s.ContentType, correlationID: s.CorrelationID, traceparent: s.Traceparent, payload: s.Body, state: "pending"}
	if err = tx.QueryRow(ctx, `SELECT nextval('field_report_positions')`).Scan(&e.position); err != nil {
		return entry{}, false, canonical.Unavailable()
	}
	e.messageID = rawinput.ID(l.envelope(e))
	_, err = tx.Exec(ctx, `INSERT INTO field_report_ledger(submitter,report_id,receipt_id,fingerprint,content_type,payload,position,message_id,received_at,correlation_id,traceparent,state) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'pending')`, e.submitter, e.ReportID, e.ReceiptID, e.fingerprint, e.contentType, e.payload, e.position, e.messageID, e.ReceivedAt, e.correlationID, e.traceparent)
	if err != nil {
		return entry{}, false, canonical.Unavailable()
	}
	if err = tx.Commit(ctx); err != nil {
		return entry{}, false, errPending
	}
	if err = l.Durability.ConfirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM field_report_ledger WHERE receipt_id=$1)`, e.ReceiptID); err != nil {
		return entry{}, false, errPending
	}
	return e, true, nil
}

func (l *Ledger) envelope(e entry) rawinput.Envelope {
	ref := e.ReceiptID
	position := e.position
	env := rawinput.Envelope{Version: 1, Source: Source, Namespace: l.Namespace, Ref: &ref, Operation: "create", Position: &position, ReceivedAt: e.ReceivedAt.Format(time.RFC3339Nano), Payload: e.payload, Size: int64(len(e.payload)), Digest: rawinput.Hash(e.payload), ContentType: e.contentType, CorrelationID: e.correlationID, Traceparent: e.traceparent, OwnershipScope: "acceptance:" + Source, OwnershipEpoch: 1, OwnershipHolder: l.Holder}
	env.MessageID = rawinput.ID(env)
	return env
}

func (l *Ledger) publish(ctx context.Context, e entry) error {
	return l.Sender.Send(ctx, rawinput.Reports.Subject, l.envelope(e))
}

func (l *Ledger) mark(ctx context.Context, receipt, from, to, reason string) error {
	tag, err := l.DB.Pool.Exec(ctx, `UPDATE field_report_ledger SET state=$3,reason=NULLIF($4,''),updated_at=clock_timestamp() WHERE receipt_id=$1 AND state=$2`, receipt, from, to, reason)
	if err != nil {
		return canonical.Unavailable()
	}
	if tag.RowsAffected() == 0 {
		return nil
	}
	return l.Durability.ConfirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM field_report_ledger WHERE receipt_id=$1 AND state<>$2)`, receipt, from)
}

func (l *Ledger) Status(ctx context.Context, receipt, submitter string) (Receipt, error) {
	if _, err := uuid.Parse(receipt); err != nil {
		return Receipt{}, canonical.Fail(404, "not_found", "unknown receipt")
	}
	e, err := scan(l.DB.Pool.QueryRow(ctx, selectEntry+` WHERE receipt_id=$1 AND ($2='' OR submitter=$2)`, receipt, submitter))
	if errors.Is(err, pgx.ErrNoRows) {
		return Receipt{}, canonical.Fail(404, "not_found", "unknown receipt")
	}
	if err != nil {
		return Receipt{}, canonical.Unavailable()
	}
	r := e.receipt()
	if e.state == "processed" || e.state == "parked" {
		r.Status = e.state
	}
	return r, nil
}

func (l *Ledger) Republish(ctx context.Context, olderThan time.Duration) (int, error) {
	rows, err := l.DB.Pool.Query(ctx, selectEntry+` WHERE state='pending' AND received_at<now()-$1::interval ORDER BY received_at LIMIT 32`, olderThan.String())
	if err != nil {
		return 0, err
	}
	pending := []entry{}
	for rows.Next() {
		e, err := scan(rows)
		if err != nil {
			rows.Close()
			return 0, err
		}
		pending = append(pending, e)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return 0, err
	}
	done := 0
	for _, e := range pending {
		if err = l.publish(ctx, e); err != nil {
			return done, err
		}
		if err = l.mark(ctx, e.ReceiptID, "pending", "accepted", ""); err != nil {
			return done, err
		}
		done++
	}
	return done, nil
}

func (l *Ledger) RunOutbox(ctx context.Context) {
	failures := 0
	for ctx.Err() == nil {
		call, cancel := context.WithTimeout(ctx, 5*time.Second)
		n, err := l.Republish(call, 2*time.Second)
		cancel()
		delay := time.Second
		if err != nil {
			failures++
			delay = rawinput.Backoff(failures)
		} else {
			failures = 0
			platform.Add("field_reports_republished_total", float64(n))
		}
		if !rawinput.Sleep(ctx, delay) {
			return
		}
	}
}

const selectEntry = `SELECT submitter,report_id,receipt_id::text,fingerprint,content_type,payload,position,message_id,received_at,correlation_id,traceparent,state FROM field_report_ledger`

func scan(row pgx.Row) (entry, error) {
	var e entry
	err := row.Scan(&e.submitter, &e.ReportID, &e.ReceiptID, &e.fingerprint, &e.contentType, &e.payload, &e.position, &e.messageID, &e.ReceivedAt, &e.correlationID, &e.traceparent, &e.state)
	e.ReceivedAt = e.ReceivedAt.UTC()
	return e, err
}

func (e entry) receipt() Receipt {
	return Receipt{ReportID: e.ReportID, ReceiptID: e.ReceiptID, Status: "accepted", ReceivedAt: e.ReceivedAt}
}
