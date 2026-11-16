package fieldreport

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/canonical"
	"aggregator/internal/platform"
	"aggregator/internal/rawinput"
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go/jetstream"
)

type Consumer struct {
	Ledger   *Ledger
	Stream   jetstream.Consumer
	Registry *avrowire.Client
	Reader   *avrowire.Reader
	Logger   *slog.Logger
}

func (c *Consumer) Run(ctx context.Context) {
	failures := 0
	for ctx.Err() == nil {
		batch, err := c.Stream.Fetch(32, jetstream.FetchMaxWait(time.Second))
		if err == nil {
			for msg := range batch.Messages() {
				if err = c.handle(ctx, msg); err != nil {
					break
				}
			}
			if err == nil {
				err = batch.Error()
			}
		}
		if err == nil || errors.Is(err, jetstream.ErrNoMessages) || errors.Is(err, context.DeadlineExceeded) {
			failures = 0
			continue
		}
		failures++
		c.Logger.Warn("field report delivery pending", "error", err)
		if !rawinput.Sleep(ctx, rawinput.Backoff(failures)) {
			return
		}
	}
}

func (c *Consumer) handle(ctx context.Context, msg jetstream.Msg) error {
	call, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	e, err := rawinput.Decode(call, c.Registry, c.Reader, msg.Data())
	if err != nil {
		var problem *canonical.Error
		if errors.As(err, &problem) && problem.Status < 500 {
			c.Logger.Error("unreadable field report envelope terminated", "error", err)
			return msg.Term()
		}
		_ = msg.NakWithDelay(time.Second)
		return err
	}
	if e.Source != Source || e.Ref == nil {
		return msg.Term()
	}
	if err = c.Ledger.project(call, *e.Ref, e.Payload); err != nil {
		var problem *canonical.Error
		if errors.As(err, &problem) && problem.Status < 500 {
			c.Logger.Error("field report without ledger entry terminated", "receipt_id", *e.Ref, "error", err)
			return msg.Term()
		}
		_ = msg.NakWithDelay(time.Second)
		return err
	}
	platform.Add("field_reports_processed_total", 1)
	return msg.DoubleAck(call)
}

func (l *Ledger) project(ctx context.Context, receipt string, body []byte) error {
	r, parseErr := Parse(body)
	tx, err := l.DB.Pool.Begin(ctx)
	if err != nil {
		return canonical.Unavailable()
	}
	defer tx.Rollback(context.Background())
	if err = l.Durability.Prepare(ctx, tx); err != nil {
		return err
	}
	var submitter, report, state string
	err = tx.QueryRow(ctx, `SELECT submitter,report_id,state FROM field_report_ledger WHERE receipt_id=$1 FOR UPDATE`, receipt).Scan(&submitter, &report, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return canonical.Fail(409, "unknown_receipt", "field report envelope has no ledger entry")
	}
	if err != nil {
		return canonical.Unavailable()
	}
	if state == "processed" || state == "parked" {
		return tx.Rollback(ctx)
	}
	if parseErr != nil {
		_, err = tx.Exec(ctx, `UPDATE field_report_ledger SET state='parked',reason=$2,updated_at=clock_timestamp() WHERE receipt_id=$1`, receipt, parseErr.Error())
	} else {
		_, err = tx.Exec(ctx, `INSERT INTO field_reports(receipt_id,submitter,report_id,report_type,occurred_at,latitude,longitude,description,attributes) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(receipt_id) DO NOTHING`, receipt, submitter, report, r.Type, r.OccurredAt, r.Latitude, r.Longitude, r.Description, []byte(r.Attributes))
		if err == nil {
			_, err = tx.Exec(ctx, `UPDATE field_report_ledger SET state='processed',updated_at=clock_timestamp() WHERE receipt_id=$1`, receipt)
		}
	}
	if err != nil {
		return canonical.Unavailable()
	}
	if err = tx.Commit(ctx); err != nil {
		return canonical.Unavailable()
	}
	return l.Durability.ConfirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM field_report_ledger WHERE receipt_id=$1 AND state IN ('processed','parked'))`, receipt)
}
