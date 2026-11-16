package coastal

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"aggregator/internal/canonical"
	"aggregator/internal/ingestion"
	"aggregator/internal/rawinput"
	"github.com/jackc/pgx/v5"
)

// Repository uses only the ingestion-owned database. Standbys serve the same
// persisted status and expected-warning identities as the acquisition leader.
type Repository struct {
	Source     *ingestion.SourceStore
	StaleAfter time.Duration
}

func (r *Repository) Now(ctx context.Context) (time.Time, error) {
	var now time.Time
	err := r.Source.DB.Pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now)
	return now.UTC(), err
}
func (r *Repository) begin(ctx context.Context, owner *canonical.Ownership) (pgx.Tx, error) {
	tx, err := r.Source.DB.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if err = r.Source.Durability.Prepare(ctx, tx); err == nil && owner != nil {
		err = r.Source.Fence.Protect(ctx, tx, *owner, []canonical.Mutation{{Source: Source}})
	}
	if err != nil {
		_ = tx.Rollback(context.Background())
		return nil, err
	}
	return tx, nil
}
func (r *Repository) SaveStatus(ctx context.Context, owner canonical.Ownership, v Snapshot) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tx, err := r.begin(ctx, &owner)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, `INSERT INTO coastal_source_status(namespace,snapshot) VALUES($1,$2) ON CONFLICT(namespace) DO UPDATE SET snapshot=excluded.snapshot,updated_at=clock_timestamp()`, r.Source.Namespace, b); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	return r.Source.Durability.ConfirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM coastal_source_status WHERE namespace=$1 AND snapshot=$2::jsonb)`, r.Source.Namespace, b)
}
func (r *Repository) Status(ctx context.Context) (Snapshot, error) {
	var b []byte
	var now time.Time
	err := r.Source.DB.Pool.QueryRow(ctx, `SELECT snapshot,clock_timestamp() FROM coastal_source_status WHERE namespace=$1`, r.Source.Namespace).Scan(&b, &now)
	if errors.Is(err, pgx.ErrNoRows) {
		return Snapshot{State: "unavailable"}, nil
	}
	if err != nil {
		return Snapshot{}, err
	}
	var v Snapshot
	if err = json.Unmarshal(b, &v); err != nil {
		return v, err
	}
	// Evaluate expiry using the database clock on every replica. Never compare a
	// leader's wall clock with a standby's possibly skewed wall clock.
	local := &Status{StaleAfter: r.StaleAfter, Observations: &Breaker{}, Stations: &Breaker{}}
	local.Restore(v)
	out := local.SnapshotAt(now)
	out.ObservationsBreaker, out.StationsBreaker = v.ObservationsBreaker, v.StationsBreaker
	return out, nil
}

type ExpectedWarning struct {
	Sequence        int64     `json:"sequence"`
	NotificationID  string    `json:"notification_id"`
	FirstReceivedAt time.Time `json:"first_received_at"`
	Rejected        bool      `json:"rejected"`
}
type ExpectationPage struct {
	Items         []ExpectedWarning `json:"items"`
	Through       int64             `json:"through"`
	AsOf          time.Time         `json:"as_of"`
	Next          int64             `json:"next"`
	CoverageSince *time.Time        `json:"coverage_since"`
}

func (r *Repository) StartCoverage(ctx context.Context, owner canonical.Ownership) error {
	tx, err := r.begin(ctx, &owner)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, `INSERT INTO coastal_warning_coverage(namespace) VALUES($1) ON CONFLICT DO NOTHING`, r.Source.Namespace); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Candidate uses only the explicit source critical flag, before validation or
// broker publication. Invalid critical input remains visible in the denominator.
func Candidate(e rawinput.Envelope) (Observation, bool) {
	var v map[string]any
	if json.Unmarshal(e.Payload, &v) != nil || v["critical"] != true {
		return Observation{}, false
	}
	id, _ := v["observation_id"].(string)
	if id == "" {
		id = "invalid:" + e.Digest
	}
	return Observation{ID: id, Critical: true}, true
}
func (r *Repository) Expect(ctx context.Context, owner canonical.Ownership, e rawinput.Envelope) error {
	if e.Source != Source || e.Namespace != r.Source.Namespace {
		return canonical.Fail(400, "invalid_request", "unexpected coastal source scope")
	}
	o, ok := Candidate(e)
	if !ok {
		return nil
	}
	op := OperationID(o, e.Digest)
	id := NotificationID(e.Namespace, o, op)
	tx, err := r.begin(ctx, &owner)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	observed := ""
	if e.EventTime != nil {
		observed = *e.EventTime
	}
	if _, err = tx.Exec(ctx, `INSERT INTO coastal_warning_expected(notification_id,namespace,observation_id,operation_id,payload_sha256,first_received_at,observed_at) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(notification_id) DO NOTHING`, id, e.Namespace, o.ID, op, e.Digest, e.ReceivedAt, observed); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	return r.Source.Durability.ConfirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM coastal_warning_expected WHERE notification_id=$1)`, id)
}
func (r *Repository) Reject(ctx context.Context, e rawinput.Envelope, reason string) error {
	o, ok := Candidate(e)
	if !ok {
		return nil
	}
	id := NotificationID(e.Namespace, o, OperationID(o, e.Digest))
	tx, err := r.begin(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	tag, err := tx.Exec(ctx, `UPDATE coastal_warning_expected SET rejected_at=coalesce(rejected_at,clock_timestamp()),rejection_reason=$2 WHERE notification_id=$1`, id, reason)
	if err != nil {
		return err
	}
	// Pre-upgrade raw inputs have no acquisition ledger entry. Their quarantine
	// still commits; coverage metadata prevents certifying historical windows.
	if tag.RowsAffected() == 0 {
		return tx.Commit(ctx)
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	return r.Source.Durability.ConfirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM coastal_warning_expected WHERE notification_id=$1 AND rejected_at IS NOT NULL)`, id)
}
func (r *Repository) Expectations(ctx context.Context, since time.Time, after, through int64, asOf time.Time) (ExpectationPage, error) {
	out := ExpectationPage{Items: []ExpectedWarning{}, Through: through, AsOf: asOf}
	if asOf.IsZero() {
		if err := r.Source.DB.Pool.QueryRow(ctx, `SELECT coalesce(max(sequence),0),clock_timestamp() FROM coastal_warning_expected WHERE namespace=$1`, r.Source.Namespace).Scan(&out.Through, &out.AsOf); err != nil {
			return out, err
		}
	}
	if err := r.Source.DB.Pool.QueryRow(ctx, `SELECT started_at FROM coastal_warning_coverage WHERE namespace=$1`, r.Source.Namespace).Scan(&out.CoverageSince); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return out, err
	}
	rows, err := r.Source.DB.Pool.Query(ctx, `SELECT sequence,notification_id,first_received_at,rejected_at IS NOT NULL AND rejected_at<=$5 FROM coastal_warning_expected WHERE namespace=$1 AND sequence>$2 AND sequence<=$3 AND first_received_at>=$4 AND first_received_at<=$5 ORDER BY sequence LIMIT 500`, r.Source.Namespace, after, out.Through, since, out.AsOf)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var v ExpectedWarning
		if err = rows.Scan(&v.Sequence, &v.NotificationID, &v.FirstReceivedAt, &v.Rejected); err != nil {
			return out, err
		}
		out.Items = append(out.Items, v)
		out.Next = v.Sequence
	}
	return out, rows.Err()
}
