package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	"consumerdashboard/internal/platform"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats.go/jetstream"
)

var errCorruptPayload = errors.New("corrupt message")
var errConsumerIncompatible = errors.New("consumer cannot handle registered schema")
var errLocalStore = errors.New("local PostgreSQL store unavailable")

const (
	unknownSchemaWindow  = 5 * time.Minute
	unknownStormLimit    = 10
	constraintLimit      = 3
	constraintStormLimit = 10
	corruptStormLimit    = 10
)

func openDB(path string) (*sql.DB, error) {
	cfg, err := pgx.ParseConfig(path)
	if err != nil {
		return nil, errors.New("invalid PostgreSQL configuration")
	}
	cfg.Tracer = platform.SQLTracer{}
	cfg.RuntimeParams["statement_timeout"] = strconv.FormatInt(platform.Duration("DB_STATEMENT_TIMEOUT", 5*time.Second).Milliseconds(), 10)
	cfg.RuntimeParams["lock_timeout"] = strconv.FormatInt(platform.Duration("DB_LOCK_TIMEOUT", 2*time.Second).Milliseconds(), 10)
	db := stdlib.OpenDB(*cfg)
	configurePool(db)
	return db, nil
}

func messageDigest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (a *projectionStore) alreadyProcessed(ctx context.Context, meta *jetstream.MsgMetadata, raw []byte) (bool, error) {
	var digest string
	err := a.db.QueryRowContext(ctx, `SELECT digest FROM processed_messages WHERE stream=$1 AND sequence=$2`, meta.Stream, meta.Sequence.Stream).Scan(&digest)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if digest != messageDigest(raw) {
		return false, fmt.Errorf("%w: stream sequence %s/%d was reused with different bytes", errConsumerIncompatible, meta.Stream, meta.Sequence.Stream)
	}
	return true, nil
}

// save commits the broker identity and projection as one PostgreSQL transaction.
func (a *projectionStore) save(ctx context.Context, meta *jetstream.MsgMetadata, raw []byte, e envelope) error {
	if e.Lifecycle != nil {
		return a.saveLifecycle(ctx, meta, raw, *e.Lifecycle)
	}
	var output any = e.Payload
	if a.role == "notifier" {
		output = map[string]any{"hazard_id": e.HazardID, "source": e.Payload.Source, "severity": e.Payload.Severity, "area_name": e.Payload.AreaName, "occurred_at": e.Payload.OccurredAt}
	}
	data, err := json.Marshal(output)
	if err != nil {
		return fmt.Errorf("%w: encode projection: %v", errConsumerIncompatible, err)
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = lockApplication(ctx, tx, "message:"+meta.Stream+":"+strconv.FormatUint(meta.Sequence.Stream, 10), "hazard:"+e.HazardID); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO processed_messages(stream,sequence,digest,processed_at) VALUES ($1,$2,$3,$4) ON CONFLICT(stream,sequence) DO NOTHING`, meta.Stream, meta.Sequence.Stream, messageDigest(raw), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if inserted == 0 {
		var digest string
		if err := tx.QueryRowContext(ctx, `SELECT digest FROM processed_messages WHERE stream=$1 AND sequence=$2`, meta.Stream, meta.Sequence.Stream).Scan(&digest); err != nil {
			return err
		}
		if digest != messageDigest(raw) {
			return fmt.Errorf("%w: stream sequence %s/%d was reused with different bytes", errConsumerIncompatible, meta.Stream, meta.Sequence.Stream)
		}
		return tx.Commit()
	}
	var version int64
	versionErr := tx.QueryRowContext(ctx, `SELECT revision FROM record_versions WHERE record_id=$1`, "hazard:"+e.HazardID).Scan(&version)
	if versionErr != nil && !errors.Is(versionErr, sql.ErrNoRows) {
		return versionErr
	}
	// An initial canonical message is revision 1. A lifecycle correction may arrive first.
	if version > 1 {
		return tx.Commit()
	}
	var previous string
	err = tx.QueryRowContext(ctx, `SELECT payload FROM records WHERE hazard_id=$1`, e.HazardID).Scan(&previous)
	if err == nil && previous != string(data) {
		return fmt.Errorf("%w: hazard %s changed without an event version", errConsumerIncompatible, e.HazardID)
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := tx.ExecContext(ctx, `INSERT INTO records(hazard_id,payload,correlation_id,processed_at) VALUES ($1,$2,$3,$4)`, e.HazardID, string(data), e.CorrelationID, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO record_versions(record_id,revision) VALUES($1,1) ON CONFLICT(record_id) DO NOTHING`, "hazard:"+e.HazardID); err != nil {
		return err
	}
	return tx.Commit()
}

type retryState struct {
	FirstSeen time.Time
	Attempts  int
	Recent    int
}

func (a *projectionStore) recordUnknown(ctx context.Context, meta *jetstream.MsgMetadata, schemaID int, now time.Time) (retryState, error) {
	var firstSeen int64
	var attempts int
	ms := now.UnixMilli()
	err := a.db.QueryRowContext(ctx, `INSERT INTO schema_retry(stream,sequence,schema_id,first_seen_ms,last_seen_ms,attempts) VALUES ($1,$2,$3,$4,$5,1)
		ON CONFLICT(stream,sequence) DO UPDATE SET attempts=schema_retry.attempts+1,last_seen_ms=excluded.last_seen_ms
		RETURNING first_seen_ms,attempts`, meta.Stream, meta.Sequence.Stream, schemaID, ms, ms).Scan(&firstSeen, &attempts)
	if err != nil {
		return retryState{}, err
	}
	var recent int
	err = a.db.QueryRowContext(ctx, `SELECT count(*) FROM schema_retry WHERE first_seen_ms>=$1`, now.Add(-time.Minute).UnixMilli()).Scan(&recent)
	return retryState{FirstSeen: time.UnixMilli(firstSeen), Attempts: attempts, Recent: recent}, err
}

func (a *projectionStore) recordConstraint(ctx context.Context, meta *jetstream.MsgMetadata, now time.Time) (retryState, error) {
	var firstSeen int64
	var attempts int
	err := a.db.QueryRowContext(ctx, `INSERT INTO constraint_retry(stream,sequence,first_seen_ms,attempts) VALUES ($1,$2,$3,1)
		ON CONFLICT(stream,sequence) DO UPDATE SET attempts=constraint_retry.attempts+1 RETURNING first_seen_ms,attempts`, meta.Stream, meta.Sequence.Stream, now.UnixMilli()).Scan(&firstSeen, &attempts)
	if err != nil {
		return retryState{}, err
	}
	var recent int
	err = a.db.QueryRowContext(ctx, `SELECT count(*) FROM constraint_retry WHERE first_seen_ms>=$1`, now.Add(-time.Minute).UnixMilli()).Scan(&recent)
	return retryState{FirstSeen: time.UnixMilli(firstSeen), Attempts: attempts, Recent: recent}, err
}

func (a *projectionStore) recordCorrupt(ctx context.Context, meta *jetstream.MsgMetadata, now time.Time) (int, error) {
	if _, err := a.db.ExecContext(ctx, `INSERT INTO corrupt_messages(stream,sequence,first_seen_ms) VALUES ($1,$2,$3) ON CONFLICT(stream,sequence) DO NOTHING`, meta.Stream, meta.Sequence.Stream, now.UnixMilli()); err != nil {
		return 0, err
	}
	var recent int
	err := a.db.QueryRowContext(ctx, `SELECT count(*) FROM corrupt_messages WHERE first_seen_ms>=$1`, now.Add(-time.Minute).UnixMilli()).Scan(&recent)
	return recent, err
}

func (a *projectionStore) clearRetries(ctx context.Context, meta *jetstream.MsgMetadata) error {
	for _, table := range []string{"schema_retry", "constraint_retry"} {
		if _, err := a.db.ExecContext(ctx, `DELETE FROM `+table+` WHERE stream=$1 AND sequence=$2`, meta.Stream, meta.Sequence.Stream); err != nil {
			return err
		}
	}
	return nil
}

func (a *app) halt(reason string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.generation++
	a.reason = reason
	a.halted.Store(true)
	a.ready.Store(false)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.controls().Halt(ctx, reason); err != nil {
		// A full or corrupt DB can prevent this flag from being persisted; keep the in-process stop.
		a.haltSticky.Store(true)
		slog.Error("cannot persist consumer halt", "service", a.cfg.Role, "error", err)
	}
}

func (a *app) refreshHalt(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.haltSticky.Load() {
		a.halted.Store(true)
		return nil
	}
	state, err := a.controls().State(ctx)
	if err != nil {
		a.halted.Store(true)
		return err
	}
	if !state.Exists {
		a.reason = ""
		a.halted.Store(false)
		return nil
	}
	reason := state.Reason

	if !a.halted.Load() {
		slog.Error("consumer remains halted; operator action required", "service", a.cfg.Role, "reason", reason)
	}
	if a.reason != reason || !a.halted.Load() {
		a.generation++
	}
	a.reason = reason
	a.halted.Store(true)
	return nil
}

type storeFailure int

const (
	storeTransient storeFailure = iota
	storeEnvironmental
	storeConstraint
)

func classifyStoreFailure(err error) storeFailure {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && len(pgErr.Code) >= 2 {
		switch pgErr.Code[:2] {
		case "40", "08", "53", "55", "57":
			return storeTransient
		case "23":
			return storeConstraint
		}
	}

	var networkError net.Error
	if errors.As(err, &networkError) || pgconn.SafeToRetry(err) || pgconn.Timeout(err) {
		return storeTransient
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return storeTransient
	}
	return storeEnvironmental
}

func retryDelay(attempt int, sequence uint64) time.Duration {
	base := 5 * time.Second
	for i := 1; i < attempt && base < time.Minute; i++ {
		base *= 2
		if base > time.Minute {
			base = time.Minute
		}
	}
	// Stable per-message jitter avoids synchronized retries after a registry restart.
	percent := 80 + int((sequence+uint64(attempt)*17)%41)
	return time.Duration(int64(base) * int64(percent) / 100)
}
