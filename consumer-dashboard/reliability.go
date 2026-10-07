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
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"modernc.org/sqlite"
)

var errCorruptPayload = errors.New("corrupt message")
var errConsumerIncompatible = errors.New("consumer cannot handle registered schema")
var errLocalStore = errors.New("local SQLite store unavailable")

const (
	unknownSchemaWindow  = 5 * time.Minute
	unknownStormLimit    = 10
	constraintLimit      = 3
	constraintStormLimit = 10
	corruptStormLimit    = 10
)

func openDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	statements := []string{
		`PRAGMA busy_timeout=1000`,
		`CREATE TABLE IF NOT EXISTS records (hazard_id TEXT PRIMARY KEY, payload TEXT NOT NULL, correlation_id TEXT NOT NULL, processed_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS processed_messages (stream TEXT NOT NULL, sequence INTEGER NOT NULL, digest TEXT NOT NULL, processed_at TEXT NOT NULL, PRIMARY KEY(stream, sequence))`,
		`CREATE TABLE IF NOT EXISTS schema_cache (registry TEXT NOT NULL, id INTEGER NOT NULL, schema TEXT NOT NULL, PRIMARY KEY(registry, id))`,
		`CREATE TABLE IF NOT EXISTS schema_retry (stream TEXT NOT NULL, sequence INTEGER NOT NULL, schema_id INTEGER NOT NULL, first_seen_ms INTEGER NOT NULL, last_seen_ms INTEGER NOT NULL, attempts INTEGER NOT NULL, PRIMARY KEY(stream, sequence))`,
		`CREATE INDEX IF NOT EXISTS schema_retry_recent ON schema_retry(first_seen_ms)`,
		`CREATE TABLE IF NOT EXISTS constraint_retry (stream TEXT NOT NULL, sequence INTEGER NOT NULL, first_seen_ms INTEGER NOT NULL, attempts INTEGER NOT NULL, PRIMARY KEY(stream, sequence))`,
		`CREATE INDEX IF NOT EXISTS constraint_retry_recent ON constraint_retry(first_seen_ms)`,
		`CREATE TABLE IF NOT EXISTS corrupt_messages (stream TEXT NOT NULL, sequence INTEGER NOT NULL, first_seen_ms INTEGER NOT NULL, PRIMARY KEY(stream, sequence))`,
		`CREATE INDEX IF NOT EXISTS corrupt_messages_recent ON corrupt_messages(first_seen_ms)`,
		`CREATE TABLE IF NOT EXISTS record_versions (record_id TEXT PRIMARY KEY, revision INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS lifecycle_events (event_id TEXT PRIMARY KEY, record_id TEXT NOT NULL, revision INTEGER NOT NULL, payload TEXT NOT NULL, processed_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS notice_records (notice_id TEXT PRIMARY KEY, revision INTEGER NOT NULL, payload TEXT NOT NULL, processed_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS source_status_records (endpoint TEXT PRIMARY KEY, revision INTEGER NOT NULL, payload TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS notifications (event_id TEXT PRIMARY KEY, kind TEXT NOT NULL, payload TEXT NOT NULL, processed_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS consumer_control (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	return db, nil
}

func messageDigest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (a *app) alreadyProcessed(ctx context.Context, meta *jetstream.MsgMetadata, raw []byte) (bool, error) {
	var digest string
	err := a.db.QueryRowContext(ctx, `SELECT digest FROM processed_messages WHERE stream=? AND sequence=?`, meta.Stream, meta.Sequence.Stream).Scan(&digest)
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

// save commits the broker identity and projection as one SQLite transaction.
func (a *app) save(ctx context.Context, meta *jetstream.MsgMetadata, raw []byte, e envelope) error {
	if e.Lifecycle != nil {
		return a.saveLifecycle(ctx, meta, raw, *e.Lifecycle)
	}
	var output any = e.Payload
	if a.cfg.Role == "notifier" {
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
	result, err := tx.ExecContext(ctx, `INSERT INTO processed_messages(stream,sequence,digest,processed_at) VALUES (?,?,?,?) ON CONFLICT(stream,sequence) DO NOTHING`, meta.Stream, meta.Sequence.Stream, messageDigest(raw), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if inserted == 0 {
		var digest string
		if err := tx.QueryRowContext(ctx, `SELECT digest FROM processed_messages WHERE stream=? AND sequence=?`, meta.Stream, meta.Sequence.Stream).Scan(&digest); err != nil {
			return err
		}
		if digest != messageDigest(raw) {
			return fmt.Errorf("%w: stream sequence %s/%d was reused with different bytes", errConsumerIncompatible, meta.Stream, meta.Sequence.Stream)
		}
		return tx.Commit()
	}
	var version int64
	versionErr := tx.QueryRowContext(ctx, `SELECT revision FROM record_versions WHERE record_id=?`, "hazard:"+e.HazardID).Scan(&version)
	if versionErr != nil && !errors.Is(versionErr, sql.ErrNoRows) {
		return versionErr
	}
	// An initial canonical message is revision 1. A lifecycle correction may arrive first.
	if version > 1 {
		return tx.Commit()
	}
	var previous string
	err = tx.QueryRowContext(ctx, `SELECT payload FROM records WHERE hazard_id=?`, e.HazardID).Scan(&previous)
	if err == nil && previous != string(data) {
		return fmt.Errorf("%w: hazard %s changed without an event version", errConsumerIncompatible, e.HazardID)
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := tx.ExecContext(ctx, `INSERT INTO records(hazard_id,payload,correlation_id,processed_at) VALUES (?,?,?,?)`, e.HazardID, string(data), e.CorrelationID, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO record_versions(record_id,revision) VALUES(?,1) ON CONFLICT(record_id) DO NOTHING`, "hazard:"+e.HazardID); err != nil {
		return err
	}
	return tx.Commit()
}

type retryState struct {
	FirstSeen time.Time
	Attempts  int
	Recent    int
}

func (a *app) recordUnknown(ctx context.Context, meta *jetstream.MsgMetadata, schemaID int, now time.Time) (retryState, error) {
	var firstSeen int64
	var attempts int
	ms := now.UnixMilli()
	err := a.db.QueryRowContext(ctx, `INSERT INTO schema_retry(stream,sequence,schema_id,first_seen_ms,last_seen_ms,attempts) VALUES (?,?,?,?,?,1)
		ON CONFLICT(stream,sequence) DO UPDATE SET attempts=attempts+1,last_seen_ms=excluded.last_seen_ms
		RETURNING first_seen_ms,attempts`, meta.Stream, meta.Sequence.Stream, schemaID, ms, ms).Scan(&firstSeen, &attempts)
	if err != nil {
		return retryState{}, err
	}
	var recent int
	err = a.db.QueryRowContext(ctx, `SELECT count(*) FROM schema_retry WHERE first_seen_ms>=?`, now.Add(-time.Minute).UnixMilli()).Scan(&recent)
	return retryState{FirstSeen: time.UnixMilli(firstSeen), Attempts: attempts, Recent: recent}, err
}

func (a *app) recordConstraint(ctx context.Context, meta *jetstream.MsgMetadata, now time.Time) (retryState, error) {
	var firstSeen int64
	var attempts int
	err := a.db.QueryRowContext(ctx, `INSERT INTO constraint_retry(stream,sequence,first_seen_ms,attempts) VALUES (?,?,?,1)
		ON CONFLICT(stream,sequence) DO UPDATE SET attempts=attempts+1 RETURNING first_seen_ms,attempts`, meta.Stream, meta.Sequence.Stream, now.UnixMilli()).Scan(&firstSeen, &attempts)
	if err != nil {
		return retryState{}, err
	}
	var recent int
	err = a.db.QueryRowContext(ctx, `SELECT count(*) FROM constraint_retry WHERE first_seen_ms>=?`, now.Add(-time.Minute).UnixMilli()).Scan(&recent)
	return retryState{FirstSeen: time.UnixMilli(firstSeen), Attempts: attempts, Recent: recent}, err
}

func (a *app) recordCorrupt(ctx context.Context, meta *jetstream.MsgMetadata, now time.Time) (int, error) {
	if _, err := a.db.ExecContext(ctx, `INSERT INTO corrupt_messages(stream,sequence,first_seen_ms) VALUES (?,?,?) ON CONFLICT(stream,sequence) DO NOTHING`, meta.Stream, meta.Sequence.Stream, now.UnixMilli()); err != nil {
		return 0, err
	}
	var recent int
	err := a.db.QueryRowContext(ctx, `SELECT count(*) FROM corrupt_messages WHERE first_seen_ms>=?`, now.Add(-time.Minute).UnixMilli()).Scan(&recent)
	return recent, err
}

func (a *app) clearRetries(ctx context.Context, meta *jetstream.MsgMetadata) error {
	for _, table := range []string{"schema_retry", "constraint_retry"} {
		if _, err := a.db.ExecContext(ctx, `DELETE FROM `+table+` WHERE stream=? AND sequence=?`, meta.Stream, meta.Sequence.Stream); err != nil {
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
	if _, err := a.db.ExecContext(ctx, `INSERT INTO consumer_control(key,value) VALUES ('halt_reason',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, reason); err != nil {
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
	var reason string
	err := a.db.QueryRowContext(ctx, `SELECT value FROM consumer_control WHERE key='halt_reason'`).Scan(&reason)
	if errors.Is(err, sql.ErrNoRows) {
		a.reason = ""
		a.halted.Store(false)
		return nil
	}
	if err != nil {
		a.halted.Store(true)
		return err
	}
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
	var sqliteErr *sqlite.Error
	if errors.As(err, &sqliteErr) {
		switch sqliteErr.Code() & 0xff {
		case 5, 6: // SQLITE_BUSY, SQLITE_LOCKED
			return storeTransient
		case 19: // SQLITE_CONSTRAINT
			return storeConstraint
		case 7, 8, 10, 11, 13, 14, 26: // NOMEM, READONLY, IOERR, CORRUPT, FULL, CANTOPEN, NOTADB
			return storeEnvironmental
		}
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
