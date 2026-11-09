package main

import (
	"context"
	"database/sql"
)

type HaltState struct {
	Reason  string
	Version int64
	Exists  bool
}
type Controls interface {
	State(context.Context) (HaltState, error)
	Halt(context.Context, string) error
	Resume(context.Context, HaltState, []byte) error
}
type postgresControl struct{ db *sql.DB }
type controlReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readHalt(ctx context.Context, db controlReader) (HaltState, error) {
	var reason sql.NullString
	var s HaltState
	err := db.QueryRowContext(ctx, `SELECT (SELECT value FROM consumer_control WHERE key='halt_reason'),COALESCE((SELECT value::bigint FROM consumer_control WHERE key='halt_version'),0)`).Scan(&reason, &s.Version)
	s.Reason = reason.String
	s.Exists = reason.Valid
	return s, err
}
func (s postgresControl) State(ctx context.Context) (HaltState, error) { return readHalt(ctx, s.db) }
func (s postgresControl) Halt(ctx context.Context, reason string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = lockApplication(ctx, tx, "consumer-control"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO consumer_control(key,value) VALUES('halt_version','1') ON CONFLICT(key) DO UPDATE SET value=(consumer_control.value::bigint+1)::text`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO consumer_control(key,value) VALUES('halt_reason',$1) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, reason); err != nil {
		return err
	}
	return tx.Commit()
}
func (s postgresControl) Resume(ctx context.Context, expected HaltState, audit []byte) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = lockApplication(ctx, tx, "consumer-control"); err != nil {
		return err
	}
	actual, err := readHalt(ctx, tx)
	if err != nil {
		return err
	}
	if actual != expected {
		return errHaltChanged
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO consumer_control(key,value) VALUES('last_resume',$1) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, string(audit)); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM consumer_control WHERE key='halt_reason'`); err != nil {
		return err
	}
	return tx.Commit()
}
func (a *app) controls() Controls {
	if a.control != nil {
		return a.control
	}
	return postgresControl{a.db}
}
