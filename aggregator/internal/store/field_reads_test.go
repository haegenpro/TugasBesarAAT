package store

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestPriorityFieldReadPoolIsReadOnlyAndIndependent(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_URL required")
	}
	t.Setenv("FIELD_READ_PRIORITY_ENABLED", "true")
	t.Setenv("FIELD_READ_DB_RESERVED", "1")
	t.Setenv("DB_MAX_OPEN_CONNS", "2")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	general, err := New(ctx, dsn, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer general.Close()
	field, closeField, err := general.NewFieldReadView(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeField()
	held, err := general.Pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	blocked, stop := context.WithTimeout(ctx, 30*time.Millisecond)
	defer stop()
	if _, err := general.Pool.Acquire(blocked); err == nil {
		t.Fatal("general pool saturation fixture failed")
	}
	var value int
	if err := field.Pool.QueryRow(ctx, "SELECT 42").Scan(&value); err != nil || value != 42 {
		t.Fatalf("reserved Field read blocked: %v", err)
	}
	if _, err := field.Pool.Exec(ctx, "CREATE TEMP TABLE forbidden_field_write(id int)"); err == nil {
		t.Fatal("Field repository accepted DDL")
	}
	if field.Pool.Config().MaxConns+general.Pool.Config().MaxConns != 2 {
		t.Fatal("Field split increased primary connection budget")
	}
	closeField()
	var alive int
	if err := held.QueryRow(ctx, "SELECT 1").Scan(&alive); err != nil {
		t.Fatalf("closing Field view affected ordinary pool: %v", err)
	}
}
