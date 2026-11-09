package main

import (
	"context"
	"errors"
	"testing"
)

func TestResumeCannotEraseInterveningIdenticalHalt(t *testing.T) {
	dsn := testDSN(t)
	db, err := openDB(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	first, second := postgresControl{db}, postgresControl{db}
	ctx := context.Background()
	if err = first.Halt(ctx, "schema failure"); err != nil {
		t.Fatal(err)
	}
	expected, err := first.State(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = second.Halt(ctx, "schema failure"); err != nil {
		t.Fatal(err)
	}
	if err = first.Resume(ctx, expected, []byte(`{"reason":"stale resume"}`)); !errors.Is(err, errHaltChanged) {
		t.Fatalf("stale resume accepted: %v", err)
	}
	actual, err := second.State(ctx)
	if err != nil || !actual.Exists || actual.Version <= expected.Version {
		t.Fatalf("new halt lost: %+v %v", actual, err)
	}
	var count int
	if err = db.QueryRow(`SELECT count(*) FROM consumer_control WHERE key='last_resume'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed resume committed audit: %d %v", count, err)
	}
}
