package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestRefreshAcrossIndependentInstances(t *testing.T) {
	dsn := testDSN(t)
	a, err := openStore(dsn, time.Minute, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer a.db.Close()
	b, err := openStore(dsn, time.Minute, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer b.db.Close()
	pair, err := a.issue(context.Background(), identity{ID: "field", Role: "field"})
	if err != nil {
		t.Fatal(err)
	}
	stores := []*sessionStore{a, b}
	var wg sync.WaitGroup
	results := make(chan error, 16)
	for n := range 16 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_, e := stores[n%2].rotate(context.Background(), pair.RefreshToken)
			results <- e
		}(n)
	}
	wg.Wait()
	close(results)
	success := 0
	for e := range results {
		if e == nil {
			success++
		} else if !errors.Is(e, errInvalidToken) {
			t.Fatal(e)
		}
	}
	if success != 1 {
		t.Fatalf("refresh succeeded %d times", success)
	}
}
