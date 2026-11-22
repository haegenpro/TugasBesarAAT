package main

import (
	"context"
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
	// Sixteen concurrent refreshes with one token, spread over two instances, perform exactly one
	// rotation: every request receives the same pair, and that pair is the session's current one.
	stores := []*sessionStore{a, b}
	var wg sync.WaitGroup
	results := make(chan tokenPair, 16)
	for n := range 16 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			p, e := stores[n%2].rotate(context.Background(), pair.RefreshToken)
			if e != nil {
				t.Errorf("refresh %d failed: %v", n, e)
			}
			results <- p
		}(n)
	}
	wg.Wait()
	close(results)
	distinct := map[string]bool{}
	var current tokenPair
	for p := range results {
		distinct[p.RefreshToken] = true
		current = p
	}
	if len(distinct) != 1 {
		t.Fatalf("one token produced %d different rotations", len(distinct))
	}
	if _, err := a.rotate(context.Background(), current.RefreshToken); err != nil {
		t.Fatalf("the shared pair is not the session's current one: %v", err)
	}
}
