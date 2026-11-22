package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// Rotation revokes the old access token at once. The old refresh token is honoured only inside the
// refresh grace window, and then only by returning the same pair (no second session); after the
// window it is rejected.
func TestSessionSurvivesRestartAndRotationRevokesOldTokenAfterGrace(t *testing.T) {
	t.Setenv("AUTH_REFRESH_GRACE", "1s")
	path := testDSN(t)
	store, err := openStore(path, time.Minute, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first, err := store.issue(ctx, identity{ID: "field-id", Role: "field"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = openStore(path, time.Minute, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer store.db.Close()
	principal, err := store.verify(ctx, first.AccessToken)
	if err != nil || principal.Role != "field" {
		t.Fatalf("session not restored: %+v, %v", principal, err)
	}
	second, err := store.rotate(ctx, first.RefreshToken)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.verify(ctx, first.AccessToken); !errors.Is(err, errInvalidToken) {
		t.Fatalf("old access token accepted: %v", err)
	}
	retried, err := store.rotate(ctx, first.RefreshToken)
	if err != nil || retried.AccessToken != second.AccessToken || retried.RefreshToken != second.RefreshToken {
		t.Fatalf("old refresh token inside the grace window must return the same pair: %v", err)
	}
	time.Sleep(2100 * time.Millisecond)
	if _, err := store.rotate(ctx, first.RefreshToken); !errors.Is(err, errInvalidToken) {
		t.Fatalf("old refresh token accepted after the grace window: %v", err)
	}
	if _, err := store.verify(ctx, second.AccessToken); err != nil {
		t.Fatal(err)
	}
	if err := store.logout(ctx, second.RefreshToken); err != nil {
		t.Fatal(err)
	}
	if _, err := store.verify(ctx, second.AccessToken); !errors.Is(err, errInvalidToken) {
		t.Fatalf("logged-out access token accepted: %v", err)
	}
}

// Two requests presenting the same refresh token at once produce one rotation: both receive the
// identical pair, and that pair is the session's current one.
func TestConcurrentRefreshProducesOneRotation(t *testing.T) {
	store, err := openStore(testDSN(t), time.Minute, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer store.db.Close()
	first, err := store.issue(context.Background(), identity{ID: "field-id", Role: "field"})
	if err != nil {
		t.Fatal(err)
	}
	pairs := make(chan tokenPair, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			pair, err := store.rotate(context.Background(), first.RefreshToken)
			if err != nil {
				t.Errorf("concurrent refresh failed: %v", err)
			}
			pairs <- pair
		}()
	}
	wait.Wait()
	close(pairs)
	distinct := map[string]bool{}
	var pair tokenPair
	for p := range pairs {
		distinct[p.RefreshToken] = true
		pair = p
	}
	if len(distinct) != 1 {
		t.Fatalf("concurrent refreshes produced %d different pairs", len(distinct))
	}
	if _, err := store.rotate(context.Background(), pair.RefreshToken); err != nil {
		t.Fatalf("the shared pair is not the session's current one: %v", err)
	}
}

func TestAccessTokenExpiresNaturally(t *testing.T) {
	store, err := openStore(testDSN(t), time.Second, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer store.db.Close()
	pair, err := store.issue(context.Background(), identity{ID: "field-id", Role: "field"})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	if _, err := store.verify(context.Background(), pair.AccessToken); !errors.Is(err, errInvalidToken) {
		t.Fatalf("expired access token accepted: %v", err)
	}
	if _, err := store.rotate(context.Background(), pair.RefreshToken); err != nil {
		t.Fatalf("refresh after expiry failed: %v", err)
	}
}

func TestDownstreamScopesRemainReadOnly(t *testing.T) {
	store, err := openStore(testDSN(t), time.Minute, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer store.db.Close()
	for _, test := range []struct{ role, scope string }{
		{"media", "hazards:summary"}, {"field", "hazards:raw"}, {"internal", "hazards:raw"},
	} {
		pair, err := store.issue(context.Background(), identity{ID: test.role + "-id", Role: test.role})
		if err != nil {
			t.Fatal(err)
		}
		p, err := store.verify(context.Background(), pair.AccessToken)
		if err != nil || pair.Scope != test.scope || p.Scope != test.scope {
			t.Fatalf("role=%s scopes=%q/%q error=%v", test.role, pair.Scope, p.Scope, err)
		}
		rotated, err := store.rotate(context.Background(), pair.RefreshToken)
		if err != nil || rotated.Scope != test.scope {
			t.Fatalf("rotation changed scope: %q %v", rotated.Scope, err)
		}
	}
}
