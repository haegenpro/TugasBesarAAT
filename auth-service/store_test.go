package main

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestSessionSurvivesRestartAndRotationRevokesOldToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth.db")
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
	if _, err := store.rotate(ctx, first.RefreshToken); !errors.Is(err, errInvalidToken) {
		t.Fatalf("old refresh token accepted: %v", err)
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

func TestConcurrentRefreshOnlyOneSucceeds(t *testing.T) {
	store, err := openStore(filepath.Join(t.TempDir(), "auth.db"), time.Minute, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer store.db.Close()
	first, err := store.issue(context.Background(), identity{ID: "field-id", Role: "field"})
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := store.rotate(context.Background(), first.RefreshToken)
			results <- err
		}()
	}
	wait.Wait()
	close(results)
	success, rejected := 0, 0
	for err := range results {
		switch {
		case err == nil:
			success++
		case errors.Is(err, errInvalidToken):
			rejected++
		default:
			t.Fatalf("unexpected refresh error: %v", err)
		}
	}
	if success != 1 || rejected != 1 {
		t.Fatalf("success=%d rejected=%d", success, rejected)
	}
}

func TestAccessTokenExpiresNaturally(t *testing.T) {
	store, err := openStore(filepath.Join(t.TempDir(), "auth.db"), time.Second, time.Hour)
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

func TestRecoveryScopeOnlyForInternalIdentity(t *testing.T) {
	store, err := openStore(filepath.Join(t.TempDir(), "auth.db"), time.Minute, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer store.db.Close()
	for _, test := range []struct{ role, scope string }{
		{"media", "hazards:summary"}, {"field", "hazards:raw"}, {"internal", "hazards:raw consumer:resume"},
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
