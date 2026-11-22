package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// A rotation whose response is lost must not turn the client's retry into a 401: the just-rotated
// token returns the same pair inside the grace window, concurrent duplicates agree, and the
// window closes on the next rotation or after it expires.
func TestRefreshGraceReturnsTheSamePairAndThenCloses(t *testing.T) {
	t.Setenv("AUTH_REFRESH_GRACE", "1s")
	store, err := openStore(testDSN(t), time.Minute, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer store.db.Close()
	ctx := context.Background()
	first, err := store.issue(ctx, identity{ID: "media-id", Secret: "s", Role: "media"})
	if err != nil {
		t.Fatal(err)
	}

	// Two requests present the same refresh token at once (a retry racing its original).
	pairs := make([]tokenPair, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for k := range pairs {
		wg.Add(1)
		go func() { defer wg.Done(); pairs[k], errs[k] = store.rotate(ctx, first.RefreshToken) }()
	}
	wg.Wait()
	for k, e := range errs {
		if e != nil {
			t.Fatalf("concurrent rotation %d failed: %v", k, e)
		}
	}
	if pairs[0].AccessToken != pairs[1].AccessToken || pairs[0].RefreshToken != pairs[1].RefreshToken {
		t.Fatal("concurrent rotations of one token returned different pairs")
	}
	rotated := pairs[0]
	// The response was lost; the client retries with the token it still holds.
	again, err := store.rotate(ctx, first.RefreshToken)
	if err != nil || again.AccessToken != rotated.AccessToken || again.RefreshToken != rotated.RefreshToken {
		t.Fatalf("retry within the grace window did not return the same pair: %v", err)
	}
	if p, err := store.verify(ctx, again.AccessToken); err != nil || p.ClientID != "media-id" {
		t.Fatalf("pair returned in the grace window is not usable: %v", err)
	}

	// The next rotation closes the window for the older token.
	next, err := store.rotate(ctx, rotated.RefreshToken)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.rotate(ctx, first.RefreshToken); !errors.Is(err, errInvalidToken) {
		t.Fatalf("a token two rotations old was accepted: %v", err)
	}
	// And the window expires.
	time.Sleep(2100 * time.Millisecond)
	if _, err = store.rotate(ctx, rotated.RefreshToken); !errors.Is(err, errInvalidToken) {
		t.Fatalf("the previous token was accepted after the grace window: %v", err)
	}
	if _, err = store.rotate(ctx, next.RefreshToken); err != nil {
		t.Fatalf("the current token stopped working: %v", err)
	}
}
