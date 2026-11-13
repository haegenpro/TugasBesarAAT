package main

import (
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// Deferred: exercise shared route budgets, reservation, spoof resistance and
// release. Full Auth/upstream starvation and game-day proofs remain separate.
func TestFieldReservationSurvivesGeneralSaturation(t *testing.T) {
	r := gin.New()
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	r.Use(func(c *gin.Context) {
		c.Set("principal", principal{ClientID: "verified-client", Role: c.Param("role")})
		c.Next()
	}, reserveFieldCapacity(3, 1, slog.New(slog.NewJSONHandler(io.Discard, nil))))
	r.GET("/:role/:action", func(c *gin.Context) {
		if c.Param("action") == "hold" {
			entered <- struct{}{}
			<-release
		}
		c.Status(200)
	})
	request := func(path string) int {
		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set("X-Role", "field")
		r.ServeHTTP(w, req)
		return w.Code
	}
	done := make(chan int, 2)
	defer close(release)
	for i := 0; i < 2; i++ {
		go func() { done <- request("/media/hold") }()
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("request did not enter")
		}
	}
	if status := request("/media/read"); status != 429 {
		t.Fatalf("spoofed general role admitted: %d", status)
	}
	if status := request("/internal/read"); status != 429 {
		t.Fatalf("internal used field reserve: %d", status)
	}
	if status := request("/field/read"); status != 200 {
		t.Fatalf("field reserve unavailable: %d", status)
	}
	if status := request("/unknown/read"); status != 403 {
		t.Fatalf("unknown role accepted: %d", status)
	}
}
