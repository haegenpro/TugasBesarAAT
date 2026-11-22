package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// A starting instance is not ready while it warms up, and becomes ready once warm-up ends.
func TestReadyWaitsForWarmUp(t *testing.T) {
	gin.SetMode(gin.TestMode)
	deps := &Dependencies{}
	deps.SetStoreReady(true)
	deps.SetBrokerReady(true)
	deps.SetLifecycleReady(true)
	a := &API{Deps: deps}
	status := func() int {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		a.Ready(c)
		return w.Code
	}
	deps.SetWarming(true)
	if code := status(); code != http.StatusServiceUnavailable {
		t.Fatalf("warming instance reported %d, want 503", code)
	}
	deps.SetWarming(false)
	if code := status(); code != http.StatusOK {
		t.Fatalf("warmed instance reported %d, want 200", code)
	}
}
