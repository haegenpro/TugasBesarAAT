package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestReadyDraining(t *testing.T) {
	deps := &Dependencies{}
	deps.SetStoreReady(true)
	a := &API{Deps: deps}
	router := gin.New()
	router.GET("/ready", a.Ready)
	router.GET("/health", Health)
	request := func(path string) *httptest.ResponseRecorder {
		reply := httptest.NewRecorder()
		router.ServeHTTP(reply, httptest.NewRequest(http.MethodGet, path, nil))
		return reply
	}
	require.Equal(t, 200, request("/ready").Code, "a standby can serve reads without source ownership")
	deps.SetDraining(true)
	require.Equal(t, 503, request("/ready").Code)
	require.Equal(t, 200, request("/health").Code, "dependency/drain state must not restart all replicas")
}
