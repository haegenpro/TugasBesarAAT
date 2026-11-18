package middleware

import (
	"bytes"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAcquisitionRecoveryLogKeepsIdentityAndOmitsCredentials(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var logs bytes.Buffer
	router := gin.New()
	router.Use(CorrelationID(), JSONLogger(slog.New(slog.NewJSONHandler(&logs, nil)), "source"))
	router.GET("/source", func(c *gin.Context) { c.Status(200) })
	for _, attempt := range []struct{ epoch, recovery string }{{"1", "false"}, {"2", "true"}} {
		request := httptest.NewRequest(http.MethodGet, "/source", nil)
		request.Header.Set("X-Acquisition-Request-ID", strings.Repeat("a", 64))
		request.Header.Set("X-Acquisition-Epoch", attempt.epoch)
		request.Header.Set("X-Acquisition-Recovery", attempt.recovery)
		request.Header.Set("Authorization", "Bearer private-test-credential")
		request.Header.Set("X-BMKG-Key", "private-test-credential")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		require.Equal(t, 200, response.Code)
	}
	require.NotContains(t, logs.String(), "private-test-credential")
	decoder := json.NewDecoder(&logs)
	for _, recovery := range []bool{false, true} {
		var record struct {
			Acquisition struct {
				RequestID string `json:"request_id"`
				Epoch     int64  `json:"epoch"`
				Recovery  bool   `json:"recovery"`
			} `json:"acquisition"`
		}
		require.NoError(t, decoder.Decode(&record))
		require.Equal(t, strings.Repeat("a", 64), record.Acquisition.RequestID)
		require.Equal(t, recovery, record.Acquisition.Recovery)
		if recovery {
			require.EqualValues(t, 2, record.Acquisition.Epoch)
		} else {
			require.EqualValues(t, 1, record.Acquisition.Epoch)
		}
	}
	// Invalid metadata remains absent; credentials and unrelated headers are not
	// added to the log as a fallback when acquisition headers are malformed.
	logs.Reset()
	request := httptest.NewRequest(http.MethodGet, "/source", nil)
	request.Header.Set("X-Acquisition-Request-ID", "private-test-credential")
	router.ServeHTTP(httptest.NewRecorder(), request)
	require.NotContains(t, logs.String(), "private-test-credential")
	var record map[string]any
	require.NoError(t, json.Unmarshal(logs.Bytes(), &record))
	require.NotContains(t, record, "acquisition")
}
