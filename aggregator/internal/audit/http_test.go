package audit

import (
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"aggregator/internal/pagecursor"
	"context"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeRepository struct {
	query Query
	calls int
	block bool
}

func (f *fakeRepository) AuditEvents(ctx context.Context, q Query) (Page, error) {
	f.calls++
	f.query = q
	if f.block {
		<-ctx.Done()
		return Page{}, canonical.Unavailable()
	}
	return Page{Items: []domain.HazardEvent{}, Unavailable: []string{}}, nil
}
func (f *fakeRepository) AuditChecksum(context.Context, string) (Checksum, error) {
	f.calls++
	return Checksum{}, nil
}
func (f *fakeRepository) AuditLocation(context.Context, string) (Locations, error) {
	f.calls++
	return Locations{}, nil
}
func TestHTTPAuthValidationAndPrecision(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	repo := &fakeRepository{}
	h := HTTP{Repository: repo, Token: "audit-secret", Signer: pagecursor.Signer{Key: []byte(strings.Repeat("s", 32))}}
	r := gin.New()
	h.Register(r)
	cases := []struct {
		path, token string
		status      int
	}{{"/internal/audit/events", "", 401}, {"/internal/audit/events", "wrong", 401}, {"/internal/audit/events?source=UNKNOWN", "audit-secret", 400}, {"/internal/audit/events?source=", "audit-secret", 400}, {"/internal/audit/events?limit=0", "audit-secret", 400}, {"/internal/audit/events?limit=1001", "audit-secret", 400}, {"/internal/audit/events?limit=1&limit=2", "audit-secret", 400}, {"/internal/audit/events?since=not-time", "audit-secret", 400}, {"/internal/audit/events?cursor=e30", "audit-secret", 400}, {"/internal/audit/checksum?since=x", "audit-secret", 400}, {"/internal/audit/location?hazard_id=missing", "audit-secret", 400}, {"/internal/audit/events?source=SATELIT&since=2026-11-13T00:00:00.123456Z&limit=1", "audit-secret", 200}}
	for _, test := range cases {
		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", test.path, nil)
		if test.token != "" {
			req.Header.Set("Authorization", "Bearer "+test.token)
		}
		r.ServeHTTP(w, req)
		require.Equal(t, test.status, w.Code, test.path)
		if w.Code != 200 {
			var b map[string]any
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &b))
			require.Contains(t, b, "error")
		}
	}
	require.Equal(t, 1, repo.calls)
	require.Equal(t, 123456000, repo.query.Since.Nanosecond())
	require.Equal(t, 1, repo.query.Limit)
}
func TestAuditDeadline(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	HTTP{Repository: &fakeRepository{block: true}, Token: "audit", Signer: pagecursor.Signer{Key: []byte(strings.Repeat("s", 32))}}.Register(r)
	req := httptest.NewRequest("GET", "/internal/audit/events", nil)
	req.Header.Set("Authorization", "Bearer audit")
	w := httptest.NewRecorder()
	started := time.Now()
	r.ServeHTTP(w, req)
	require.Equal(t, 503, w.Code)
	require.Less(t, time.Since(started), 2250*time.Millisecond)
}
