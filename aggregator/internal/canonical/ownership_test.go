package canonical

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

type leaseFixture struct{ calls int }

func (f *leaseFixture) Acquire(_ context.Context, r AcquireOwnership) (Lease, error) {
	f.calls++
	return Lease{Ownership: Ownership{Scope: r.Scope, Epoch: 1, Holder: r.Holder}, ExpiresAt: time.Date(2026, 11, 13, 0, 0, 15, 0, time.UTC), RenewAfterMS: 5000}, nil
}
func (f *leaseFixture) Renew(_ context.Context, o Ownership) (Lease, error) {
	f.calls++
	return Lease{}, Fail(403, "stale_owner", "expired")
}
func (f *leaseFixture) Release(_ context.Context, o Ownership) error { f.calls++; return nil }

func TestOwnershipHTTPBoundary(t *testing.T) {
	gin.SetMode(gin.TestMode)
	f := &leaseFixture{}
	r := gin.New()
	HTTP{Token: "private-service-token", Ownership: f}.Register(r)
	for _, tc := range []struct {
		name, action, token, body string
		status                    int
		called                    bool
	}{
		{"missing credentials", "acquire", "", `{}`, 401, false},
		{"wrong credentials", "acquire", "audit-token", `{}`, 403, false},
		{"duplicate key", "acquire", "private-service-token", `{"scope":"a","scope":"b","holder":"one"}`, 400, false},
		{"caller clock", "acquire", "private-service-token", `{"scope":"a","holder":"one","expires_at":"2099-01-01T00:00:00Z"}`, 400, false},
		{"caller ttl", "acquire", "private-service-token", `{"scope":"a","holder":"one","ttl":999}`, 400, false},
		{"trailing JSON", "acquire", "private-service-token", `{} {}`, 400, false},
		{"oversized", "acquire", "private-service-token", `{"holder":"` + strings.Repeat("a", 9000) + `"}`, 413, false},
		{"acquire", "acquire", "private-service-token", `{"scope":"canonical:BMKG","holder":"one"}`, 200, true},
		{"renew stale", "renew", "private-service-token", `{"scope":"canonical:BMKG","epoch":1,"holder":"one"}`, 403, true},
		{"release", "release", "private-service-token", `{"scope":"canonical:BMKG","epoch":1,"holder":"one"}`, 200, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := f.calls
			req := httptest.NewRequest("POST", "/internal/ownership/"+tc.action, strings.NewReader(tc.body))
			if tc.token != "" {
				req.Header.Set("Authorization", "Bearer "+tc.token)
			}
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			require.Equal(t, tc.status, w.Code, w.Body.String())
			require.Equal(t, tc.called, f.calls > before)
		})
	}
	r = gin.New()
	HTTP{Token: "private-service-token"}.Register(r)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("POST", "/internal/ownership/acquire", strings.NewReader(`{}`)))
	require.Equal(t, 404, w.Code, "ownership must require explicit wiring")
}

func TestOwnershipCommitError(t *testing.T) {
	var p *Error
	require.ErrorAs(t, CommitError(&pgconn.PgError{Code: StaleOwnerSQLState}), &p)
	require.Equal(t, "stale_owner", p.Code)
	require.Equal(t, 403, p.Status)
	require.ErrorAs(t, CommitError(errors.New("lost commit acknowledgement")), &p)
	require.Equal(t, 503, p.Status)
}
