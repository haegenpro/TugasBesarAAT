package main

import (
	"authservice/internal/platform"
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgconn"
)

// storageBusy reports errors that mean the auth database is overloaded right now (too many
// connections, a statement or lock timeout, a deadline, a serialization conflict) rather than
// broken. Retrying shortly is the right response to these.
func storageBusy(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		switch pg.Code {
		case "53300", "53400", "57014", "55P03", "40001", "40P01":
			return true
		}
	}
	return false
}

// storageContext bounds one request's storage work. database/sql waits for a free pool connection
// until its context ends; without a deadline of its own, a saturated pool held requests until the
// caller gave up, and the cancellation then looked like a broken store (503). With the deadline
// the wait ends inside Auth as DeadlineExceeded, which is answered as a controlled 429, and a
// refresh rotation that misses it is rolled back instead of committing after its caller left.
func (a *authServer) storageContext(c *gin.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(c.Request.Context(), platform.Duration("AUTH_STORAGE_TIMEOUT", 1500*time.Millisecond))
}

// storageFailure answers a storage error: a controlled 429 with Retry-After when the database is
// merely saturated, so callers back off instead of counting a server failure; 503 otherwise.
func (a *authServer) storageFailure(c *gin.Context, storage context.Context, err error) {
	if storageBusy(err) || (errors.Is(storage.Err(), context.DeadlineExceeded) && c.Request.Context().Err() == nil) {
		platform.Add("auth_storage_busy_rejected_total", 1)
		c.Header("Retry-After", "1")
		c.JSON(http.StatusTooManyRequests, gin.H{"error": platform.ProblemStatus(http.StatusTooManyRequests, "auth storage busy")})
		return
	}
	platform.Add("auth_storage_unavailable_total", 1)
	a.log.Warn("auth storage unavailable", "service", "auth-service", "error", err.Error())
	c.JSON(http.StatusServiceUnavailable, gin.H{"error": platform.ProblemStatus(http.StatusServiceUnavailable, "auth storage unavailable")})
}
