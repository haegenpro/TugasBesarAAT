package canonical

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgconn"
)

// StaleOwnerSQLState identifies the database's deferred ownership check.
const StaleOwnerSQLState = "P0F01"

type AcquireOwnership struct {
	Scope  string `json:"scope"`
	Holder string `json:"holder"`
}

type Lease struct {
	Ownership    Ownership `json:"ownership"`
	ExpiresAt    time.Time `json:"expires_at"`
	RenewAfterMS int64     `json:"renew_after_ms"`
}

type OwnershipRepository interface {
	Acquire(context.Context, AcquireOwnership) (Lease, error)
	Renew(context.Context, Ownership) (Lease, error)
	Release(context.Context, Ownership) error
}

// CommitError preserves an authoritative expiry rejection at COMMIT. Other
// errors include unknown outcomes and must remain retryable, not claim rollback.
func CommitError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == StaleOwnerSQLState {
		return Fail(403, "stale_owner", "ownership expired before commit")
	}
	return Unavailable()
}

func (h HTTP) ownership(c *gin.Context, action string) {
	if !h.authorize(c) {
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), Deadline)
	defer cancel()
	body := http.MaxBytesReader(c.Writer, c.Request.Body, 8<<10)
	defer body.Close()
	stop := context.AfterFunc(ctx, func() { _ = body.Close() })
	defer stop()
	decode := func(v any) bool {
		if err := Decode(body, v); err != nil {
			var over *http.MaxBytesError
			switch {
			case ctx.Err() != nil:
				writeError(c, Unavailable())
			case errors.As(err, &over):
				writeError(c, Fail(413, "payload_too_large", "ownership request exceeds 8 KiB"))
			default:
				writeError(c, Fail(400, "invalid_request", "invalid ownership request JSON"))
			}
			return false
		}
		return true
	}
	var lease Lease
	var err error
	if action == "acquire" {
		var request AcquireOwnership
		if !decode(&request) {
			return
		}
		lease, err = h.Ownership.Acquire(ctx, request)
	} else {
		var request Ownership
		if !decode(&request) {
			return
		}
		if action == "renew" {
			lease, err = h.Ownership.Renew(ctx, request)
		} else {
			err = h.Ownership.Release(ctx, request)
		}
	}
	if ctx.Err() != nil {
		err = Unavailable()
	}
	if err != nil {
		writeError(c, err)
		return
	}
	if action == "release" {
		c.JSON(200, gin.H{"outcome": "released"})
		return
	}
	c.JSON(200, lease)
}
