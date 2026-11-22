package ingest

import (
	"aggregator/internal/platform"
	"context"
	"crypto/subtle"
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"io"
	"mime"
	"net/http"
	"strconv"
	"time"
)

func serviceAuth(token string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if token == "" || subtle.ConstantTimeCompare([]byte(c.GetHeader("Authorization")), []byte("Bearer "+token)) != 1 {
			c.AbortWithStatusJSON(401, gin.H{"error": platform.ProblemStatus(401, "invalid service credential")})
			return
		}
		c.Next()
	}
}
func (s *Store) Handler(token, sloToken string) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	s.registerOps(r, token)
	r.GET("/health", func(c *gin.Context) { c.JSON(200, gin.H{"status": "ok"}) })
	r.GET("/ready", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), time.Second)
		defer cancel()
		if s.Pool.Ping(ctx) != nil {
			c.JSON(503, gin.H{"status": "unavailable"})
			return
		}
		c.JSON(200, gin.H{"status": "ready"})
	})
	reports := r.Group("/internal/field-reports", serviceAuth(token))
	reports.POST("", func(c *gin.Context) {
		key, submitter := c.GetHeader("Idempotency-Key"), c.GetHeader("X-Submitter-ID")
		if !ValidKey(key) || submitter == "" {
			c.JSON(400, gin.H{"error": platform.ProblemStatus(400, "valid Idempotency-Key and submitter required")})
			return
		}
		media, _, e := mime.ParseMediaType(c.GetHeader("Content-Type"))
		if e != nil || media != "application/json" {
			c.JSON(400, gin.H{"error": platform.ProblemStatus(400, "application/json required")})
			return
		}
		body, e := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, (1<<20)-(16<<10)))
		if e != nil {
			c.JSON(413, gin.H{"error": platform.ProblemStatus(413, "report too large")})
			return
		}
		if e = ValidateReport(body); e != nil {
			c.JSON(400, gin.H{"error": platform.ProblemStatus(400, e.Error())})
			return
		}
		// Acceptance has its own deadline inside the caller's budget. When storage is too slow to
		// accept in time, the answer is a controlled 429 instead of a request held until the client
		// gives up. Retrying the same key is safe even if the commit did complete: the ledger
		// returns the existing receipt.
		accept, cancel := context.WithTimeout(c.Request.Context(), platform.Milliseconds("FIELD_ACCEPT_TIMEOUT_MS", 2000))
		defer cancel()
		result, e := s.AcceptReport(accept, submitter, key, body, c.GetHeader("X-Correlation-ID"))
		if errors.Is(e, ErrConflict) {
			c.JSON(409, gin.H{"error": platform.ProblemStatus(409, e.Error())})
			return
		}
		if e != nil {
			c.Header("Retry-After", "1")
			if accept.Err() != nil && c.Request.Context().Err() == nil {
				platform.Add("field_accept_busy_total", 1)
				c.JSON(429, gin.H{"error": platform.ProblemStatus(429, "acceptance busy; retry the same key")})
				return
			}
			c.JSON(503, gin.H{"error": platform.ProblemStatus(503, "acceptance unavailable; retry the same key")})
			return
		}
		c.JSON(202, result)
	})
	reports.GET("/:receipt_id", func(c *gin.Context) {
		id := c.Param("receipt_id")
		if _, e := uuid.Parse(id); e != nil {
			c.JSON(400, gin.H{"error": platform.ProblemStatus(400, "invalid receipt")})
			return
		}
		result, e := s.Report(c.Request.Context(), id, c.GetHeader("X-Submitter-ID"), c.GetHeader("X-Submitter-Role"))
		if errors.Is(e, pgx.ErrNoRows) {
			c.JSON(404, gin.H{"error": platform.ProblemStatus(404, "receipt not found")})
			return
		}
		if e != nil {
			c.JSON(503, gin.H{"error": platform.ProblemStatus(503, "receipt unavailable")})
			return
		}
		c.JSON(200, result)
	})
	r.GET("/internal/coastal/warnings/expected", serviceAuth(sloToken), s.expected)
	r.GET("/internal/ingestion/health", serviceAuth(token), func(c *gin.Context) {
		rows, e := s.Pool.Query(c.Request.Context(), `SELECT endpoint,state,last_success,last_attempt,last_new,latest_observed,source_generated FROM source_health ORDER BY endpoint`)
		if e != nil {
			c.Status(503)
			return
		}
		defer rows.Close()
		items := []map[string]any{}
		for rows.Next() {
			var endpoint, state string
			var success, attempt, fresh, observed, generated *time.Time
			if rows.Scan(&endpoint, &state, &success, &attempt, &fresh, &observed, &generated) != nil {
				c.Status(503)
				return
			}
			items = append(items, map[string]any{"endpoint": endpoint, "state": state, "last_success": success, "last_attempt": attempt, "last_new": fresh, "latest_observed": observed, "source_generated": generated})
		}
		if rows.Err() != nil {
			c.Status(503)
			return
		}
		c.JSON(200, gin.H{"items": items})
	})
	r.GET("/internal/ingestion/progress", serviceAuth(token), func(c *gin.Context) {
		rows, e := s.Pool.Query(c.Request.Context(), `SELECT source,namespace,received,prepared,applied,virtual_now,watermark,singletons,excluded,late_mutations,(SELECT count(*) FROM raw_inputs r WHERE r.source=p.source AND r.namespace=p.namespace AND r.state<>'applied' AND r.position<=p.applied) FROM source_progress p ORDER BY source,namespace`)
		if e != nil {
			c.Status(503)
			return
		}
		defer rows.Close()
		items := []map[string]any{}
		for rows.Next() {
			var source, ns string
			var received, prepared, applied int64
			var singletons, excluded, late, replay int64
			var virtual, watermark *time.Time
			if rows.Scan(&source, &ns, &received, &prepared, &applied, &virtual, &watermark, &singletons, &excluded, &late, &replay) != nil {
				c.Status(503)
				return
			}
			items = append(items, map[string]any{"source": source, "namespace": ns, "received": received, "prepared": prepared, "applied": applied, "virtual_now": virtual, "watermark": watermark, "singletons": singletons, "excluded": excluded, "late_mutations": late, "historical_pending": replay})
		}
		if rows.Err() != nil {
			c.Status(503)
			return
		}
		c.JSON(200, gin.H{"items": items})
	})
	return r
}
func (s *Store) expected(c *gin.Context) {
	since, e := time.Parse(time.RFC3339Nano, c.Query("since"))
	if e != nil {
		c.JSON(400, gin.H{"error": platform.ProblemStatus(400, "since required")})
		return
	}
	after := int64(0)
	through := int64(0)
	asOf := time.Now().UTC()
	if c.Query("after") != "" {
		after, e = strconv.ParseInt(c.Query("after"), 10, 64)
		if e != nil || after < 0 {
			c.Status(400)
			return
		}
		through, e = strconv.ParseInt(c.Query("through"), 10, 64)
		if e != nil || through < after {
			c.Status(400)
			return
		}
		asOf, e = time.Parse(time.RFC3339Nano, c.Query("as_of"))
		if e != nil {
			c.Status(400)
			return
		}
	} else {
		if s.Pool.QueryRow(c.Request.Context(), `SELECT coalesce(max(sequence),0),clock_timestamp() FROM warning_expectations`).Scan(&through, &asOf) != nil {
			c.Status(503)
			return
		}
	}
	rows, e := s.Pool.Query(c.Request.Context(), `SELECT sequence,notification_id,first_received_at,rejected FROM warning_expectations WHERE sequence>$1 AND sequence<=$2 AND first_received_at>=$3 AND first_received_at<=$4 ORDER BY sequence LIMIT 500`, after, through, since, asOf)
	if e != nil {
		c.Status(503)
		return
	}
	defer rows.Close()
	items := []map[string]any{}
	next := after
	for rows.Next() {
		var seq int64
		var id string
		var at time.Time
		var rejected bool
		if rows.Scan(&seq, &id, &at, &rejected) != nil {
			c.Status(503)
			return
		}
		items = append(items, map[string]any{"sequence": seq, "notification_id": id, "first_received_at": at, "rejected": rejected})
		next = seq
	}
	if rows.Err() != nil {
		c.Status(503)
		return
	}
	var coverage *time.Time
	_ = s.Pool.QueryRow(c.Request.Context(), `SELECT min(received_at) FROM raw_inputs WHERE source='SENSOR_PANTAI'`).Scan(&coverage)
	c.JSON(200, gin.H{"items": items, "through": through, "as_of": asOf, "next": next, "coverage_since": coverage})
}
