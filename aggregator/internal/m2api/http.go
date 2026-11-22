// Package m2api exposes bounded internal operations over the canonical owner.
package m2api

import (
	"aggregator/internal/platform"
	"aggregator/internal/store"
	"context"
	"crypto/subtle"
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"net/http"
	"strconv"
	"time"
)

type Config struct{ AuditToken, CanonicalToken, AnalyticsToken, AdminToken, NodeA, NodeB, IngestionURL, IngestionToken string }

func auth(token string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if token == "" || subtle.ConstantTimeCompare([]byte(c.GetHeader("Authorization")), []byte("Bearer "+token)) != 1 {
			c.AbortWithStatusJSON(401, gin.H{"error": platform.ProblemStatus(401, "invalid service token")})
			return
		}
		c.Next()
	}
}
func problem(c *gin.Context, status int, msg string) {
	c.JSON(status, gin.H{"error": platform.ProblemStatus(status, msg)})
}
func parameters(c *gin.Context, allowed ...string) bool {
	set := map[string]bool{}
	for _, k := range allowed {
		set[k] = true
	}
	for k, v := range c.Request.URL.Query() {
		if !set[k] || len(v) != 1 {
			problem(c, 400, "unknown or repeated parameter")
			return false
		}
	}
	return true
}
func source(c *gin.Context) (string, bool) {
	s := c.Query("source")
	if s != "" && s != "BMKG" && s != "PVMBG" && s != "SATELIT" && s != "SENSOR_PANTAI" {
		problem(c, 400, "invalid source")
		return "", false
	}
	return s, true
}
func Register(r *gin.Engine, s *store.Store, cfg Config) {
	registerAdmin(r, s, cfg)
	registerCatalog(r, s, cfg)
	audit := r.Group("/internal/audit", auth(cfg.AuditToken))
	audit.GET("/events", func(c *gin.Context) {
		if !parameters(c, "source", "since", "limit", "cursor") {
			return
		}
		src, ok := source(c)
		if !ok {
			return
		}
		q := store.AuditQuery{Source: src, Limit: 100, Cursor: c.Query("cursor")}
		var e error
		if v := c.Query("since"); v != "" {
			q.Since, e = time.Parse(time.RFC3339Nano, v)
			if e != nil {
				problem(c, 400, "invalid since")
				return
			}
		}
		if v := c.Query("limit"); v != "" {
			q.Limit, e = strconv.Atoi(v)
			if e != nil || q.Limit < 1 || q.Limit > 1000 {
				problem(c, 400, "limit must be 1..1000")
				return
			}
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()
		page, e := s.AuditEvents(ctx, q)
		if errors.Is(e, store.ErrCursor) {
			problem(c, 400, "invalid cursor")
			return
		}
		if e != nil {
			problem(c, 503, "canonical data temporarily unavailable")
			return
		}
		c.JSON(200, page)
	})
	audit.GET("/checksum", func(c *gin.Context) {
		if !parameters(c, "source") {
			return
		}
		src, ok := source(c)
		if !ok {
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 4*time.Second)
		defer cancel()
		result, e := s.AuditChecksum(ctx, src)
		if e != nil {
			problem(c, 503, "checksum unavailable")
			return
		}
		c.JSON(200, result)
	})
	audit.GET("/location", func(c *gin.Context) {
		if !parameters(c, "hazard_id") {
			return
		}
		id := c.Query("hazard_id")
		if _, e := uuid.Parse(id); e != nil {
			problem(c, 400, "invalid hazard_id")
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()
		result, e := s.AuditLocation(ctx, id, cfg.NodeA, cfg.NodeB)
		if errors.Is(e, store.ErrNotFound) {
			problem(c, 404, "hazard not found")
			return
		}
		if e != nil {
			c.JSON(200, gin.H{"hazard_id": id, "locations": []any{}, "partial": true, "unavailable_units": []string{"canonical-visibility"}})
			return
		}
		c.JSON(200, result)
	})
	audit.GET("/unprocessed", func(c *gin.Context) {
		if !parameters(c, "source", "limit", "cursor") {
			return
		}
		src, ok := source(c)
		if !ok {
			return
		}
		limit := 100
		var e error
		if v := c.Query("limit"); v != "" {
			limit, e = strconv.Atoi(v)
			if e != nil || limit < 1 || limit > 1000 {
				problem(c, 400, "limit must be 1..1000")
				return
			}
		}
		page, e := unprocessed(c.Request.Context(), s, cfg, src, limit, c.Query("cursor"))
		if errors.Is(e, store.ErrCursor) {
			problem(c, 400, "invalid cursor")
			return
		}
		if e != nil {
			problem(c, 503, "unprocessed ledger unavailable")
			return
		}
		c.JSON(200, page)
	})
	r.POST("/internal/canonical/batches", auth(cfg.CanonicalToken), func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4<<20)
		var b store.CanonicalBatch
		if c.ShouldBindJSON(&b) != nil {
			problem(c, 400, "invalid canonical batch")
			return
		}
		e := s.ApplyCanonical(c.Request.Context(), b, c.GetString("correlation_id"))
		if errors.Is(e, store.ErrOrder) {
			problem(c, 409, e.Error())
			return
		}
		if e != nil {
			problem(c, 503, "canonical commit unavailable; retry the same batch")
			return
		}
		c.JSON(200, gin.H{"operation_id": b.ID, "position": b.Position})
	})
	// Several original operations of one namespace in predecessor order, applied in one owner
	// transaction. Same per-operation identity, digest and idempotency rules as /batches.
	r.POST("/internal/canonical/sequences", auth(cfg.CanonicalToken), func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 32<<20)
		var body struct {
			Batches []store.CanonicalBatch `json:"batches"`
		}
		if c.ShouldBindJSON(&body) != nil || len(body.Batches) == 0 || len(body.Batches) > store.MaxCanonicalSequence {
			problem(c, 400, "invalid canonical sequence")
			return
		}
		e := s.ApplyCanonicalSequence(c.Request.Context(), body.Batches, c.GetString("correlation_id"))
		if errors.Is(e, store.ErrOrder) {
			problem(c, 409, e.Error())
			return
		}
		if e != nil {
			problem(c, 503, "canonical commit unavailable; retry the same sequence")
			return
		}
		last := body.Batches[len(body.Batches)-1]
		c.JSON(200, gin.H{"applied": len(body.Batches), "position": last.Position})
	})
	r.GET("/internal/analytics/summary", auth(cfg.AnalyticsToken), func(c *gin.Context) {
		if !parameters(c, "from", "to", "interval", "source") {
			return
		}
		src, ok := source(c)
		if !ok {
			return
		}
		from, e := time.Parse(time.RFC3339Nano, c.Query("from"))
		to, e2 := time.Parse(time.RFC3339Nano, c.Query("to"))
		interval := c.DefaultQuery("interval", "day")
		if e != nil || e2 != nil || !to.After(from) || to.Sub(from) > 366*24*time.Hour || (interval != "hour" && interval != "day") || (interval == "hour" && to.Sub(from) > 31*24*time.Hour) {
			problem(c, 400, "valid range required (daily <=366 days; hourly <=31 days)")
			return
		}
		result, e := s.Analytics(c.Request.Context(), from, to, interval, src)
		if e != nil {
			problem(c, 503, "analytics unavailable")
			return
		}
		c.JSON(200, result)
	})
}
