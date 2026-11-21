package m2api

import (
	"aggregator/internal/clients"
	"aggregator/internal/store"
	"context"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"io"
	"net/http"
	"strconv"
	"time"
)

func strictBody(c *gin.Context, limit int64, out any) bool {
	d := json.NewDecoder(http.MaxBytesReader(c.Writer, c.Request.Body, limit))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil || d.Decode(new(any)) != io.EOF {
		problem(c, 400, "invalid request fields")
		return false
	}
	return true
}
func registerAdmin(r *gin.Engine, s *store.Store, cfg Config) {
	admin := r.Group("/internal/ops", auth(cfg.AdminToken))
	admin.GET("/schema-policy", func(c *gin.Context) {
		version, e := s.Policy(c.Request.Context())
		if e != nil {
			problem(c, 503, "reader policy unavailable")
			return
		}
		c.JSON(200, gin.H{"desired_version": version, "local_version": clients.VolcanicAdapter()})
	})
	admin.PUT("/schema-policy", func(c *gin.Context) {
		var req struct {
			Version int `json:"version"`
		}
		if !strictBody(c, 4096, &req) {
			return
		}
		if req.Version != 1 && req.Version != 2 {
			problem(c, 400, "version must be 1 or 2")
			return
		}
		if e := s.SetPolicy(c.Request.Context(), req.Version); e != nil {
			problem(c, 503, "policy update unavailable; retry the same version")
			return
		}
		c.JSON(202, gin.H{"desired_version": req.Version, "status": "adopting"})
	})
	admin.GET("/failures", func(c *gin.Context) {
		limit := 100
		if v := c.Query("limit"); v != "" {
			var e error
			limit, e = strconv.Atoi(v)
			if e != nil || limit < 1 || limit > 1000 {
				problem(c, 400, "limit must be 1..1000")
				return
			}
		}
		page, e := s.Unprocessed(c.Request.Context(), c.Query("source"), limit, c.Query("cursor"))
		if e != nil {
			problem(c, 503, "quarantine metadata unavailable")
			return
		}
		c.JSON(200, page)
	})
	admin.GET("/failures/:id/payload", func(c *gin.Context) {
		id := c.Param("id")
		if _, e := uuid.Parse(id); e != nil {
			problem(c, 400, "invalid message ID")
			return
		}
		size, digest, e := s.FailurePayloadMetadata(c.Request.Context(), id)
		if e != nil {
			problem(c, 404, "retained message not found")
			return
		}
		c.Header("Content-Type", "application/octet-stream")
		c.Header("Content-Disposition", "attachment; filename=quarantine-"+id+".bin")
		c.Header("Content-Length", strconv.FormatInt(size, 10))
		c.Header("X-Content-SHA256", digest)
		c.Header("X-Content-Type-Options", "nosniff")
		_, _, _, _ = s.WriteFailurePayload(c.Request.Context(), id, c.Writer)
	})
	admin.POST("/replays", func(c *gin.Context) {
		var req struct {
			IDs []string `json:"message_ids"`
		}
		if !strictBody(c, 1<<20, &req) {
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
		defer cancel()
		selected, e := s.RequestReplay(ctx, req.IDs)
		if e != nil {
			problem(c, 409, "select known PVMBG message IDs once each")
			return
		}
		c.JSON(202, gin.H{"selected": selected, "status": "pending"})
	})
}
