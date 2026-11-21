package ingest

import (
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"io"
	"net/http"
	"strconv"
	"time"
)

func (s *Store) registerOps(r *gin.Engine, token string) {
	ops := r.Group("/internal/ops", serviceAuth(token))
	ops.GET("/failures", func(c *gin.Context) {
		limit := 100
		var e error
		if value := c.Query("limit"); value != "" {
			limit, e = strconv.Atoi(value)
			if e != nil || limit < 1 || limit > 1000 {
				c.Status(400)
				return
			}
		}
		page, e := s.Failures(c.Request.Context(), c.Query("source"), limit, c.Query("cursor"))
		if errors.Is(e, ErrConflict) {
			c.Status(400)
			return
		}
		if e != nil {
			c.JSON(503, gin.H{"error": gin.H{"code": "temporarily_unavailable", "message": "retained input metadata unavailable"}})
			return
		}
		c.JSON(200, page)
	})
	ops.GET("/failures/:id/payload", func(c *gin.Context) {
		_ = http.NewResponseController(c.Writer).SetWriteDeadline(time.Now().Add(2 * time.Minute))
		id := c.Param("id")
		if _, e := uuid.Parse(id); e != nil {
			c.Status(400)
			return
		}
		var raw []byte
		var digest string
		e := s.Pool.QueryRow(c.Request.Context(), `SELECT payload FROM raw_inputs WHERE message_id=$1`, id).Scan(&raw)
		if errors.Is(e, pgx.ErrNoRows) {
			var size int64
			if e = s.Pool.QueryRow(c.Request.Context(), `SELECT size,sha256 FROM response_failures WHERE message_id=$1`, id).Scan(&size, &digest); e != nil {
				c.Status(404)
				return
			}
			c.Header("Content-Disposition", "attachment; filename=quarantine-"+id+".bin")
			c.Header("Content-Type", "application/octet-stream")
			c.Header("X-Content-Type-Options", "nosniff")
			c.Header("X-Content-SHA256", digest)
			c.Header("Content-Length", strconv.FormatInt(size, 10))
			_, _, _ = s.WriteResponsePayload(c.Request.Context(), id, c.Writer)
			return
		}
		if e != nil {
			c.Status(503)
			return
		}
		digest = Hash(raw)
		c.Header("Content-Disposition", "attachment; filename=quarantine-"+id+".bin")
		c.Header("X-Content-Type-Options", "nosniff")
		c.Header("X-Content-SHA256", digest)
		c.Data(200, "application/octet-stream", raw)
	})
	ops.POST("/replays", func(c *gin.Context) {
		var req struct {
			IDs []string `json:"message_ids"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&req) != nil || decoder.Decode(new(any)) != io.EOF {
			c.Status(400)
			return
		}
		selected, e := s.RequestReplay(c.Request.Context(), req.IDs)
		if e != nil {
			c.JSON(409, gin.H{"error": gin.H{"code": "conflict", "message": "select known parked input IDs once each"}})
			return
		}
		c.JSON(202, gin.H{"selected": selected, "status": "pending"})
	})
}
