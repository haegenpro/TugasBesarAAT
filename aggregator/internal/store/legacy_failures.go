package store

import (
	"aggregator/internal/canonical"
	"aggregator/internal/rawinput"
	"context"
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"net/http"
	"strconv"
	"time"
)

// RegisterLegacyFailures keeps historical M1 failures at their existing owner.
// The ingestion service federates metadata over HTTP, not database credentials.
// No original namespace/offset is invented for these historical records.
func (s *Store) RegisterLegacyFailures(r gin.IRoutes, token string) {
	r.GET("/internal/ingestion/legacy-failures/:id/payload", func(c *gin.Context) {
		if !rawinput.Authorize(c, token) {
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), canonical.Deadline)
		defer cancel()
		var digest, media string
		var size int64
		err := s.Pool.QueryRow(ctx, `SELECT f.digest,coalesce(p.content_type,''),coalesce(p.size,octet_length(f.raw_payload)) FROM failed_ingestions f LEFT JOIN failed_ingestion_payloads p USING(payload_id) WHERE f.message_id::text=$1`, c.Param("id")).Scan(&digest, &media, &size)
		if errors.Is(err, pgx.ErrNoRows) {
			rawinput.Error(c, canonical.Fail(404, "not_found", "unknown failure"))
			return
		}
		if err != nil {
			rawinput.Error(c, canonical.Unavailable())
			return
		}
		c.Header("Content-Type", "application/octet-stream")
		c.Header("X-Payload-SHA256", digest)
		c.Header("X-Source-Content-Type", media)
		c.Header("Content-Length", strconv.FormatInt(size, 10))
		if _, _, _, err = s.WriteFailurePayload(ctx, c.Param("id"), c.Writer); err != nil {
			_ = c.Error(err)
		}
	})
	r.GET("/internal/ingestion/legacy-failures", func(c *gin.Context) {
		if !rawinput.Authorize(c, token) {
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), canonical.Deadline)
		defer cancel()
		limit, err := strconv.Atoi(c.Query("limit"))
		if err != nil || limit < 1 || limit > 1001 {
			rawinput.Error(c, canonical.Fail(400, "bad_request", "invalid limit"))
			return
		}
		var after any
		if v := c.Query("after_time"); v != "" {
			t, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				rawinput.Error(c, canonical.Fail(400, "bad_request", "invalid time"))
				return
			}
			after = t
		}
		rows, err := s.Pool.Query(ctx, `SELECT message_id::text,source,received_at,reason,status IN ('retrying','replaying'),attempts,CASE WHEN status='replaying' THEN 'retrying' ELSE status END FROM failed_ingestions WHERE status<>'recovered' AND ($1='' OR source=$1) AND ($2='' OR status=$2 OR $2='retrying' AND status='replaying') AND ($3::timestamptz IS NULL OR (received_at,message_id::text COLLATE "C",'legacy-m1'::text COLLATE "C")>($3,$4::text COLLATE "C",$5::text COLLATE "C")) ORDER BY received_at,message_id::text COLLATE "C" LIMIT $6`, c.Query("source"), c.Query("status"), after, c.Query("after_id"), c.Query("after_namespace"), limit)
		if err != nil {
			rawinput.Error(c, canonical.Unavailable())
			return
		}
		defer rows.Close()
		type item struct {
			ID        string    `json:"message_id"`
			Source    string    `json:"source"`
			At        time.Time `json:"received_at"`
			Reason    string    `json:"reason"`
			Retryable bool      `json:"retryable"`
			Attempts  int64     `json:"attempts"`
			Status    string    `json:"status"`
		}
		type entry struct {
			Item      item   `json:"item"`
			Namespace string `json:"namespace"`
		}
		out := []entry{}
		for rows.Next() {
			var v item
			if err = rows.Scan(&v.ID, &v.Source, &v.At, &v.Reason, &v.Retryable, &v.Attempts, &v.Status); err != nil {
				rawinput.Error(c, err)
				return
			}
			v.At = v.At.UTC()
			out = append(out, entry{Item: v, Namespace: "legacy-m1"})
		}
		if rows.Err() != nil {
			rawinput.Error(c, canonical.Unavailable())
			return
		}
		c.JSON(http.StatusOK, gin.H{"items": out})
	})
}
