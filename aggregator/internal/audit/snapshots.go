package audit

import (
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"aggregator/internal/rawinput"
	"bytes"
	"context"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"io"
	"time"
)

type Session struct {
	ID       string         `json:"snapshot_id"`
	Status   string         `json:"status"`
	Source   *domain.Source `json:"source"`
	Cut      *time.Time     `json:"as_of"`
	Expires  *time.Time     `json:"expires_at"`
	Absolute time.Time      `json:"absolute_expires_at"`
	Reason   *string        `json:"reason,omitempty"`
}
type Sessions interface {
	StartSnapshot(context.Context, *domain.Source) (Session, error)
	SnapshotStatus(context.Context, string) (Session, error)
	SnapshotEvents(context.Context, Query) (Page, error)
	SnapshotChecksum(context.Context, string, string) (Checksum, error)
}

func (h HTTP) createSnapshot(c *gin.Context) {
	if !h.authorized(c) {
		return
	}
	var request struct {
		Source *domain.Source `json:"source"`
	}
	b, e := io.ReadAll(io.LimitReader(c.Request.Body, 1025))
	if e != nil || len(b) > 1024 || canonical.Decode(bytes.NewReader(b), &request) != nil || (request.Source != nil && !domain.ValidSource(string(*request.Source))) {
		rawinput.Error(c, canonical.Fail(400, "bad_request", "invalid snapshot scope"))
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), canonical.Deadline)
	defer cancel()
	v, e := h.Sessions.StartSnapshot(ctx, request.Source)
	if e != nil {
		rawinput.Error(c, e)
		return
	}
	c.Header("X-Audit-Snapshot-ID", v.ID)
	c.JSON(202, v)
}
func (h HTTP) snapshotStatus(c *gin.Context) {
	if !h.authorized(c) {
		return
	}
	id, e := uuid.Parse(c.Param("snapshot_id"))
	if e != nil || id.String() != c.Param("snapshot_id") {
		rawinput.Error(c, canonical.Fail(400, "bad_request", "invalid snapshot identity"))
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), canonical.Deadline)
	defer cancel()
	v, e := h.Sessions.SnapshotStatus(ctx, id.String())
	if e != nil {
		rawinput.Error(c, e)
		return
	}
	c.JSON(200, v)
}
