package audit

import (
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"aggregator/internal/pagecursor"
	"aggregator/internal/rawinput"
	"context"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type HTTP struct {
	Sessions                     Sessions
	Repository                   Repository
	Token                        string
	Signer                       pagecursor.Signer
	IngestionURL, IngestionToken string
	Client                       *http.Client
}

func (h HTTP) Register(r gin.IRoutes) {
	if h.Sessions != nil {
		r.POST("/internal/audit/snapshots", h.createSnapshot)
		r.GET("/internal/audit/snapshots/:snapshot_id", h.snapshotStatus)
	}
	r.GET("/internal/audit/events", h.events)
	r.GET("/internal/audit/checksum", h.checksum)
	r.GET("/internal/audit/location", h.location)
	r.GET("/internal/audit/unprocessed", h.unprocessed)
}
func (h HTTP) authorized(c *gin.Context) bool {
	if h.Token == "" || !strings.HasPrefix(c.GetHeader("Authorization"), "Bearer ") {
		rawinput.Error(c, canonical.Fail(401, "unauthorized", "audit bearer required"))
		return false
	}
	if c.GetHeader("Authorization") != "Bearer "+h.Token {
		rawinput.Error(c, canonical.Fail(401, "unauthorized", "invalid audit token"))
		return false
	}
	return true
}
func parameters(c *gin.Context, allowed ...string) bool {
	set := map[string]bool{}
	for _, s := range allowed {
		set[s] = true
	}
	for k, v := range c.Request.URL.Query() {
		if !set[k] || len(v) != 1 {
			rawinput.Error(c, canonical.Fail(400, "bad_request", "unknown or repeated query parameter"))
			return false
		}
	}
	return true
}
func source(c *gin.Context) (string, bool) {
	s := c.Query("source")
	_, present := c.Request.URL.Query()["source"]
	if present && (s == "" || !domain.ValidSource(s)) {
		rawinput.Error(c, canonical.Fail(400, "bad_request", "invalid source"))
		return "", false
	}
	return s, true
}
func (h HTTP) events(c *gin.Context) {
	if !h.authorized(c) || !parameters(c, "source", "since", "limit", "cursor", "snapshot_id") {
		return
	}
	s, ok := source(c)
	if !ok {
		return
	}
	q := Query{Source: s, Limit: 100, Scope: "audit"}
	var snapshot *string
	if v, present := c.Request.URL.Query()["snapshot_id"]; present {
		if h.Sessions == nil {
			rawinput.Error(c, canonical.Fail(400, "bad_request", "sessions disabled"))
			return
		}
		id, e := uuid.Parse(v[0])
		if e != nil || id.String() != v[0] {
			rawinput.Error(c, canonical.Fail(400, "bad_request", "invalid snapshot"))
			return
		}
		q.Snapshot = id.String()
		snapshot = &q.Snapshot
	}
	if v, present := c.Request.URL.Query()["since"]; present {
		t, err := time.Parse(time.RFC3339Nano, v[0])
		_, offset := t.Zone()
		if err != nil || offset != 0 {
			rawinput.Error(c, canonical.Fail(400, "bad_request", "invalid UTC since"))
			return
		}
		q.Since = t.UTC()
	}
	if v, present := c.Request.URL.Query()["limit"]; present {
		n, err := strconv.Atoi(v[0])
		if err != nil || n < 1 || n > 1000 {
			rawinput.Error(c, canonical.Fail(400, "bad_request", "limit must be 1..1000"))
			return
		}
		q.Limit = n
	}
	if v, present := c.Request.URL.Query()["cursor"]; present {
		p, err := h.Signer.DecodeSnapshot(v[0], "audit/events", q.Scope, q.Filters(), snapshot)
		if err != nil {
			rawinput.Error(c, canonical.Fail(400, "bad_request", "invalid cursor"))
			return
		}
		if p.ID != "" {
			if id, err := uuid.Parse(p.ID); err != nil || id.String() != p.ID || p.Time.IsZero() {
				rawinput.Error(c, canonical.Fail(400, "bad_request", "invalid ordering key"))
				return
			}
		}
		q.After = p
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), canonical.Deadline)
	defer cancel()
	var p Page
	var err error
	if snapshot != nil {
		p, err = h.Sessions.SnapshotEvents(ctx, q)
		c.Header("X-Audit-Snapshot-ID", q.Snapshot)
	} else {
		p, err = h.Repository.AuditEvents(ctx, q)
	}
	if err != nil {
		rawinput.Error(c, err)
		return
	}
	c.JSON(200, p)
}
func (h HTTP) checksum(c *gin.Context) {
	if !h.authorized(c) || !parameters(c, "source", "snapshot_id") {
		return
	}
	s, ok := source(c)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), canonical.Deadline)
	defer cancel()
	var v Checksum
	var err error
	if id, present := c.Request.URL.Query()["snapshot_id"]; present {
		u, e := uuid.Parse(id[0])
		if h.Sessions == nil || e != nil || u.String() != id[0] {
			rawinput.Error(c, canonical.Fail(400, "bad_request", "invalid snapshot"))
			return
		}
		v, err = h.Sessions.SnapshotChecksum(ctx, id[0], s)
		c.Header("X-Audit-Snapshot-ID", id[0])
	} else {
		v, err = h.Repository.AuditChecksum(ctx, s)
	}
	if err != nil {
		rawinput.Error(c, err)
		return
	}
	c.JSON(200, v)
}
func (h HTTP) location(c *gin.Context) {
	if !h.authorized(c) || !parameters(c, "hazard_id") {
		return
	}
	id, err := uuid.Parse(c.Query("hazard_id"))
	if err != nil || id.String() != c.Query("hazard_id") {
		rawinput.Error(c, canonical.Fail(400, "bad_request", "canonical hazard UUID required"))
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), canonical.Deadline)
	defer cancel()
	v, err := h.Repository.AuditLocation(ctx, id.String())
	if err != nil {
		rawinput.Error(c, err)
		return
	}
	c.JSON(200, v)
}
func (h HTTP) unprocessed(c *gin.Context) {
	if !h.authorized(c) || !parameters(c, "source", "limit", "cursor") {
		return
	}
	if _, ok := source(c); !ok {
		return
	}
	if h.IngestionURL == "" || h.IngestionToken == "" || h.Client == nil {
		rawinput.Error(c, canonical.Unavailable())
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), canonical.Deadline)
	defer cancel()
	u, err := url.Parse(strings.TrimRight(h.IngestionURL, "/") + "/internal/ingestion/failures")
	if err != nil {
		rawinput.Error(c, canonical.Unavailable())
		return
	}
	u.RawQuery = c.Request.URL.RawQuery
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		rawinput.Error(c, canonical.Unavailable())
		return
	}
	req.Header.Set("Authorization", "Bearer "+h.IngestionToken)
	resp, err := h.Client.Do(req)
	if err != nil {
		rawinput.Error(c, canonical.Unavailable())
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		rawinput.Error(c, canonical.Unavailable())
		return
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, (4<<20)+1))
	if err != nil || len(b) > 4<<20 {
		rawinput.Error(c, canonical.Unavailable())
		return
	}
	var body struct {
		Items       []json.RawMessage `json:"items"`
		Next        *string           `json:"next_cursor"`
		Partial     *bool             `json:"partial"`
		Unavailable []string          `json:"unavailable_units"`
	}
	if json.Unmarshal(b, &body) != nil || body.Items == nil || body.Partial == nil || body.Unavailable == nil {
		rawinput.Error(c, canonical.Unavailable())
		return
	}
	c.Data(200, "application/json", b)
}
