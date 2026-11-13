package rawinput

import (
	"aggregator/internal/canonical"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"github.com/gin-gonic/gin"
	"net/http"
	"strconv"
	"strings"
)

type AppendRequest struct {
	Version   int                 `json:"contract_version"`
	Ownership canonical.Ownership `json:"ownership"`
	Messages  []Envelope          `json:"messages"`
}
type ReceiptItem struct {
	MessageID string `json:"message_id"`
	Outcome   string `json:"outcome"`
}
type Receipt struct {
	Namespace string        `json:"namespace"`
	Through   int64         `json:"durable_through"`
	Items     []ReceiptItem `json:"items"`
}
type HistoryPage struct {
	Items     []Envelope `json:"items"`
	NextAfter *int64     `json:"next_after"`
}
type History interface {
	AppendHistory(context.Context, AppendRequest) (Receipt, error)
	ReadHistory(context.Context, string, *int64, int) (HistoryPage, error)
}
type HTTP struct {
	Repository History
	Token      string
}

const HistoryInlineLimit = 4 << 20

func Error(c *gin.Context, err error) {
	var p *canonical.Error
	if !errors.As(err, &p) {
		p = canonical.Unavailable()
	}
	detail := gin.H{"code": p.Code, "message": p.Message}
	if p.Details != nil {
		detail["details"] = p.Details
	}
	c.JSON(p.Status, gin.H{"error": detail})
}
func Authorize(c *gin.Context, token string) bool {
	v := c.GetHeader("Authorization")
	if token == "" || !strings.HasPrefix(v, "Bearer ") {
		Error(c, canonical.Fail(401, "unauthorized", "service credential required"))
		return false
	}
	a, b := sha256.Sum256([]byte(strings.TrimPrefix(v, "Bearer "))), sha256.Sum256([]byte(token))
	if subtle.ConstantTimeCompare(a[:], b[:]) != 1 {
		Error(c, canonical.Fail(403, "forbidden", "credential denied"))
		return false
	}
	return true
}
func (h HTTP) Register(r gin.IRoutes) {
	r.POST("/internal/satellite/history", h.append)
	r.GET("/internal/satellite/history", h.history)
}
func (h HTTP) append(c *gin.Context) {
	if !Authorize(c, h.Token) {
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), canonical.Deadline)
	defer cancel()
	body := http.MaxBytesReader(c.Writer, c.Request.Body, HistoryInlineLimit)
	stop := context.AfterFunc(ctx, func() { _ = body.Close() })
	defer stop()
	var req AppendRequest
	if err := canonical.Decode(body, &req); err != nil {
		var over *http.MaxBytesError
		if errors.As(err, &over) {
			Error(c, canonical.Fail(413, "payload_too_large", "history request exceeds 4 MiB"))
		} else {
			Error(c, canonical.Fail(400, "invalid_request", "invalid history request"))
		}
		return
	}
	if req.Version != 1 || len(req.Messages) < 1 || len(req.Messages) > 32 {
		Error(c, canonical.Fail(400, "invalid_request", "version 1 and 1..32 messages required"))
		return
	}
	for _, m := range req.Messages {
		if m.Source != "SATELIT" {
			Error(c, canonical.Fail(400, "invalid_request", "satellite history only"))
			return
		}
		if err := Validate(m); err != nil {
			Error(c, err)
			return
		}
	}
	v, err := h.Repository.AppendHistory(ctx, req)
	if err != nil {
		Error(c, err)
		return
	}
	c.JSON(200, v)
}
func (h HTTP) history(c *gin.Context) {
	if !Authorize(c, h.Token) {
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), canonical.Deadline)
	defer cancel()
	q := c.Request.URL.Query()
	for k, v := range q {
		if (k != "namespace" && k != "after" && k != "limit") || len(v) != 1 {
			Error(c, canonical.Fail(400, "invalid_request", "invalid history query"))
			return
		}
	}
	ns := c.Query("namespace")
	limit := 100
	var after *int64
	if !text(ns) {
		Error(c, canonical.Fail(400, "invalid_request", "namespace required"))
		return
	}
	if raw, ok := q["after"]; ok {
		n, e := strconv.ParseInt(raw[0], 10, 64)
		if e != nil || n < 0 {
			Error(c, canonical.Fail(400, "invalid_request", "invalid after"))
			return
		}
		after = &n
	}
	if raw, ok := q["limit"]; ok {
		n, e := strconv.Atoi(raw[0])
		if e != nil || n < 1 || n > 1000 {
			Error(c, canonical.Fail(400, "invalid_request", "invalid limit"))
			return
		}
		limit = n
	}
	v, err := h.Repository.ReadHistory(ctx, ns, after, limit)
	if err != nil {
		Error(c, err)
		return
	}
	c.JSON(200, v)
}
