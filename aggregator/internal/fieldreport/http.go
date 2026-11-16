package fieldreport

import (
	"aggregator/internal/canonical"
	"aggregator/internal/middleware"
	"aggregator/internal/rawinput"
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type HTTP struct {
	Ledger *Ledger
	Token  string
}

func (h HTTP) Register(r gin.IRoutes) {
	r.POST("/internal/field-reports", h.accept)
	r.GET("/internal/field-reports/:receipt_id", h.status)
}

func (h HTTP) accept(c *gin.Context) {
	if !rawinput.Authorize(c, h.Token) {
		return
	}
	media, ok := MediaType(c.GetHeader("Content-Type"))
	if !ok {
		rawinput.Error(c, canonical.Fail(415, "unsupported_media_type", "field reports must be application/json"))
		return
	}
	body, err := io.ReadAll(io.LimitReader(c.Request.Body, MaxBody+1))
	if err != nil {
		rawinput.Error(c, invalid("unreadable request body"))
		return
	}
	if len(body) > MaxBody {
		rawinput.Error(c, canonical.Fail(413, "payload_too_large", "field report exceeds the accepted size"))
		return
	}
	cid := c.GetHeader(middleware.CorrelationIDHeader)
	if cid == "" {
		cid = uuid.NewString()
	}
	var trace *string
	if v := c.GetHeader("Traceparent"); v != "" {
		trace = &v
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), canonical.Deadline)
	defer cancel()
	receipt, err := h.Ledger.Accept(ctx, Submission{Submitter: c.GetHeader("X-Submitter-ID"), Key: c.GetHeader("Idempotency-Key"), ContentType: media, Body: body, CorrelationID: cid, Traceparent: trace})
	if err != nil {
		if errors.Is(err, errPending) || ctx.Err() != nil {
			c.Header("Retry-After", "1")
		}
		rawinput.Error(c, err)
		return
	}
	c.JSON(http.StatusAccepted, receipt)
}

func (h HTTP) status(c *gin.Context) {
	if !rawinput.Authorize(c, h.Token) {
		return
	}
	submitter := c.GetHeader("X-Submitter-ID")
	if c.GetHeader("X-Submitter-Role") == "internal" {
		submitter = ""
	} else if submitter == "" {
		rawinput.Error(c, canonical.Fail(403, "forbidden", "submitter identity required"))
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), canonical.Deadline)
	defer cancel()
	receipt, err := h.Ledger.Status(ctx, c.Param("receipt_id"), submitter)
	if err != nil {
		rawinput.Error(c, err)
		return
	}
	c.JSON(http.StatusOK, receipt)
}
