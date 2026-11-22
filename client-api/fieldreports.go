package main

import (
	"bytes"
	"clientapi/internal/platform"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

const maxFieldReportBytes = 1 << 20

func (a *api) submitFieldReport(c *gin.Context) {
	p, ok := c.MustGet("principal").(principal)
	if !ok || p.Role != "field" {
		c.JSON(http.StatusForbidden, gin.H{"error": platform.ProblemStatus(http.StatusForbidden, "only Tim Lapangan can submit field reports")})
		return
	}
	body, err := io.ReadAll(io.LimitReader(c.Request.Body, maxFieldReportBytes+1))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": platform.ProblemStatus(http.StatusBadRequest, "unreadable request body")})
		return
	}
	if len(body) > maxFieldReportBytes {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": platform.ProblemStatus(http.StatusRequestEntityTooLarge, "field report exceeds 1 MiB")})
		return
	}
	headers := http.Header{}
	headers.Set("Content-Type", c.GetHeader("Content-Type"))
	headers.Set("Idempotency-Key", c.GetHeader("Idempotency-Key"))
	headers.Set("X-Submitter-ID", p.ClientID)
	a.forwardIngestion(c, http.MethodPost, "/internal/field-reports", headers, body)
}

func (a *api) fieldReportStatus(c *gin.Context) {
	p, ok := c.MustGet("principal").(principal)
	if !ok || p.Role != "field" && p.Role != "internal" {
		c.JSON(http.StatusForbidden, gin.H{"error": platform.ProblemStatus(http.StatusForbidden, "field report status is limited to the submitter and Internal Ops")})
		return
	}
	headers := http.Header{}
	headers.Set("X-Submitter-ID", p.ClientID)
	headers.Set("X-Submitter-Role", p.Role)
	a.forwardIngestion(c, http.MethodGet, "/internal/field-reports/"+c.Param("receipt_id"), headers, nil)
}

func (a *api) forwardIngestion(c *gin.Context, method, path string, headers http.Header, body []byte) {
	req, err := http.NewRequestWithContext(c.Request.Context(), method, joinURL(a.cfg.IngestionURL, path), bytes.NewReader(body))
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": platform.ProblemStatus(http.StatusServiceUnavailable, "field report service unavailable")})
		return
	}
	req.Header = headers
	req.Header.Set("Authorization", "Bearer "+a.cfg.FieldReportToken)
	req.Header.Set("X-Correlation-ID", c.GetString("correlation_id"))
	start := time.Now()
	resp, err := a.client.Do(req)
	if err != nil {
		countUpstreamFailure("ingestion", failureClass(err))
		a.log.Warn("outbound request", "service", "client-api", "correlation_id", c.GetString("correlation_id"), "target_service", "ingestion", "target_endpoint", path, "latency_ms", time.Since(start).Milliseconds(), "success", false, "error", err.Error())
		c.Header("Retry-After", "1")
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": platform.ProblemStatus(http.StatusServiceUnavailable, "field report service unavailable")})
		return
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	a.log.Info("outbound request", "service", "client-api", "correlation_id", c.GetString("correlation_id"), "target_service", "ingestion", "target_endpoint", path, "status_code", resp.StatusCode, "latency_ms", time.Since(start).Milliseconds(), "success", resp.StatusCode < 500)
	if resp.StatusCode >= 500 {
		countUpstreamFailure("ingestion", "status_5xx")
	}
	if err != nil || resp.Header.Get("Content-Type") == "" {
		if err != nil {
			countUpstreamFailure("ingestion", failureClass(err))
		} else {
			countUpstreamFailure("ingestion", "unreadable")
		}
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": platform.ProblemStatus(http.StatusServiceUnavailable, "field report service unavailable")})
		return
	}
	if retry := resp.Header.Get("Retry-After"); retry != "" {
		c.Header("Retry-After", retry)
	}
	c.Data(resp.StatusCode, "application/json; charset=utf-8", payload)
}
