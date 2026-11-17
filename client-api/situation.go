package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

const situationTimeout = time.Second

func (a *api) situation(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), situationTimeout)
	defer cancel()
	unavailable := func(reason string) {
		c.JSON(http.StatusOK, gin.H{"status": "unavailable", "reasons": []string{reason}, "ai_summary": nil, "facts": nil, "disclaimer": "Ringkasan AI tidak tersedia. Gunakan /hazards dan peringatan resmi."})
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, joinURL(a.cfg.SituationURL, "/summary"), nil)
	if err != nil {
		unavailable("ringkasan AI tidak dapat dihubungi")
		return
	}
	req.Header.Set("X-Correlation-ID", c.GetString("correlation_id"))
	start := time.Now()
	resp, err := a.client.Do(req)
	if err != nil {
		a.log.Warn("outbound request", "service", "client-api", "correlation_id", c.GetString("correlation_id"), "target_service", "situation-summarizer", "latency_ms", time.Since(start).Milliseconds(), "success", false, "error", err.Error())
		unavailable("ringkasan AI tidak merespons")
		return
	}
	defer resp.Body.Close()
	var body json.RawMessage
	if resp.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&body) != nil {
		unavailable("ringkasan AI mengembalikan respons tidak valid")
		return
	}
	c.Data(http.StatusOK, "application/json; charset=utf-8", body)
}
