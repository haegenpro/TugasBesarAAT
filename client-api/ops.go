package main

import (
	"bytes"
	"clientapi/internal/platform"
	"embed"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"io"
	"net/http"
	"net/url"
	"strings"
)

//go:embed ops-web/*
var opsFiles embed.FS

func internalOps(c *gin.Context) {
	p, ok := c.MustGet("principal").(principal)
	if !ok || p.Role != "internal" {
		c.AbortWithStatusJSON(403, gin.H{"error": platform.ProblemStatus(403, "Internal Ops role required")})
		return
	}
	c.Next()
}
func (a *api) opsAsset(c *gin.Context) {
	file := c.Param("file")
	media := "text/html; charset=utf-8"
	if file == "" {
		file = "index.html"
	} else if file == "app.js" {
		media = "application/javascript; charset=utf-8"
	} else {
		c.Status(404)
		return
	}
	raw, e := opsFiles.ReadFile("ops-web/" + file)
	if e != nil {
		c.Status(404)
		return
	}
	c.Header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
	c.Header("Cache-Control", "no-store")
	c.Header("X-Content-Type-Options", "nosniff")
	c.Data(200, media, raw)
}
func (a *api) opsTarget(c *gin.Context) (string, string, bool) {
	switch c.DefaultQuery("owner", "canonical") {
	case "canonical":
		return a.cfg.AggregatorURL, a.cfg.OpsToken, true
	case "ingestion":
		if a.cfg.IngestionURL != "" {
			return a.cfg.IngestionURL, a.cfg.FieldReportToken, true
		}
	}
	c.JSON(400, gin.H{"error": platform.ProblemStatus(400, "owner must be canonical or ingestion")})
	return "", "", false
}
func (a *api) opsForward(c *gin.Context, path string, payload bool) {
	base, token, ok := a.opsTarget(c)
	if !ok {
		return
	}
	var body []byte
	var e error
	if payload {
		body, e = io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20))
		if e != nil {
			c.Status(413)
			return
		}
	}
	req, e := http.NewRequestWithContext(c.Request.Context(), c.Request.Method, joinURL(base, path), bytes.NewReader(body))
	if e != nil {
		c.Status(503)
		return
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Correlation-ID", c.GetString("correlation_id"))
	response, e := a.client.Do(req)
	if e != nil {
		c.JSON(503, gin.H{"error": platform.ProblemStatus(503, "operation owner unavailable")})
		return
	}
	defer response.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(response.Body, (2<<20)+1))
	if e != nil || len(raw) > 2<<20 {
		c.JSON(503, gin.H{"error": platform.ProblemStatus(503, "operation result unavailable")})
		return
	}
	c.Header("Cache-Control", "no-store")
	c.Data(response.StatusCode, "application/json", raw)
}
func (a *api) opsFailures(c *gin.Context) {
	q := url.Values{}
	for _, key := range []string{"source", "limit", "cursor"} {
		if v := c.Query(key); v != "" {
			q.Set(key, v)
		}
	}
	a.opsForward(c, "/internal/ops/failures?"+q.Encode(), false)
}
func (a *api) opsReplays(c *gin.Context) { a.opsForward(c, "/internal/ops/replays", true) }
func (a *api) opsPolicy(c *gin.Context) {
	if c.Query("owner") == "ingestion" {
		c.Status(400)
		return
	}
	a.opsForward(c, "/internal/ops/schema-policy", c.Request.Method == "PUT")
}
func (a *api) opsProgress(c *gin.Context) {
	c.Request.URL.RawQuery = "owner=ingestion"
	a.opsForward(c, "/internal/ingestion/progress", false)
}
func (a *api) opsPayload(c *gin.Context) {
	id := c.Param("id")
	if _, e := uuid.Parse(id); e != nil {
		c.Status(400)
		return
	}
	base, token, ok := a.opsTarget(c)
	if !ok {
		return
	}
	req, e := http.NewRequestWithContext(c.Request.Context(), "GET", joinURL(base, "/internal/ops/failures/"+url.PathEscape(id)+"/payload"), nil)
	if e != nil {
		c.Status(503)
		return
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Correlation-ID", c.GetString("correlation_id"))
	response, e := a.client.Do(req)
	if e != nil {
		c.Status(503)
		return
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		c.Status(response.StatusCode)
		return
	}
	c.Header("Content-Type", "application/octet-stream")
	c.Header("Content-Disposition", "attachment; filename=quarantine-"+id+".bin")
	c.Header("X-Content-Type-Options", "nosniff")
	for _, header := range []string{"Content-Length", "X-Content-SHA256"} {
		if v := response.Header.Get(header); v != "" && !strings.ContainsAny(v, "\r\n") {
			c.Header(header, v)
		}
	}
	c.Status(200)
	_, _ = io.Copy(c.Writer, response.Body)
}
