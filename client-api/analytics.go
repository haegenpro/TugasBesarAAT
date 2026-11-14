package main

import (
	"context"
	"embed"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"io"
	"net/http"
	"strings"
	"time"
)

//go:embed analytics-web/*
var analyticsFiles embed.FS

type analyticsBucket struct {
	At      string           `json:"at"`
	Counts  map[string]int64 `json:"counts"`
	Tsunami int64            `json:"tsunami_warning_overlay"`
}
type analyticsArea struct {
	Name  string `json:"area_name"`
	Count int64  `json:"count"`
}
type analyticsResponse struct {
	Filters struct {
		From     string `json:"from"`
		To       string `json:"to"`
		Interval string `json:"interval"`
		Source   string `json:"source,omitempty"`
		Snapshot string `json:"snapshot_id,omitempty"`
	} `json:"filters"`
	Total       int64             `json:"total"`
	Types       map[string]int64  `json:"by_type"`
	Severity    map[string]int64  `json:"by_severity"`
	Areas       []analyticsArea   `json:"by_area"`
	Series      []analyticsBucket `json:"series"`
	Tsunami     int64             `json:"tsunami_warning_overlay"`
	AsOf        string            `json:"as_of"`
	Mode        string            `json:"mode"`
	Generation  int64             `json:"generation"`
	Complete    bool              `json:"data_complete"`
	Partial     bool              `json:"partial"`
	Unavailable []string          `json:"unavailable_units"`
	Freshness   json.RawMessage   `json:"source_freshness"`
}

func (a *api) analyticsAsset(c *gin.Context) {
	file := c.Param("file")
	contentType := "text/html; charset=utf-8"
	if file == "" {
		file = "index.html"
	} else {
		switch file {
		case "app.js":
			contentType = "application/javascript; charset=utf-8"
		case "style.css":
			contentType = "text/css; charset=utf-8"
		default:
			c.Status(404)
			return
		}
	}
	body, e := analyticsFiles.ReadFile("analytics-web/" + file)
	if e != nil {
		c.Status(404)
		return
	}
	c.Header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
	c.Header("Cache-Control", "no-store")
	c.Header("X-Content-Type-Options", "nosniff")
	c.Data(200, contentType, body)
}
func (a *api) analyticsSummary(c *gin.Context) {
	p := c.MustGet("principal").(principal)
	if !hasScope(p.Scope, "hazards:summary") && !hasScope(p.Scope, "hazards:raw") {
		c.JSON(403, gin.H{"error": gin.H{"code": "forbidden", "message": "hazard scope required"}})
		return
	}
	for k, v := range c.Request.URL.Query() {
		if len(v) != 1 {
			c.JSON(400, gin.H{"error": gin.H{"code": "bad_request", "message": "repeated filter"}})
			return
		}
		switch k {
		case "from", "to", "interval", "source", "snapshot_id":
		default:
			c.JSON(400, gin.H{"error": gin.H{"code": "bad_request", "message": "unknown filter"}})
			return
		}
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()
	req, e := http.NewRequestWithContext(ctx, "GET", joinURL(a.cfg.AggregatorURL, "/internal/analytics/summary")+"?"+c.Request.URL.RawQuery, nil)
	if e != nil {
		c.Status(503)
		return
	}
	req.Header.Set("Authorization", "Bearer "+a.cfg.AnalyticsToken)
	req.Header.Set("X-Correlation-ID", c.GetString("correlation_id"))
	response, e := a.client.Do(req)
	if e != nil {
		c.JSON(503, gin.H{"error": gin.H{"code": "temporarily_unavailable", "message": "analytics unavailable"}})
		return
	}
	defer response.Body.Close()
	body, e := io.ReadAll(io.LimitReader(response.Body, (2<<20)+1))
	if e != nil || len(body) > 2<<20 {
		c.JSON(503, gin.H{"error": gin.H{"code": "temporarily_unavailable", "message": "analytics response unavailable"}})
		return
	}
	c.Header("Cache-Control", "no-store")
	if response.StatusCode != 200 {
		status := response.StatusCode
		switch status {
		case 400, 429, 503:
		default:
			status = 503
		}
		var failure struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal(body, &failure) != nil || failure.Error.Code == "" {
			c.JSON(status, gin.H{"error": gin.H{"code": "temporarily_unavailable", "message": "analytics unavailable"}})
		} else {
			c.JSON(status, failure)
		}
		return
	}
	var out analyticsResponse
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&out) != nil || out.Types == nil || out.Severity == nil || out.Areas == nil || out.Series == nil || out.Unavailable == nil || out.AsOf == "" {
		c.JSON(503, gin.H{"error": gin.H{"code": "temporarily_unavailable", "message": "invalid analytics response"}})
		return
	}
	c.JSON(200, out)
}
