package main

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type loginRequest struct {
	ClientID     string `json:"client_id" binding:"required"`
	ClientSecret string `json:"client_secret" binding:"required"`
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token" binding:"required"`
}

func (a *api) router() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery(), correlation(), requestLog(a.log))
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "client-api"})
	})
	r.GET("/ready", a.ready)
	limited := r.Group("/", limitInFlight(a.cfg.MaxInFlight, a.log))
	limited.POST("/auth/login", a.login)
	limited.POST("/auth/refresh", a.refresh)
	limited.POST("/auth/logout", a.logout)
	protected := limited.Group("/", a.authenticate(), limitInFlightPerClient(a.cfg.MaxInFlightPerClient, a.log))
	protected.GET("/hazards", a.listHazards)
	protected.GET("/hazards/:id", a.getHazard)
	protected.GET("/notices", a.listNotices)
	protected.GET("/notices/:id", a.getNotice)
	return r
}

func correlation() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader("X-Correlation-ID")
		if id == "" {
			id = uuid.NewString()
		}
		c.Set("correlation_id", id)
		c.Header("X-Correlation-ID", id)
		c.Next()
	}
}

// limitInFlight bounds concurrent requests. When all slots are taken it fails
// fast with 429 instead of queueing, so overload cannot pile up goroutines or
// upstream calls to Auth and the Aggregator. n <= 0 disables the limit.
func limitInFlight(n int, log logger) gin.HandlerFunc {
	if n <= 0 {
		return func(c *gin.Context) { c.Next() }
	}
	slots := make(chan struct{}, n)
	return func(c *gin.Context) {
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
			c.Next()
		default:
			log.Warn("request rejected", "service", "client-api", "correlation_id", c.GetString("correlation_id"), "reason", "max_in_flight", "limit", n)
			c.Header("Retry-After", "1")
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "server busy"})
		}
	}
}

func requestLog(log logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		log.Info("http request", "service", "client-api", "correlation_id", c.GetString("correlation_id"), "method", c.Request.Method, "path", c.FullPath(), "status_code", c.Writer.Status(), "latency_ms", time.Since(start).Milliseconds())
	}
}

func (a *api) ready(c *gin.Context) {
	authCode, authErr := a.call(c, "auth-service", http.MethodGet, joinURL(a.cfg.AuthURL, "/health"), nil, false, nil)
	aggCode, aggErr := a.call(c, "aggregator", http.MethodGet, joinURL(a.cfg.AggregatorURL, "/ready"), nil, false, nil)
	if authErr != nil || aggErr != nil || authCode != http.StatusOK || aggCode != http.StatusOK {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable", "auth": authErr == nil && authCode == http.StatusOK, "aggregator": aggErr == nil && aggCode == http.StatusOK})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready", "auth": true, "aggregator": true})
}

func (a *api) login(c *gin.Context) {
	var req loginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "client_id and client_secret are required"})
		return
	}
	a.proxyAuth(c, "/internal/login", req)
}

func (a *api) refresh(c *gin.Context) {
	var req refreshRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "refresh_token is required"})
		return
	}
	a.proxyAuth(c, "/internal/refresh", req)
}

func (a *api) logout(c *gin.Context) {
	var req refreshRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "refresh_token is required"})
		return
	}
	a.proxyAuth(c, "/internal/logout", req)
}

func (a *api) proxyAuth(c *gin.Context, path string, payload any) {
	var response map[string]any
	status, err := a.call(c, "auth-service", http.MethodPost, joinURL(a.cfg.AuthURL, path), payload, true, &response)
	if err != nil || status >= 500 || status == 0 {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "auth service unavailable"})
		return
	}
	if status == http.StatusNoContent {
		c.Status(status)
		return
	}
	if status != http.StatusOK {
		c.JSON(status, gin.H{"error": "invalid credentials or token"})
		return
	}
	c.JSON(http.StatusOK, response)
}

func (a *api) authenticate() gin.HandlerFunc {
	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		parts := strings.SplitN(header, " ", 2)
		if len(parts) != 2 || parts[0] != "Bearer" || strings.TrimSpace(parts[1]) == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "bearer token required"})
			return
		}
		var principal principal
		status, err := a.call(c, "auth-service", http.MethodPost, joinURL(a.cfg.AuthURL, "/internal/introspect"), gin.H{"access_token": parts[1]}, true, &principal)
		if err != nil || status >= 500 || status == 0 {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "auth service unavailable"})
			return
		}
		if status != http.StatusOK {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid access token"})
			return
		}
		c.Set("principal", principal)
		c.Next()
	}
}

func hasScope(scopes, required string) bool {
	for _, scope := range strings.Fields(scopes) {
		if scope == required {
			return true
		}
	}
	return false
}

func (a *api) view(c *gin.Context) (string, bool) {
	principal := c.MustGet("principal").(principal)
	view := c.DefaultQuery("view", "")
	if view == "" {
		if hasScope(principal.Scope, "hazards:raw") {
			view = "raw"
		} else {
			view = "summary"
		}
	}
	if view != "summary" && view != "raw" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "view must be summary or raw"})
		return "", false
	}
	if view == "raw" && !hasScope(principal.Scope, "hazards:raw") {
		c.JSON(http.StatusForbidden, gin.H{"error": "raw view forbidden for media"})
		return "", false
	}
	return view, true
}

func (a *api) listHazards(c *gin.Context) {
	view, ok := a.view(c)
	if !ok {
		return
	}
	query, err := filterQuery(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	endpoint := joinURL(a.cfg.AggregatorURL, "/internal/hazards")
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	var list aggregatorList
	status, err := a.call(c, "aggregator", http.MethodGet, endpoint, nil, false, &list)
	if err != nil || status != http.StatusOK {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "aggregator unavailable"})
		return
	}
	if list.Data == nil {
		list.Data = []hazard{}
	}
	if view == "summary" {
		summaries := make([]summaryHazard, 0, len(list.Data))
		for _, item := range list.Data {
			summaries = append(summaries, item.summary())
		}
		c.JSON(http.StatusOK, gin.H{"data": summaries, "count": len(summaries), "as_of": list.AsOf, "source_status": list.SourceStatus, "metadata": list.Metadata})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": list.Data, "count": len(list.Data), "as_of": list.AsOf, "source_status": list.SourceStatus, "metadata": list.Metadata})
}

func (a *api) getHazard(c *gin.Context) {
	view, ok := a.view(c)
	if !ok {
		return
	}
	id := c.Param("id")
	if _, err := uuid.Parse(id); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid hazard ID"})
		return
	}
	var detail aggregatorDetail
	status, err := a.call(c, "aggregator", http.MethodGet, joinURL(a.cfg.AggregatorURL, "/internal/hazards/"+id), nil, false, &detail)
	if err != nil || status >= 500 || status == 0 {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "aggregator unavailable"})
		return
	}
	if status == http.StatusNotFound {
		c.JSON(http.StatusNotFound, gin.H{"error": "hazard not found"})
		return
	}
	if status != http.StatusOK {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "aggregator unavailable"})
		return
	}
	item := detail.Data
	metadata := detail.SourceStatus
	if view == "summary" {
		c.JSON(http.StatusOK, gin.H{"data": item.summary(), "source_status": metadata, "metadata": detail.Metadata, "as_of": detail.AsOf})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": item, "source_status": metadata, "metadata": detail.Metadata, "as_of": detail.AsOf})
}
