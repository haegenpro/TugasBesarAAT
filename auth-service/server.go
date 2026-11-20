package main

import (
	"authservice/internal/platform"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type authServer struct {
	config config
	store  Sessions
	log    *slog.Logger
}

type loginRequest struct {
	ClientID     string `json:"client_id" binding:"required"`
	ClientSecret string `json:"client_secret" binding:"required"`
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token" binding:"required"`
}

type verifyRequest struct {
	AccessToken string `json:"access_token" binding:"required"`
}

func sameSecret(a, b string) bool {
	aHash := sha256.Sum256([]byte(a))
	bHash := sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(aHash[:], bHash[:]) == 1
}

func (a *authServer) router() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.HandleMethodNotAllowed = true
	r.NoRoute(func(c *gin.Context) { c.JSON(404, gin.H{"error": platform.ProblemStatus(404, "not found")}) })
	r.NoMethod(func(c *gin.Context) { c.JSON(405, gin.H{"error": platform.ProblemStatus(405, "method not allowed")}) })
	r.Use(gin.Recovery(), correlation(), requestLog(a.log, "auth-service"))
	r.GET("/health", func(c *gin.Context) { c.JSON(200, gin.H{"status": "ok", "service": "auth-service"}) })
	r.GET("/ready", func(c *gin.Context) {
		if err := a.store.Ping(c.Request.Context()); err != nil {
			c.JSON(503, gin.H{"error": platform.ProblemStatus(503, "auth storage unavailable")})
			return
		}
		c.JSON(200, gin.H{"status": "ready"})
	})

	internal := r.Group("/internal", a.internalOnly())
	internal.POST("/login", a.login)
	internal.POST("/refresh", a.refresh)
	internal.POST("/logout", a.logout)
	internal.POST("/introspect", a.introspect)
	return r
}

func (a *authServer) internalOnly() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !sameSecret(c.GetHeader("X-Internal-Key"), a.config.InternalKey) {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": platform.ProblemStatus(http.StatusUnauthorized, "unauthorized")})
			return
		}
		c.Next()
	}
}

func (a *authServer) login(c *gin.Context) {
	var req loginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": platform.ProblemStatus(http.StatusBadRequest, "client_id and client_secret are required")})
		return
	}
	identity, exists := a.config.Identities[req.ClientID]
	if !exists || !sameSecret(req.ClientSecret, identity.Secret) {
		c.JSON(http.StatusUnauthorized, gin.H{"error": platform.ProblemStatus(http.StatusUnauthorized, "invalid credentials")})
		return
	}
	pair, err := a.store.issue(c.Request.Context(), identity)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": platform.ProblemStatus(http.StatusServiceUnavailable, "auth storage unavailable")})
		return
	}
	c.JSON(http.StatusOK, pair)
}

func (a *authServer) refresh(c *gin.Context) {
	var req refreshRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": platform.ProblemStatus(http.StatusBadRequest, "refresh_token is required")})
		return
	}
	pair, err := a.store.rotate(c.Request.Context(), req.RefreshToken)
	if errors.Is(err, errInvalidToken) {
		c.JSON(http.StatusUnauthorized, gin.H{"error": platform.ProblemStatus(http.StatusUnauthorized, "invalid refresh token")})
		return
	}
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": platform.ProblemStatus(http.StatusServiceUnavailable, "auth storage unavailable")})
		return
	}
	c.JSON(http.StatusOK, pair)
}

func (a *authServer) logout(c *gin.Context) {
	var req refreshRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": platform.ProblemStatus(http.StatusBadRequest, "refresh_token is required")})
		return
	}
	err := a.store.logout(c.Request.Context(), req.RefreshToken)
	if errors.Is(err, errInvalidToken) {
		c.JSON(http.StatusUnauthorized, gin.H{"error": platform.ProblemStatus(http.StatusUnauthorized, "invalid refresh token")})
		return
	}
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": platform.ProblemStatus(http.StatusServiceUnavailable, "auth storage unavailable")})
		return
	}
	c.Status(http.StatusNoContent)
}

func (a *authServer) introspect(c *gin.Context) {
	var req verifyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": platform.ProblemStatus(http.StatusBadRequest, "access_token is required")})
		return
	}
	principal, err := a.store.verify(c.Request.Context(), req.AccessToken)
	if errors.Is(err, errInvalidToken) {
		c.JSON(http.StatusUnauthorized, gin.H{"error": platform.ProblemStatus(http.StatusUnauthorized, "invalid access token")})
		return
	}
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": platform.ProblemStatus(http.StatusServiceUnavailable, "auth storage unavailable")})
		return
	}
	c.JSON(http.StatusOK, principal)
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

func requestLog(log *slog.Logger, service string) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		log.Info("http request", "service", service, "correlation_id", c.GetString("correlation_id"), "trace_id", platform.TraceID(c.Request.Context()), "method", c.Request.Method, "path", c.FullPath(), "status_code", c.Writer.Status(), "latency_ms", time.Since(start).Milliseconds())
	}
}
