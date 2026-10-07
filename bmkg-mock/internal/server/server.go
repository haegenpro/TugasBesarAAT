package server

import (
	"log/slog"

	"github.com/gin-gonic/gin"

	"bmkg-mock/internal/auth"
	"bmkg-mock/internal/handlers"
	"bmkg-mock/internal/middleware"
)

// New builds the Gin engine with structured logging, correlation ID
// propagation, recovery, and auth wired in. No default plain-text access log.
func New(logger *slog.Logger, apiKey string, h *handlers.Handler) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(middleware.CorrelationID())
	r.Use(middleware.JSONLogger(logger, "bmkg-mock"))

	r.GET("/health", handlers.Health)

	protected := r.Group("/")
	protected.Use(auth.RequireKey(apiKey))
	protected.GET("/seismic-events", h.SeismicEvents)
	protected.GET("/tsunami-warnings", h.TsunamiWarnings)
	protected.GET("/history", h.History)

	return r
}
