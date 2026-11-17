package server

import (
	"aggregator/internal/platform"
	"log/slog"

	"github.com/gin-gonic/gin"

	"aggregator/internal/api"
	"aggregator/internal/middleware"
)

func New(logger *slog.Logger, a *api.API) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.HandleMethodNotAllowed = true
	r.NoRoute(func(c *gin.Context) { c.JSON(404, gin.H{"error": platform.ProblemStatus(404, "not found")}) })
	r.NoMethod(func(c *gin.Context) { c.JSON(405, gin.H{"error": platform.ProblemStatus(405, "method not allowed")}) })
	r.Use(gin.Recovery())
	r.Use(middleware.CorrelationID())
	r.Use(middleware.JSONLogger(logger, "aggregator"))

	r.GET("/health", api.Health)
	r.GET("/ready", a.Ready)
	reads := r.Group("/internal")
	if platform.Bool("FIELD_READ_PRIORITY_ENABLED", false) {
		reads.Use(readAdmission(platform.Int("GENERAL_READ_MAX_INFLIGHT", 8), "general_read_capacity_rejected_total"))
	}
	reads.GET("/hazards", a.ListHazards)
	reads.GET("/hazards/:id", a.GetHazard)
	reads.GET("/notices", a.ListNotices)
	reads.GET("/notices/:id", a.GetNotice)

	return r
}
