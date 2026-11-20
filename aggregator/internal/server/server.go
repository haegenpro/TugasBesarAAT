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
	r.GET("/internal/hazards", a.ListHazards)
	r.GET("/internal/hazards/:id", a.GetHazard)
	r.GET("/internal/notices", a.ListNotices)
	r.GET("/internal/notices/:id", a.GetNotice)

	return r
}
