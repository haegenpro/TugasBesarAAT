package server

import (
	"log/slog"

	"github.com/gin-gonic/gin"

	"aggregator/internal/api"
	"aggregator/internal/middleware"
)

func New(logger *slog.Logger, a *api.API) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
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
