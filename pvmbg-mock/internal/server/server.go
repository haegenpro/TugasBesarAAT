package server

import (
	"log/slog"

	"github.com/gin-gonic/gin"

	"pvmbg-mock/internal/auth"
	"pvmbg-mock/internal/handlers"
	"pvmbg-mock/internal/middleware"
	"pvmbg-mock/internal/state"
)

func New(logger *slog.Logger, token, adminToken string, h *handlers.Handler, toggles *state.Toggles) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(middleware.CorrelationID())
	r.Use(middleware.JSONLogger(logger, "pvmbg-mock"))

	r.GET("/health", handlers.Health)

	data := r.Group("/")
	data.Use(auth.RequireBearer(token))
	data.GET("/volcanic-reports", h.VolcanicReports)

	admin := r.Group("/admin")
	admin.Use(auth.RequireBearer(adminToken))
	admin.POST("/schema-version", h.AdminSchemaVersion)
	admin.POST("/outage", handlers.AdminOutage(toggles))

	return r
}
