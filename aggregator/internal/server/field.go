package server

import (
	"aggregator/internal/api"
	"aggregator/internal/platform"
	"crypto/subtle"
	"github.com/gin-gonic/gin"
)

// Only Client API can select this lane, after live token introspection.
func RegisterFieldReads(r *gin.Engine, a *api.API, key string) {
	group := r.Group("/internal/field", func(c *gin.Context) {
		if len(key) < 32 || subtle.ConstantTimeCompare([]byte(c.GetHeader("X-Field-Read-Key")), []byte(key)) != 1 {
			c.AbortWithStatusJSON(403, gin.H{"error": platform.ProblemStatus(403, "trusted Field lane required")})
			return
		}
		c.Next()
	})
	group.GET("/hazards", a.ListHazards)
	group.GET("/hazards/:id", a.GetHazard)
	group.GET("/notices", a.ListNotices)
	group.GET("/notices/:id", a.GetNotice)
}
