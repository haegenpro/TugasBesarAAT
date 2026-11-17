package server

import (
	"aggregator/internal/api"
	"aggregator/internal/platform"
	"crypto/sha256"
	"crypto/subtle"
	"github.com/gin-gonic/gin"
	"net/http"
)

func readAdmission(max int, metric string) gin.HandlerFunc {
	if max < 1 {
		panic("positive read capacity required")
	}
	slots := make(chan struct{}, max)
	return func(c *gin.Context) {
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
			c.Next()
		default:
			platform.Add(metric, 1)
			c.Header("Retry-After", "1")
			c.AbortWithStatusJSON(429, gin.H{"error": platform.ProblemStatus(429, "read capacity unavailable")})
		}
	}
}

func RegisterFieldReads(router *gin.Engine, a *api.API, token string, max int) {
	if len(token) < 32 {
		panic("dedicated FIELD_READ_TOKEN of at least 32 bytes required")
	}
	group := router.Group("/internal/field", func(c *gin.Context) {
		expected := sha256.Sum256([]byte(token))
		actual := sha256.Sum256([]byte(c.GetHeader("X-Field-Read-Key")))
		if subtle.ConstantTimeCompare(expected[:], actual[:]) != 1 {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		c.Next()
	}, readAdmission(max, "field_read_capacity_rejected_total"))
	group.GET("/hazards", a.ListHazards)
	group.GET("/hazards/:id", a.GetHazard)
	group.GET("/notices", a.ListNotices)
	group.GET("/notices/:id", a.GetNotice)
}
