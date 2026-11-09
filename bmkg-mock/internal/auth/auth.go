// Package auth implements BMKG's X-BMKG-Key header check.
package auth

import (
	"bmkg-mock/internal/platform"
	"net/http"

	"github.com/gin-gonic/gin"
)

const HeaderName = "X-BMKG-Key"

func RequireKey(expected string) gin.HandlerFunc {
	return func(c *gin.Context) {
		key := c.GetHeader(HeaderName)
		if key == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": platform.ProblemStatus(http.StatusUnauthorized, "missing "+HeaderName)})
			return
		}
		if key != expected {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": platform.ProblemStatus(http.StatusForbidden, "invalid credential")})
			return
		}
		c.Next()
	}
}
