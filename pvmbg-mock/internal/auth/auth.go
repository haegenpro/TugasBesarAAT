// Package auth implements PVMBG's "Authorization: Bearer <token>" checks.
// Deliberately a different credential mechanism/header than BMKG's X-BMKG-Key.
package auth

import (
	"net/http"
	"pvmbg-mock/internal/platform"
	"strings"

	"github.com/gin-gonic/gin"
)

func bearerToken(c *gin.Context) (string, bool) {
	h := c.GetHeader("Authorization")
	if h == "" {
		return "", false
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(h, prefix) {
		return "", false
	}
	return strings.TrimPrefix(h, prefix), true
}

func RequireBearer(expected string) gin.HandlerFunc {
	return func(c *gin.Context) {
		token, ok := bearerToken(c)
		if !ok || token == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": platform.ProblemStatus(http.StatusUnauthorized, "missing bearer token")})
			return
		}
		if token != expected {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": platform.ProblemStatus(http.StatusForbidden, "invalid credential")})
			return
		}
		c.Next()
	}
}
