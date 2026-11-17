package main

import (
	"authservice/internal/platform"
	"github.com/gin-gonic/gin"
	"net/http"
	"time"
)

func (a *authServer) reserve(c *gin.Context, field bool) func() {
	if a.generalSlots == nil {
		return func() {}
	}
	slots := a.generalSlots
	if field {
		slots = a.fieldSlots
	}
	select {
	case slots <- struct{}{}:
		return func() { <-slots }
	default:
		platform.Add("auth_capacity_rejected_total", 1)
		c.Header("Retry-After", "1")
		c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": platform.ProblemStatus(429, "auth capacity unavailable")})
		return nil
	}
}

func (a *authServer) fieldToken(token, kind string) bool {
	return a.config.PriorityEnabled && platform.FieldPriorityToken(token, kind, a.config.PriorityKey, time.Now())
}
