package main

import (
	"clientapi/internal/platform"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
)

func rejectPriority(c *gin.Context, log logger, reason string) {
	platform.Add("admission_"+reason+"_rejected_total", 1)
	log.Warn("request rejected", "service", "client-api", "correlation_id", c.GetString("correlation_id"), "reason", reason)
	c.Header("Retry-After", "1")
	c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": platform.ProblemStatus(http.StatusTooManyRequests, "server busy")})
}

// reserveFieldCapacity applies only after Auth validated the principal. General
// traffic has total-reserved slots; field traffic can use any remaining slot.
// Counters are local to one API instance and never infer roles from headers.
func reserveFieldCapacity(total, reserved int, log logger) gin.HandlerFunc {
	if total < 2 || reserved < 1 || reserved >= total {
		panic("invalid field capacity reservation")
	}
	var mu sync.Mutex
	active, general := 0, 0
	return func(c *gin.Context) {
		value, exists := c.Get("principal")
		p, valid := value.(principal)
		if !exists || !valid || p.ClientID == "" {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		if p.Role != "field" && p.Role != "media" && p.Role != "internal" {
			c.AbortWithStatus(http.StatusForbidden)
			return
		}
		field := p.Role == "field"
		mu.Lock()
		if active >= total || (!field && general >= total-reserved) {
			mu.Unlock()
			rejectPriority(c, log, "role_capacity")
			return
		}
		active++
		if !field {
			general++
		}
		mu.Unlock()
		defer func() {
			mu.Lock()
			active--
			if !field {
				general--
			}
			mu.Unlock()
		}()
		c.Next()
	}
}
