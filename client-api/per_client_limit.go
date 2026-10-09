package main

import (
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
)

// limitInFlightPerClient shares one budget across all protected routes and
// sessions of an authenticated client. Counts are local to this API instance.
func limitInFlightPerClient(n int, log logger) gin.HandlerFunc {
	var mu sync.Mutex
	active := make(map[string]int)
	return func(c *gin.Context) {
		value, exists := c.Get("principal")
		p, valid := value.(principal)
		if !exists || !valid || p.ClientID == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "authenticated client required"})
			return
		}
		if n <= 0 {
			c.Next()
			return
		}

		mu.Lock()
		if active[p.ClientID] >= n {
			mu.Unlock()
			log.Warn("request rejected", "service", "client-api", "correlation_id", c.GetString("correlation_id"), "client_id", p.ClientID, "reason", "max_in_flight_per_client", "limit", n)
			c.Header("Retry-After", "1")
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "client concurrency limit reached"})
			return
		}
		active[p.ClientID]++
		mu.Unlock()

		defer func() {
			mu.Lock()
			active[p.ClientID]--
			if active[p.ClientID] == 0 {
				delete(active, p.ClientID)
			}
			mu.Unlock()
		}()
		c.Next()
	}
}
