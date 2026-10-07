package handlers

import (
	"net/http"
	"strconv"

	"bmkg-mock/internal/history"
	"github.com/gin-gonic/gin"
)

func (h *Handler) AttachArchive(a *history.Archive) error {
	records := []history.Record{}
	for _, e := range h.Events {
		records = append(records, history.Record{Entity: "seismic-events", ID: e.EventID, Payload: e})
	}
	for _, w := range h.Warnings {
		records = append(records, history.Record{Entity: "tsunami-warnings", ID: w.WarningID, Payload: w})
	}
	if err := a.Append(records...); err != nil {
		return err
	}
	h.Archive = a
	return nil
}
func (h *Handler) History(c *gin.Context) {
	h.delay()
	entity := c.Query("entity")
	if entity != "seismic-events" && entity != "tsunami-warnings" {
		c.JSON(400, gin.H{"error": "invalid entity"})
		return
	}
	after := int64(0)
	limit := 100
	var err error
	if v := c.Query("after"); v != "" {
		after, err = strconv.ParseInt(v, 10, 64)
		if err != nil || after < 0 {
			c.JSON(400, gin.H{"error": "invalid cursor"})
			return
		}
	}
	if v := c.Query("limit"); v != "" {
		limit, err = strconv.Atoi(v)
		if err != nil || limit < 1 || limit > 1000 {
			c.JSON(400, gin.H{"error": "invalid limit"})
			return
		}
	}
	if h.Archive == nil {
		c.JSON(503, gin.H{"error": "archive unavailable"})
		return
	}
	page, err := h.Archive.Read(entity, after, limit)
	if err != nil {
		c.JSON(503, gin.H{"error": "archive unavailable"})
		return
	}

	c.JSON(http.StatusOK, page)
}
