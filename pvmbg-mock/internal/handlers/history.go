package handlers

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"net/http"
	"pvmbg-mock/internal/avrowire"
	"pvmbg-mock/internal/domain"
	"pvmbg-mock/internal/history"
	"pvmbg-mock/internal/middleware"
	"strconv"
	"strings"
)

func (h *Handler) AttachArchive(a *history.Archive) error {
	records := []history.Record{}
	for _, r := range h.Reports {
		records = append(records, history.Record{Entity: "volcanic-reports", ID: r.ReportID, Payload: r})
	}
	if err := a.Append(records...); err != nil {
		return err
	}
	h.Archive = a
	return nil
}
func (h *Handler) History(c *gin.Context) {
	if h.Toggles.Outage() {
		c.JSON(503, gin.H{"error": "pvmbg outage in progress"})
		return
	}
	h.randomDelay()
	if h.Toggles.Outage() {
		c.JSON(503, gin.H{"error": "pvmbg outage in progress"})
		return
	}
	entity := c.Query("entity")
	if entity != "volcanic-reports" {
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
	// Keep each history page on one schema view while a toggle is in flight.
	h.schemaMu.RLock()
	defer h.schemaMu.RUnlock()
	page, err := h.Archive.Read(entity, after, limit)
	if err != nil {
		c.JSON(503, gin.H{"error": "archive unavailable"})
		return
	}
	for i := range page.Data {
		var r domain.VolcanicReport
		if err := json.Unmarshal(page.Data[i].Payload, &r); err != nil {
			c.JSON(503, gin.H{"error": "archive invalid"})
			return
		}
		b, _ := json.Marshal(r.WireCopy(h.Toggles.ShowConfidence()))
		page.Data[i].Payload = b
	}
	if strings.Contains(c.GetHeader("Accept"), avrowire.MediaType) {
		if h.Registry == nil {
			c.JSON(503, gin.H{"error": "schema registry unavailable"})
			return
		}
		text, err := volcanicHistorySchema(h.Toggles.ShowConfidence())
		if err != nil {
			c.JSON(503, gin.H{"error": "history schema invalid"})
			return
		}
		schema, err := h.Registry.Register(avrowire.WithCorrelationID(c.Request.Context(), middleware.GetCorrelationID(c)), volcanicHistorySubject, text)
		if err != nil {
			c.JSON(503, gin.H{"error": "history schema unavailable"})
			return
		}
		body, err := avrowire.EncodeJSON(schema, page)
		if err != nil {
			c.JSON(503, gin.H{"error": "history encoding failed"})
			return
		}
		c.Data(200, avrowire.MediaType, body)
		return
	}
	c.JSON(http.StatusOK, page)
}
