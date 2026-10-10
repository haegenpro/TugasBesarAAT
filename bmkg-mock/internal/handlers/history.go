package handlers

import (
	"bmkg-mock/internal/avrowire"
	"bmkg-mock/internal/domain"
	"bmkg-mock/internal/middleware"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

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

	// Render archived payloads through the fixed source contract, including
	// archives written by older builds that exposed extra metadata.
	for i := range page.Data {
		var value any
		if entity == "seismic-events" {
			value = &domain.SeismicEvent{}
		} else {
			value = &domain.TsunamiWarning{}
		}
		if err := json.Unmarshal(page.Data[i].Payload, value); err != nil {
			c.JSON(503, gin.H{"error": "archive invalid"})
			return
		}
		b, err := json.Marshal(value)
		if err != nil {
			c.JSON(503, gin.H{"error": "archive invalid"})
			return
		}
		page.Data[i].Payload = b
	}

	if strings.Contains(c.GetHeader("Accept"), avrowire.MediaType) {
		if h.Registry == nil {
			c.JSON(503, gin.H{"error": "schema registry unavailable"})
			return
		}
		live, name := seismicSchema, "SeismicHistoryPage"
		if entity == "tsunami-warnings" {
			live, name = warningSchema, "WarningHistoryPage"
		}
		text, err := avrowire.HistorySchema(live, name)
		if err != nil {
			c.JSON(503, gin.H{"error": "history schema invalid"})
			return
		}
		schema, err := h.Registry.Register(avrowire.WithCorrelationID(c.Request.Context(), middleware.GetCorrelationID(c)), "bmkg."+entity+"-history-value", text)
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
