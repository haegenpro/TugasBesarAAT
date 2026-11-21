package m2api

import (
	"aggregator/internal/domain"
	"aggregator/internal/ownership"
	"aggregator/internal/store"
	"aggregator/internal/timeutil"
	"context"
	"math"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func validCatalogEvent(event domain.HazardEvent) bool {
	return event.Source == domain.SourceBMKG && event.HazardType == domain.HazardSeismic &&
		event.SourceRefID != "" && len(event.SourceRefID) <= 1024 && event.AreaName != "" &&
		event.Severity != "" && domain.ValidSeverity(string(event.Severity)) &&
		!event.OccurredAt.IsZero() && event.Attributes != nil &&
		!math.IsNaN(event.Latitude) && !math.IsNaN(event.Longitude) &&
		math.Abs(event.Latitude) <= 90 && math.Abs(event.Longitude) <= 180
}
func registerCatalog(r *gin.Engine, s *store.Store, cfg Config) {
	authority := ownership.New(s.Pool)
	r.POST("/internal/ops/catalog", auth(cfg.AdminToken), func(c *gin.Context) {
		var request struct {
			Events []domain.HazardEvent `json:"events"`
		}
		if !strictBody(c, 4<<20, &request) {
			return
		}
		if len(request.Events) < 1 || len(request.Events) > 100 {
			problem(c, 400, "send 1..100 normalized seismic catalog events")
			return
		}
		for _, event := range request.Events {
			if !validCatalogEvent(event) {
				problem(c, 400, "invalid normalized catalog event")
				return
			}
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
		defer cancel()
		inserted := 0
		e := authority.Run(ctx, "catalog-import", func(owned context.Context) error {
			for _, event := range request.Events {
				event.HazardID = uuid.NewSHA1(uuid.NameSpaceOID, []byte("BMKG\x00"+event.SourceRefID)).String()
				event.IngestedAt = timeutil.New(time.Now().UTC())
				fresh, e := s.SaveDedup(owned, event, c.GetString("correlation_id"))
				if e != nil {
					return e
				}
				if fresh {
					inserted++
				}
			}
			return nil
		})
		if e != nil {
			c.Header("Retry-After", "1")
			problem(c, 503, "catalog import interrupted; retry the same identities")
			return
		}
		c.JSON(200, gin.H{"accepted": len(request.Events), "inserted": inserted})
	})
}
