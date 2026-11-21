package api

import (
	"aggregator/internal/domain"
	"aggregator/internal/store"
	"aggregator/internal/timeutil"
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

type summaryRepository struct {
	Repository
	page    store.Page
	filters store.Filters
}

func (s *summaryRepository) QueryPage(_ context.Context, f store.Filters, _ store.PageRequest) (store.Page, error) {
	s.filters = f
	return s.page, nil
}
func (s *summaryRepository) PollStatuses(context.Context, []string) (map[string]domain.EndpointStatus, error) {
	now := time.Now()
	return map[string]domain.EndpointStatus{"BMKG": {State: "fresh", LastSuccess: &now},
		"BMKG_TSUNAMI": {State: "fresh", LastSuccess: &now}}, nil
}

func TestOwnerSummaryKeepsPageAndAssessmentWithoutRawFields(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, view := range []string{"raw", "summary"} {
		for _, partial := range []bool{false, true} {
			t.Run(view+"/partial="+map[bool]string{true: "true", false: "false"}[partial], func(t *testing.T) {
				now := timeutil.New(time.Now().UTC())
				event := domain.HazardEvent{HazardID: "event", Source: domain.SourceBMKG, SourceRefID: "private",
					HazardType: domain.HazardSeismic, Severity: domain.SeverityAwas, AreaName: "Aceh", Latitude: 1, Longitude: 2,
					OccurredAt: now, IngestedAt: now, Attributes: map[string]any{"potential_tsunami": true, "private": "secret"}}
				cursor := "cursor"
				repo := &summaryRepository{page: store.Page{Events: []domain.HazardEvent{event}, Metadata: map[string]domain.RecordMetadata{"event": {Revision: 3}},
					Partial: partial, UnavailableUnits: []string{}, NextCursor: &cursor}}
				if partial {
					repo.page.NextCursor = nil
					repo.page.UnavailableUnits = []string{"worker-2"}
				}
				deps := &Dependencies{}
				deps.SetStoreReady(true)
				a := New(repo, nil, deps, time.Minute, time.Minute)
				r := gin.New()
				r.GET("/hazards", a.ListHazards)
				w := httptest.NewRecorder()
				r.ServeHTTP(w, httptest.NewRequest("GET", "/hazards?view="+view, nil))
				if w.Code != 200 {
					t.Fatalf("status=%d body=%s", w.Code, w.Body)
				}
				var result struct {
					Data        []map[string]json.RawMessage     `json:"data"`
					Partial     bool                             `json:"partial"`
					Next        *string                          `json:"next_cursor"`
					Unavailable []string                         `json:"unavailable_units"`
					Metadata    map[string]domain.RecordMetadata `json:"metadata"`
				}
				if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				if len(result.Data) != 1 || result.Partial != partial || repo.filters.SummaryOnly != (view == "summary") {
					t.Fatalf("page changed: %#v", result)
				}
				for _, key := range []string{"source_ref_id", "latitude", "longitude", "attributes"} {
					_, present := result.Data[0][key]
					if present != (view == "raw") {
						t.Fatalf("%s presence=%v", key, present)
					}
				}
				if result.Metadata["event"].AssessmentState != "complete" || result.Metadata["event"].Revision != 3 {
					t.Fatalf("assessment changed: %#v", result.Metadata)
				}
				if partial && (result.Next != nil || len(result.Unavailable) != 1) {
					t.Fatal("partial markers lost")
				}
				if !partial && (result.Next == nil || *result.Next != cursor) {
					t.Fatal("cursor lost")
				}
			})
		}
	}
}
