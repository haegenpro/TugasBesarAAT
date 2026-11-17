package server

import (
	"aggregator/internal/api"
	"aggregator/internal/domain"
	"aggregator/internal/poller"
	"aggregator/internal/store"
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type fieldReadFixture struct {
	api.Repository
	entered chan struct{}
	release chan struct{}
}

func (f fieldReadFixture) QueryPage(ctx context.Context, _ store.Filters, _ store.PageRequest) (store.Page, error) {
	if f.entered != nil {
		f.entered <- struct{}{}
		select {
		case <-f.release:
		case <-ctx.Done():
			return store.Page{}, ctx.Err()
		}
	}
	return store.Page{Events: []domain.HazardEvent{}, Metadata: map[string]domain.RecordMetadata{}}, nil
}
func (f fieldReadFixture) PollStatuses(context.Context, []string) (map[string]domain.EndpointStatus, error) {
	return map[string]domain.EndpointStatus{}, nil
}
func TestPriorityFieldReadRoutesStayAvailableAndRejectUntrustedHeaders(t *testing.T) {
	t.Setenv("FIELD_READ_PRIORITY_ENABLED", "true")
	t.Setenv("GENERAL_READ_MAX_INFLIGHT", "1")
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	deps := &api.Dependencies{}
	deps.SetStoreReady(true)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	general := api.New(fieldReadFixture{entered: entered, release: release}, poller.NewPVMBGStatus(), deps, time.Minute, time.Minute)
	router := New(logger, general)
	field := api.New(fieldReadFixture{}, poller.NewPVMBGStatus(), deps, time.Minute, time.Minute)
	token := strings.Repeat("f", 32)
	RegisterFieldReads(router, field, token, 1)
	request := func(path, key string) int {
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set("X-Field-Read-Key", key)
		req.Header.Set("X-Role", "field")
		reply := httptest.NewRecorder()
		router.ServeHTTP(reply, req)
		return reply.Code
	}
	done := make(chan int, 1)
	go func() { done <- request("/internal/hazards", "") }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("general read did not enter")
	}
	if status := request("/internal/hazards", token); status != 429 {
		t.Fatalf("header bypassed general admission: %d", status)
	}
	if status := request("/internal/field/hazards", "spoofed"); status != 401 {
		t.Fatalf("Field route accepted unknown credential: %d", status)
	}
	if status := request("/internal/field/hazards", token); status != 200 {
		t.Fatalf("Field route blocked by general requests: %d", status)
	}
	once.Do(func() { close(release) })
	if status := <-done; status != 200 {
		t.Fatalf("general read failed after release: %d", status)
	}
}
