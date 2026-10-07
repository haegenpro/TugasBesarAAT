package handlers_test

import (
	"bmkg-mock/internal/handlers"
	"bmkg-mock/internal/history"
	"bmkg-mock/internal/middleware"
	"bmkg-mock/internal/seed"
	"bmkg-mock/internal/server"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestHistoryRetainsEvictedObservationsAndEnforcesSourcePolicy(t *testing.T) {
	events, warnings := seed.Build()
	h := handlers.New(events, warnings, 0)
	a, err := history.Open(filepath.Join(t.TempDir(), "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.DB.Close()
	if err = h.AttachArchive(a); err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 125; i++ {
		ev, warning := seed.NextEvent(12345, i+1, time.Now().UTC(), rng)
		h.AddEvent(ev, warning)
	}
	ts := httptest.NewServer(server.New(middleware.NewLogger(), testKey, h))
	defer ts.Close()
	req, _ := http.NewRequest("GET", ts.URL+"/history?entity=seismic-events&limit=1000", nil)
	req.Header.Set("X-BMKG-Key", testKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var page history.Page
	if err = json.NewDecoder(resp.Body).Decode(&page); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(page.Data) != 145 {
		t.Fatalf("archive lost observations: %d", len(page.Data))
	}
	req, _ = http.NewRequest("GET", ts.URL+"/seismic-events", nil)
	req.Header.Set("X-BMKG-Key", testKey)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var records []json.RawMessage
	if err = json.NewDecoder(resp.Body).Decode(&records); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(records) != 120 {
		t.Fatalf("live retention changed: %d", len(records))
	}
	if resp.Header.Get("X-Source-Sequence") == "" {
		t.Fatal("missing live snapshot watermark")
	}
	resp, err = http.Get(ts.URL + "/history?entity=seismic-events")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 && resp.StatusCode != 403 {
		t.Fatal("unauthenticated archive exposed")
	}

}
