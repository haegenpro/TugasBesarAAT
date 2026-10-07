package handlers_test

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"pvmbg-mock/internal/handlers"
	"pvmbg-mock/internal/history"
	"pvmbg-mock/internal/middleware"
	"pvmbg-mock/internal/seed"
	"pvmbg-mock/internal/server"
	"pvmbg-mock/internal/state"
	"testing"
	"time"
)

func TestHistoryRetainsEvictedObservationsAndEnforcesSourcePolicy(t *testing.T) {
	toggles := state.New()
	h := handlers.New(seed.Build(), toggles, 0, 0)
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
		h.AddReport(seed.NextReport(12345, i+1, time.Now().UTC(), rng))
	}
	ts := httptest.NewServer(server.New(middleware.NewLogger(), testToken, testAdminToken, h, toggles))
	defer ts.Close()
	req, _ := http.NewRequest("GET", ts.URL+"/history?entity=volcanic-reports&limit=1000", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
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
	req, _ = http.NewRequest("GET", ts.URL+"/volcanic-reports", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
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
	resp, err = http.Get(ts.URL + "/history?entity=volcanic-reports")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 && resp.StatusCode != 403 {
		t.Fatal("unauthenticated archive exposed")
	}
	toggles.SetOutage(true)
	req, _ = http.NewRequest("GET", ts.URL+"/history?entity=volcanic-reports", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 503 {
		t.Fatal("archive bypassed outage")
	}
}
