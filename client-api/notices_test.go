package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNoticeListAndHistoryNeverLeakRawFieldsToMedia(t *testing.T) {
	id := testHazardID
	n := `{"notice_id":"` + id + `","source":"BMKG","source_ref_id":"private-event","observation_type":"SEISMIC","area_name":"Coast","occurred_at":"2026-10-07T00:00:00Z","reported_at":"2026-10-07T00:00:01Z","revision":2,"state":"resolved","pending_dependency":"","hazard_id":"` + id + `","observation":{"magnitude":7,"potential_tsunami":true,"epicenter_lat":-6}}`
	h := `{"hazard_id":"` + id + `","source":"BMKG","source_ref_id":"private-event","hazard_type":"SEISMIC","severity":"AWAS","area_name":"Coast","latitude":-6,"longitude":106,"occurred_at":"2026-10-07T00:00:00Z","ingested_at":"2026-10-07T00:00:01Z","attributes":{"threat_level":"Awas"}}`
	agg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, id) {
			io.WriteString(w, `{"data":`+n+`,"history":[{"event_id":"transition","event_type":"notice_resolved","revision":2,"published_at":"2026-10-07T00:00:02Z","notice":`+n+`,"hazard":`+h+`}],"as_of":"2026-10-07T00:00:02Z","source_status":{"BMKG":{"state":"fresh"}}}`)
		} else {
			io.WriteString(w, `{"data":[`+n+`],"count":1,"as_of":"2026-10-07T00:00:02Z","source_status":{"BMKG":{"state":"fresh"}}}`)
		}
	}))
	defer agg.Close()
	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"client_id":"media","role":"media","scope":"hazards:summary"}`)
	}))
	defer auth.Close()
	a := &api{cfg: config{AuthURL: auth.URL, AggregatorURL: agg.URL, AuthInternalKey: "test"}, log: slog.New(slog.NewTextHandler(io.Discard, nil)), client: http.DefaultClient}
	downstream := httptest.NewServer(a.router())
	defer downstream.Close()
	for _, path := range []string{"/notices", "/notices/" + id, "/notices?view=raw", "/notices/" + id + "?view=raw"} {
		req, _ := http.NewRequest("GET", downstream.URL+path, nil)
		req.Header.Set("Authorization", "Bearer media-token")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if strings.Contains(path, "raw") {
			if resp.StatusCode != 403 {
				t.Fatalf("raw access returned %d", resp.StatusCode)
			}
			continue
		}
		if resp.StatusCode != 200 {
			t.Fatalf("%s: %d %s", path, resp.StatusCode, b)
		}
		var decoded any
		if err = json.Unmarshal(b, &decoded); err != nil {
			t.Fatal(err)
		}
		var check func(any)
		check = func(v any) {
			switch o := v.(type) {
			case map[string]any:
				for key, item := range o {
					switch key {
					case "source_ref_id", "observation", "attributes", "latitude", "longitude", "magnitude", "potential_tsunami":
						t.Fatalf("Media leak: %s", key)
					}
					check(item)
				}
			case []any:
				for _, item := range o {
					check(item)
				}
			}
		}
		check(decoded)
	}
}
