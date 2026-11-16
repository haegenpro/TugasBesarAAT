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

type forwarded struct {
	method, path, submitter, role, key, token, contentType, body string
}

func fieldReportServer(t *testing.T, status int, reply string) (*httptest.Server, *[]forwarded) {
	t.Helper()
	calls := &[]forwarded{}
	ingestion := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		*calls = append(*calls, forwarded{r.Method, r.URL.Path, r.Header.Get("X-Submitter-ID"), r.Header.Get("X-Submitter-Role"), r.Header.Get("Idempotency-Key"), r.Header.Get("Authorization"), r.Header.Get("Content-Type"), string(body)})
		w.Header().Set("Content-Type", "application/json")
		if status == http.StatusServiceUnavailable {
			w.Header().Set("Retry-After", "1")
		}
		w.WriteHeader(status)
		io.WriteString(w, reply)
	}))
	t.Cleanup(ingestion.Close)
	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			AccessToken string `json:"access_token"`
		}
		json.NewDecoder(r.Body).Decode(&payload)
		role := strings.TrimSuffix(payload.AccessToken, "-token")
		json.NewEncoder(w).Encode(principal{ClientID: role + "-demo", Role: role, Scope: "hazards:raw"})
	}))
	t.Cleanup(auth.Close)
	a := &api{cfg: config{AuthURL: auth.URL, AggregatorURL: auth.URL, AuthInternalKey: "internal-secret", IngestionURL: ingestion.URL, FieldReportToken: "report-secret"}, log: slog.New(slog.NewJSONHandler(io.Discard, nil)), client: http.DefaultClient}
	server := httptest.NewServer(a.router())
	t.Cleanup(server.Close)
	return server, calls
}

func send(t *testing.T, server *httptest.Server, method, path, token, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, server.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "report-fixture-1")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestFieldReportIsForwardedForTimLapanganOnly(t *testing.T) {
	receipt := `{"report_id":"report-fixture-1","receipt_id":"00000000-0000-4000-8000-000000000101","status":"accepted","received_at":"2026-11-16T03:00:00Z"}`
	server, calls := fieldReportServer(t, http.StatusAccepted, receipt)
	body := `{"report_type":"field_observation","occurred_at":"2026-11-16T03:00:00Z","latitude":-6.9,"longitude":107.6,"description":"banjir"}`
	resp := send(t, server, http.MethodPost, "/field-reports", "field-token", body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("field report returned %d", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	if string(got) != receipt {
		t.Fatalf("receipt changed in transit: %s", got)
	}
	call := (*calls)[0]
	if call.path != "/internal/field-reports" || call.submitter != "field-demo" || call.key != "report-fixture-1" || call.token != "Bearer report-secret" || call.contentType != "application/json" || call.body != body {
		t.Fatalf("unexpected forwarded request %+v", call)
	}
	for _, token := range []string{"media-token", "internal-token"} {
		if resp := send(t, server, http.MethodPost, "/field-reports", token, body); resp.StatusCode != http.StatusForbidden {
			t.Fatalf("%s submitted a field report with %d", token, resp.StatusCode)
		}
	}
	if len(*calls) != 1 {
		t.Fatalf("forbidden submissions reached ingestion: %d", len(*calls))
	}
	if resp := send(t, server, http.MethodPost, "/field-reports", "field-token", strings.Repeat("x", maxFieldReportBytes+1)); resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized report returned %d", resp.StatusCode)
	}
}

func TestFieldReportStatusAndErrorsPassThrough(t *testing.T) {
	server, calls := fieldReportServer(t, http.StatusConflict, `{"error":{"code":"identity_conflict","message":"Idempotency-Key was already used with different content"}}`)
	resp := send(t, server, http.MethodPost, "/field-reports", "field-token", `{}`)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("conflict returned %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "identity_conflict") {
		t.Fatalf("conflict code lost: %s", body)
	}
	send(t, server, http.MethodGet, "/field-reports/00000000-0000-4000-8000-000000000101", "internal-token", "")
	last := (*calls)[len(*calls)-1]
	if last.method != http.MethodGet || last.role != "internal" || last.submitter != "internal-demo" {
		t.Fatalf("status call lost submitter scope: %+v", last)
	}
	if resp := send(t, server, http.MethodGet, "/field-reports/x", "media-token", ""); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("media read field report status with %d", resp.StatusCode)
	}
	pending, _ := fieldReportServer(t, http.StatusServiceUnavailable, `{"error":{"code":"acceptance_pending","message":"retry"}}`)
	resp = send(t, pending, http.MethodPost, "/field-reports", "field-token", `{}`)
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "1" {
		t.Fatalf("pending acceptance returned %d retry=%q", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
}
