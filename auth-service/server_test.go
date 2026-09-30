package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestLoginUsesDistinctServerSideIdentities(t *testing.T) {
	store, err := openStore(filepath.Join(t.TempDir(), "auth.db"), time.Minute, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer store.db.Close()
	server := httptest.NewServer((&authServer{store: store, config: config{InternalKey: "internal-key", Identities: map[string]identity{
		"media-id":    {ID: "media-id", Secret: "media-secret", Role: "media"},
		"field-id":    {ID: "field-id", Secret: "field-secret", Role: "field"},
		"internal-id": {ID: "internal-id", Secret: "internal-secret", Role: "internal"},
	}}, log: slog.New(slog.NewJSONHandler(io.Discard, nil))}).router())
	defer server.Close()
	for _, test := range []struct {
		id, secret, role string
		status           int
	}{
		{"media-id", "media-secret", "media", http.StatusOK},
		{"field-id", "field-secret", "field", http.StatusOK},
		{"internal-id", "internal-secret", "internal", http.StatusOK},
		{"media-id", "field-secret", "", http.StatusUnauthorized},
		{"media-id", "bmkg-key", "", http.StatusUnauthorized},
		{"field-id", "pvmbg-token", "", http.StatusUnauthorized},
	} {
		body, _ := json.Marshal(loginRequest{ClientID: test.id, ClientSecret: test.secret})
		req, err := http.NewRequest(http.MethodPost, server.URL+"/internal/login", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Internal-Key", "internal-key")
		resp, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var result tokenPair
		json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		if resp.StatusCode != test.status || (test.status == http.StatusOK && result.Role != test.role) {
			t.Fatalf("id=%s status=%d role=%s", test.id, resp.StatusCode, result.Role)
		}
	}
}
