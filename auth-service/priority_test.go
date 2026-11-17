package main

import (
	"authservice/internal/platform"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPriorityPoolSurvivesGeneralDBSaturationAndRevocation(t *testing.T) {
	t.Setenv("AUTH_PRIORITY_ENABLED", "true")
	t.Setenv("AUTH_PRIORITY_KEY", strings.Repeat("p", 32))
	t.Setenv("AUTH_DB_FIELD_RESERVED", "1")
	t.Setenv("DB_MAX_OPEN_CONNS", "2")
	store, err := openStore(testDSN(t), time.Minute, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	field, err := store.issue(ctx, identity{ID: "field", Role: "field"})
	if err != nil {
		t.Fatal(err)
	}
	media, err := store.issue(ctx, identity{ID: "media", Role: "media"})
	if err != nil {
		t.Fatal(err)
	}
	if !platform.FieldPriorityToken(field.AccessToken, "access", strings.Repeat("p", 32), time.Now()) {
		t.Fatal("Auth did not issue a verifiable scheduling hint")
	}
	held, err := store.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	app := &authServer{store: store, config: config{InternalKey: "internal", PriorityEnabled: true, PriorityKey: strings.Repeat("p", 32), MaxInFlight: 2, FieldReserved: 1}, log: slog.New(slog.NewJSONHandler(io.Discard, nil))}
	router := app.router()
	request := func(path, key, token string) int {
		body, _ := json.Marshal(map[string]string{key: token})
		req := httptest.NewRequest("POST", "/internal/"+path, bytes.NewReader(body)).WithContext(ctx)
		req.Header.Set("X-Internal-Key", "internal")
		req.Header.Set("X-Role", "field")
		reply := httptest.NewRecorder()
		router.ServeHTTP(reply, req)
		return reply.Code
	}
	done := make(chan int, 1)
	go func() { done <- request("introspect", "access_token", media.AccessToken) }()
	deadline := time.Now().Add(time.Second)
	for store.db.Stats().WaitCount == 0 {
		if time.Now().After(deadline) {
			t.Fatal("general request did not wait for occupied connection")
		}
		time.Sleep(time.Millisecond)
	}
	if status := request("introspect", "access_token", field.AccessToken); status != 200 {
		t.Fatalf("field introspection blocked by general DB pool: %d", status)
	}
	spoof := strings.Replace(media.AccessToken, ".media.", ".field.", 1)
	if status := request("introspect", "access_token", spoof); status != 429 {
		t.Fatalf("forged role hint claimed reserved admission: %d", status)
	}
	rotated, err := store.rotate(ctx, field.RefreshToken)
	if err != nil {
		t.Fatal(err)
	}
	if status := request("introspect", "access_token", field.AccessToken); status != 401 {
		t.Fatalf("rotated token was cached/accepted: %d", status)
	}
	if status := request("introspect", "access_token", rotated.AccessToken); status != 200 {
		t.Fatalf("rotated field session unavailable: %d", status)
	}
	if status := request("logout", "refresh_token", rotated.RefreshToken); status != 204 {
		t.Fatalf("field logout blocked: %d", status)
	}
	if status := request("introspect", "access_token", rotated.AccessToken); status != 401 {
		t.Fatalf("logged-out token was cached/accepted: %d", status)
	}
	held.Close()
	if status := <-done; status != 200 {
		t.Fatalf("general request did not recover: %d", status)
	}
	if store.db.Stats().MaxOpenConnections+store.fieldDB.Stats().MaxOpenConnections != 2 {
		t.Fatal("pool split increased total DB connection budget")
	}
}
