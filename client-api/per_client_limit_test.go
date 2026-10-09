package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

type limiterTestLog struct {
	mu         sync.Mutex
	rejections []map[string]any
}

func (*limiterTestLog) Info(string, ...any)  {}
func (*limiterTestLog) Error(string, ...any) {}
func (l *limiterTestLog) Warn(message string, args ...any) {
	if message != "request rejected" {
		return
	}
	entry := make(map[string]any)
	for i := 0; i+1 < len(args); i += 2 {
		entry[args[i].(string)] = args[i+1]
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rejections = append(l.rejections, entry)
}

type limiterHarness struct {
	router    *gin.Engine
	entered   chan struct{}
	release   chan struct{}
	unblock   sync.Once
	authCalls atomic.Int32
	reads     atomic.Int32
	log       limiterTestLog
}

func newLimiterHarness(t *testing.T, limit int) *limiterHarness {
	t.Helper()
	h := &limiterHarness{entered: make(chan struct{}, 1), release: make(chan struct{})}
	agg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ready" {
			io.WriteString(w, `{"status":"ready"}`)
			return
		}
		h.reads.Add(1)
		if r.Header.Get("X-Correlation-ID") == "hold-limiter" {
			h.entered <- struct{}{}
			select {
			case <-h.release:
			case <-r.Context().Done():
				return
			}
		}
		io.WriteString(w, `{"data":[],"count":0,"as_of":"2026-10-09T00:00:00Z","source_status":{}}`)
	}))
	t.Cleanup(agg.Close)
	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/introspect" {
			io.WriteString(w, `{"status":"ok"}`)
			return
		}
		h.authCalls.Add(1)
		var payload struct {
			Token string `json:"access_token"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			w.WriteHeader(400)
			return
		}
		id := "client-a"
		switch payload.Token {
		case "a-token", "a-new-token":
		case "b-token":
			id = "client-b"
		case "empty-id":
			id = ""
		default:
			w.WriteHeader(401)
			return
		}
		json.NewEncoder(w).Encode(principal{ClientID: id, Role: "field", Scope: "hazards:raw"})
	}))
	t.Cleanup(auth.Close)
	// Cleanup unblocks requests before httptest servers wait for their handlers.
	t.Cleanup(func() { h.unblock.Do(func() { close(h.release) }) })
	a := &api{cfg: config{AuthURL: auth.URL, AggregatorURL: agg.URL, AuthInternalKey: "test-internal", MaxInFlight: 128, MaxInFlightPerClient: limit}, log: &h.log, client: &http.Client{Timeout: 3 * time.Second}}
	h.router = a.router()
	return h
}

func (h *limiterHarness) request(method, path, token, cid, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Correlation-ID", cid)
	// Client-supplied identity must never change the authenticated budget.
	req.Header.Set("X-Client-ID", "spoofed-client")
	w := httptest.NewRecorder()
	h.router.ServeHTTP(w, req)
	return w
}

func (h *limiterHarness) hold(t *testing.T, path string) <-chan *httptest.ResponseRecorder {
	t.Helper()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- h.request("GET", path, "a-token", "hold-limiter", "") }()
	select {
	case <-h.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("held request did not reach Aggregator")
	}
	return done
}

func TestPerClientLimitSharesRoutesAndTokensButIsolatesIdentities(t *testing.T) {
	for _, heldPath := range []string{"/hazards", "/notices"} {
		t.Run(heldPath, func(t *testing.T) {
			h := newLimiterHarness(t, 1)
			done := h.hold(t, heldPath)
			paths := []string{"/hazards", "/hazards/" + testHazardID, "/notices", "/notices/" + testHazardID}
			results := make(chan *httptest.ResponseRecorder, 16)
			callsBefore := h.authCalls.Load()
			for i := 0; i < 16; i++ {
				path := paths[i%len(paths)] + "?client_id=another-client"
				go func() { results <- h.request("GET", path, "a-new-token", "reject-limiter", "") }()
			}
			for i := 0; i < 16; i++ {
				select {
				case w := <-results:
					if w.Code != 429 || w.Header().Get("Retry-After") != "1" || !strings.Contains(w.Body.String(), "client concurrency limit reached") {
						t.Fatalf("shared client budget not enforced: %d %s", w.Code, w.Body.String())
					}
				case <-time.After(5 * time.Second):
					t.Fatal("rejection was queued or hung")
				}
			}
			if h.authCalls.Load() != callsBefore+16 || h.reads.Load() != 1 {
				t.Fatalf("per-client rejection must authenticate but not read: auth=%d reads=%d", h.authCalls.Load(), h.reads.Load())
			}
			if w := h.request("GET", "/hazards", "b-token", "other-client", ""); w.Code != 200 {
				t.Fatalf("another client of the same role was blocked: %d", w.Code)
			}
			for _, path := range []string{"/health", "/ready"} {
				if w := h.request("GET", path, "", "health-limiter", ""); w.Code != 200 {
					t.Fatalf("%s blocked: %d", path, w.Code)
				}
			}
			for path, body := range map[string]string{
				"/auth/login":   `{"client_id":"client-a","client_secret":"test-secret"}`,
				"/auth/refresh": `{"refresh_token":"test-refresh"}`,
				"/auth/logout":  `{"refresh_token":"test-refresh"}`,
			} {
				if w := h.request("POST", path, "", "auth-limiter", body); w.Code != 200 {
					t.Fatalf("%s blocked by per-client limit: %d", path, w.Code)
				}
			}
			h.log.mu.Lock()
			entries := append([]map[string]any(nil), h.log.rejections...)
			h.log.mu.Unlock()
			if len(entries) != 16 {
				t.Fatalf("rejection log count=%d", len(entries))
			}
			for _, entry := range entries {
				if entry["client_id"] != "client-a" || entry["reason"] != "max_in_flight_per_client" || entry["limit"] != 1 || entry["correlation_id"] != "reject-limiter" {
					t.Fatalf("incomplete rejection log: %v", entry)
				}
			}
			h.unblock.Do(func() { close(h.release) })
			select {
			case w := <-done:
				if w.Code != 200 {
					t.Fatalf("held request returned %d", w.Code)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("held request did not finish")
			}
			if w := h.request("GET", "/hazards", "a-new-token", "released-limiter", ""); w.Code != 200 {
				t.Fatalf("slot was not released: %d", w.Code)
			}
		})
	}
}

func TestPerClientLimitZeroDisablesConcurrencyCap(t *testing.T) {
	h := newLimiterHarness(t, 0)
	done := h.hold(t, "/hazards")
	if w := h.request("GET", "/hazards", "a-new-token", "uncapped", ""); w.Code != 200 {
		t.Fatalf("zero limit rejected same client: %d", w.Code)
	}
	h.unblock.Do(func() { close(h.release) })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("held request did not finish")
	}
}

func TestPerClientLimitRequiresVerifiedPrincipal(t *testing.T) {
	for _, n := range []int{0, 1} {
		for name, value := range map[string]any{"missing": nil, "wrong-type": "client-a", "empty-id": principal{}} {
			t.Run(name+"/"+strconv.Itoa(n), func(t *testing.T) {
				r := gin.New()
				r.Use(func(c *gin.Context) {
					if value != nil {
						c.Set("principal", value)
					}
					c.Next()
				}, limitInFlightPerClient(n, slog.New(slog.NewJSONHandler(io.Discard, nil))))
				r.GET("/test", func(c *gin.Context) { t.Error("invalid principal reached handler") })
				w := httptest.NewRecorder()
				r.ServeHTTP(w, httptest.NewRequest("GET", "/test", nil))
				if w.Code != 401 {
					t.Fatalf("invalid principal status=%d", w.Code)
				}
			})
		}
	}
	h := newLimiterHarness(t, 1)
	for _, token := range []string{"empty-id", "unknown-token"} {
		if w := h.request("GET", "/hazards", token, "invalid", ""); w.Code != 401 {
			t.Fatalf("token %s returned %d", token, w.Code)
		}
	}
	if h.reads.Load() != 0 {
		t.Fatal("invalid identity reached Aggregator")
	}
}

func TestPerClientLimitReleasesSlotOnErrorAndRecoveredPanic(t *testing.T) {
	r := gin.New()
	r.Use(gin.RecoveryWithWriter(io.Discard), func(c *gin.Context) { c.Set("principal", principal{ClientID: "client-a"}); c.Next() }, limitInFlightPerClient(1, slog.New(slog.NewJSONHandler(io.Discard, nil))))
	r.GET("/error", func(c *gin.Context) { c.AbortWithStatus(503) })
	r.GET("/panic", func(c *gin.Context) { panic("handler failure") })
	r.GET("/ok", func(c *gin.Context) { c.Status(200) })
	for _, path := range []string{"/error", "/panic"} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		want := 503
		if path == "/panic" {
			want = 500
		}
		if w.Code != want {
			t.Fatalf("%s status=%d, want %d", path, w.Code, want)
		}
		w = httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", "/ok", nil))
		if w.Code != 200 {
			t.Fatalf("slot leaked after %s: %d", path, w.Code)
		}
	}
}
