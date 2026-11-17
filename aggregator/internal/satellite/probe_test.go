package satellite

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProbeReusesNormalizerAndChecksInclusiveResume(t *testing.T) {
	for _, fault := range []string{"", "gap", "changed-payload", "session-change", "normalizer-reject", "truncated", "redirect", "deadline"} {
		t.Run(fault, func(t *testing.T) {
			streams, sessions := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "Bearer private-test-token", r.Header.Get("Authorization"))
				require.Equal(t, UserAgent, r.Header.Get("User-Agent"))
				if r.URL.Path == "/v1/session" {
					sessions++
					group := "practice-group"
					if fault == "session-change" && sessions > 1 {
						group = "reset-group"
					}
					fmt.Fprintf(w, `{"group":%q,"profile":"practice","speed":60,"latest_offset":50,"virtual_now":"2026-11-19T00:00:00Z"}`, group)
					return
				}
				streams++
				if fault == "redirect" {
					http.Redirect(w, r, "https://other.invalid/", http.StatusFound)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				if fault == "deadline" {
					w.(http.Flusher).Flush()
					<-r.Context().Done()
					return
				}
				from, _ := strconv.Atoi(r.URL.Query().Get("from"))
				if fault == "gap" && streams == 2 {
					from++
				}
				fmt.Fprint(w, ": keepalive\n\nevent: heartbeat\ndata: {}\n\n")
				count := 4
				if fault == "truncated" {
					count = 1
				}
				for offset := from; offset < from+count; offset++ {
					frp := "2.5"
					if fault == "changed-payload" && streams == 2 {
						frp = "7.5"
					}
					kind := "detection"
					if fault == "normalizer-reject" {
						kind = "unexpected-kind"
					}
					fmt.Fprintf(w, "id: %d\nevent: %s\ndata: {\"offset\":%d,\"detection\":{\"satellite\":\"A\",\"acq_date\":\"2026-11-19\",\"acq_time\":31,\"latitude\":-6.1,\"longitude\":106.8,\"confidence\":\"n\",\"frp\":%s}}\n\n", offset, kind, offset, frp)
				}
			}))
			defer server.Close()
			duration := 2 * time.Second
			if fault == "deadline" {
				duration = 50 * time.Millisecond
			}
			report, err := Probe(context.Background(), ProbeConfig{BaseURL: server.URL, Token: "private-test-token", Namespace: "probe-test", From: 10, Records: 4, Duration: duration, AllowLoopbackHTTP: true})
			if fault == "" {
				require.NoError(t, err)
				require.True(t, report.Passed)
				require.Len(t, report.Phases, 3)
				require.Equal(t, int64(13), report.Phases[1].From)
				require.Equal(t, int64(14), report.Phases[2].From)
				require.Equal(t, 12, report.Kinds["detection"])
				require.Equal(t, report.Phases[0].Records[3], report.Phases[1].Records[0])
			} else {
				require.Error(t, err)
				require.False(t, report.Passed)
				require.NotEmpty(t, report.Error)
			}
			require.False(t, strings.Contains(fmt.Sprintf("%+v", report), "private-test-token"))
		})
	}
}

func TestProbeRefusesUnsafeOriginAndInvalidBounds(t *testing.T) {
	for _, origin := range []string{"http://192.0.2.1", "https://user:private@feed.invalid", "https://feed.invalid/?token=private", "https://feed.invalid/v1"} {
		_, err := Probe(context.Background(), ProbeConfig{BaseURL: origin, Token: "private", Namespace: "test", Records: 4, Duration: time.Second, AllowLoopbackHTTP: true})
		require.Error(t, err)
	}
	_, err := Probe(context.Background(), ProbeConfig{BaseURL: "https://feed.invalid", Token: "private", Namespace: "test", Records: 1000000, Duration: time.Second})
	require.Error(t, err)
}
