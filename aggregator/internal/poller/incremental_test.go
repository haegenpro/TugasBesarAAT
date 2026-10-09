package poller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"aggregator/internal/clients"
	"aggregator/internal/domain"
	"github.com/stretchr/testify/require"
)

// This store models the commit-before-status contract. HTTP requests still go
// through the real clients, including their response validation.
type incrementalStore struct {
	mu                            sync.Mutex
	status                        map[string]domain.EndpointStatus
	readErr, commitErr, recordErr error
	commits                       map[string]int
	warningComplete               bool
	seismic                       []clients.SeismicEvent
}

func newIncrementalStore() *incrementalStore {
	return &incrementalStore{status: make(map[string]domain.EndpointStatus), commits: make(map[string]int)}
}
func (*incrementalStore) SaveDedup(context.Context, domain.HazardEvent, string) (bool, error) {
	return true, nil
}
func (s *incrementalStore) commit(endpoint string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.commitErr != nil {
		return s.commitErr
	}
	s.commits[endpoint]++
	return nil
}
func (s *incrementalStore) CommitSeismic(_ context.Context, events []clients.SeismicEvent, _ string, _ int64) error {
	if err := s.commit("BMKG"); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seismic = append(s.seismic, events...)
	return nil
}
func (s *incrementalStore) CommitVolcanic(context.Context, []clients.VolcanicReport, []domain.HazardEvent, string, int64) error {
	return s.commit("PVMBG")
}
func (s *incrementalStore) CommitWarnings(_ context.Context, _ []clients.TsunamiWarning, _ string, _ int64, _ time.Time, complete bool) error {
	s.mu.Lock()
	s.warningComplete = complete
	s.mu.Unlock()
	return s.commit("BMKG_TSUNAMI")
}
func (*incrementalStore) HistoryCursor(context.Context, string, string) (int64, error) { return 0, nil }
func (s *incrementalStore) PollStatus(_ context.Context, endpoint string) (domain.EndpointStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status[endpoint], s.readErr
}
func (s *incrementalStore) RecordAttempt(_ context.Context, endpoint, _ string, success bool, _, _ time.Duration, started ...time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.recordErr != nil {
		return s.recordErr
	}
	p := s.status[endpoint]
	p.LastAttempt = &started[0]
	if success {
		p.LastSuccess = &started[0]
		p.State = "fresh"
	} else {
		p.State = "stale"
	}
	s.status[endpoint] = p
	return nil
}

func TestIncrementalPollingUsesIndependentPersistedLastSuccess(t *testing.T) {
	ctx := context.Background()
	st := newIncrementalStore()
	var mu sync.Mutex
	queries := map[string][]string{}
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		queries[r.URL.Path] = append(queries[r.URL.Path], r.URL.Query().Get("since"))
		mu.Unlock()
		if r.URL.Path == "/history" {
			io.WriteString(w, `{"data":[],"next_cursor":0,"has_more":false}`)
			return
		}
		io.WriteString(w, `[]`)
	}))
	defer source.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	makePoller := func() *Poller {
		return &Poller{Store: st, BMKG: clients.NewBMKGClient(source.URL, "key", logger), PVMBG: clients.NewPVMBGClient(source.URL, "token", logger)}
	}
	p := makePoller()
	p.PollBMKGOnce(ctx)
	p.PollPVMBGOnce(ctx)
	for _, endpoint := range []string{"BMKG", "BMKG_TSUNAMI", "PVMBG"} {
		require.NotNil(t, st.status[endpoint].LastSuccess, "successful empty responses advance the boundary")
	}
	// A new Poller instance reads the persisted store rather than an in-memory cursor.
	p = makePoller()
	for i, endpoint := range []string{"BMKG", "BMKG_TSUNAMI", "PVMBG"} {
		last := time.Now().UTC().Add(-time.Duration(i+1) * time.Minute)
		st.status[endpoint] = domain.EndpointStatus{LastSuccess: &last}
	}
	want := map[string]string{}
	for path, endpoint := range map[string]string{"/seismic-events": "BMKG", "/tsunami-warnings": "BMKG_TSUNAMI", "/volcanic-reports": "PVMBG"} {
		want[path] = st.status[endpoint].LastSuccess.Add(-time.Second).Format(time.RFC3339Nano)
	}
	p.PollBMKGOnce(ctx)
	p.PollPVMBGOnce(ctx)
	mu.Lock()
	for path, since := range want {
		require.Equal(t, []string{"", since}, queries[path], path)
	}
	mu.Unlock()
	require.False(t, st.warningComplete, "an incremental warning snapshot cannot establish absence")
}

func TestIncrementalPollingRetainsBoundaryOnFailures(t *testing.T) {
	for _, failure := range []string{"http", "malformed", "null", "commit", "record", "status-read", "cancelled", "timeout"} {
		t.Run(failure, func(t *testing.T) {
			st := newIncrementalStore()
			last := time.Now().UTC().Add(-time.Minute)
			st.status["PVMBG"] = domain.EndpointStatus{LastSuccess: &last}
			var calls atomic.Int32
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				switch failure {
				case "timeout":
					<-r.Context().Done()
				case "http":
					w.WriteHeader(503)
				case "malformed":
					io.WriteString(w, `[{"report_id":"missing required fields"}]`)
				case "null":
					io.WriteString(w, `null`)
				default:
					json.NewEncoder(w).Encode([]any{})
				}
			}))
			defer source.Close()
			switch failure {
			case "commit":
				st.commitErr = errors.New("DB commit failed")
			case "record":
				st.recordErr = errors.New("status commit failed")
			case "status-read":
				st.readErr = errors.New("DB read failed")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if failure == "cancelled" {
				cancel()
			}
			p := &Poller{Store: st, PVMBG: clients.NewPVMBGClient(source.URL, "token", slog.New(slog.NewTextHandler(io.Discard, nil)))}
			if failure == "timeout" {
				p.PVMBG.HTTP.Timeout = 20 * time.Millisecond
			}
			err := p.pollOnce(ctx, "PVMBG", "test", p.pollVolcanic)
			require.Error(t, err)
			require.Equal(t, last, *st.status["PVMBG"].LastSuccess)
			if failure == "status-read" {
				require.Zero(t, calls.Load(), "an unreadable boundary must not trigger a full fetch")
			}
			if failure == "http" || failure == "malformed" || failure == "null" || failure == "cancelled" || failure == "timeout" {
				require.Zero(t, st.commits["PVMBG"])
			}
		})
	}
}

func TestFailedBootstrapStillFetchesFullSnapshotOnRetry(t *testing.T) {
	st := newIncrementalStore()
	var mu sync.Mutex
	var queries []string
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		queries = append(queries, r.URL.Query().Get("since"))
		if len(queries) == 1 {
			w.WriteHeader(503)
			return
		}
		io.WriteString(w, `[]`)
	}))
	defer source.Close()
	p := &Poller{Store: st, PVMBG: clients.NewPVMBGClient(source.URL, "token", slog.New(slog.NewTextHandler(io.Discard, nil)))}
	ctx := context.Background()
	require.Error(t, p.pollOnce(ctx, "PVMBG", "test", p.pollVolcanic))
	require.Nil(t, st.status["PVMBG"].LastSuccess)
	require.NoError(t, p.pollOnce(ctx, "PVMBG", "test", p.pollVolcanic))
	require.NotNil(t, st.status["PVMBG"].LastSuccess)
	require.NoError(t, p.pollOnce(ctx, "PVMBG", "test", p.pollVolcanic))
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, queries, 3)
	require.Empty(t, queries[0])
	require.Empty(t, queries[1])
	require.NotEmpty(t, queries[2])
}

func TestSinceBoundaryAndClockCorrection(t *testing.T) {
	now := time.Date(2026, 10, 9, 12, 0, 15, 0, time.UTC)
	p := &Poller{}
	for _, tc := range []struct {
		name string
		last *time.Time
		full bool
	}{
		{"bootstrap", nil, true},
		{"zero timestamp", new(time.Time), true},
		{"recent successful poll", timePtr(now.Add(-time.Second)), false},
		{"old successful poll stays incremental", timePtr(now.Add(-time.Hour)), false},
		{"stored timestamp in future", timePtr(now.Add(time.Hour)), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, endpoint := range []string{"BMKG", "BMKG_TSUNAMI", "PVMBG"} {
				since := p.since(endpoint, tc.last, now)
				require.Equal(t, tc.full, since.IsZero())
				if !tc.full {
					require.Equal(t, tc.last.Add(-time.Second), since)
				}
			}
		})
	}
	require.True(t, p.since("history:seismic-events", timePtr(now.Add(-time.Second)), now).IsZero())
}

func timePtr(t time.Time) *time.Time { return &t }

func TestHistoryReplayRecoversRecordAbsentFromLiveWindow(t *testing.T) {
	st := newIncrementalStore()
	last := time.Now().UTC().Add(-time.Minute)
	st.status["BMKG"] = domain.EndpointStatus{LastSuccess: &last}
	old := clients.SeismicEvent{EventID: "late-record", OccurredAt: last.Add(-time.Hour), RegionName: "Late", Magnitude: 5}
	payload, err := json.Marshal(old)
	require.NoError(t, err)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/history" {
			json.NewEncoder(w).Encode(clients.HistoryPage{Data: []clients.HistoryItem{{Cursor: 1, Payload: payload}}, NextCursor: 1})
			return
		}
		io.WriteString(w, `[]`)
	}))
	defer source.Close()
	p := &Poller{Store: st, BMKG: clients.NewBMKGClient(source.URL, "key", slog.New(slog.NewTextHandler(io.Discard, nil)))}
	ctx := context.Background()
	require.NoError(t, p.pollOnce(ctx, "BMKG", "test", p.pollSeismic))
	require.Empty(t, st.seismic)
	boundary := *st.status["BMKG"].LastSuccess
	require.NoError(t, p.replay(ctx, "test", "seismic-events"))
	require.Len(t, st.seismic, 1)
	require.Equal(t, old.EventID, st.seismic[0].EventID)
	require.Equal(t, boundary, *st.status["BMKG"].LastSuccess, "history replay must not advance the live polling boundary")
}
