package poller_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"aggregator/internal/clients"
	"aggregator/internal/domain"
	"aggregator/internal/poller"
	"aggregator/internal/volcano"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stderr, nil))
}

// fakeStore records SaveDedup calls and mimics the (source, source_ref_id)
// unique-constraint dedup behavior without a real Postgres.
type fakeStore struct {
	mu   sync.Mutex
	seen map[string]bool
	Puts int32
}

func newFakeStore() *fakeStore { return &fakeStore{seen: map[string]bool{}} }

func (f *fakeStore) SaveDedup(ctx context.Context, e domain.HazardEvent, correlationID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := string(e.Source) + "|" + e.SourceRefID
	if f.seen[key] {
		return false, nil
	}
	f.seen[key] = true
	atomic.AddInt32(&f.Puts, 1)
	return true, nil
}

type fakeLookup struct{}

func (fakeLookup) Get(ctx context.Context, volcanoID string) (volcano.Reference, bool, error) {
	return volcano.Reference{AreaName: "Test Volcano", Latitude: -7.5, Longitude: 110.4}, true, nil
}

func bmkgTestServer(t *testing.T, delay time.Duration) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if delay > 0 {
			time.Sleep(delay)
		}
		switch r.URL.Path {
		case "/seismic-events":
			json.NewEncoder(w).Encode([]clients.SeismicEvent{
				{EventID: "evt-1", Magnitude: 3.0, DepthKM: 10, EpicenterLat: -6.5,
					EpicenterLon: 106.5, RegionName: "Test Area", OccurredAt: time.Now(),
					PotentialTsunami: false},
			})
		case "/tsunami-warnings":
			json.NewEncoder(w).Encode([]clients.TsunamiWarning{})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func pvmbgTestServer(delay time.Duration, reports []map[string]any) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if delay > 0 {
			time.Sleep(delay)
		}
		json.NewEncoder(w).Encode(reports)
	}))
}

func oneVolcanicReport() []map[string]any {
	return []map[string]any{{
		"report_id": "rep-1", "volcano_id": "merapi", "alert_level": "Waspada",
		"eruption_count_24h": 1, "ash_column_height_m": 500.0,
		"reported_at": time.Now().UTC().Format(time.RFC3339Nano),
	}}
}

func TestDeduplication_SameRecordTwice_OneInsert(t *testing.T) {
	bmkg := bmkgTestServer(t, 0)
	defer bmkg.Close()

	fs := newFakeStore()
	p := &poller.Poller{
		BMKG:     clients.NewBMKGClient(bmkg.URL, "key", testLogger()),
		Store:    fs,
		Status:   poller.NewPVMBGStatus(),
		Interval: time.Hour,
		Logger:   testLogger(),
	}

	ctx := context.Background()
	pollBMKG := getBMKGPoll(p)
	pollBMKG(ctx)
	pollBMKG(ctx) // second cycle sees the same source record again

	assert.Equal(t, int32(1), fs.Puts, "second cycle must not create a duplicate canonical record")
}

func TestPVMBGOutage_DoesNotBlockBMKG(t *testing.T) {
	bmkg := bmkgTestServer(t, 0)
	defer bmkg.Close()
	pvmbg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer pvmbg.Close()

	fs := newFakeStore()
	status := poller.NewPVMBGStatus()
	p := &poller.Poller{
		BMKG:     clients.NewBMKGClient(bmkg.URL, "key", testLogger()),
		PVMBG:    clients.NewPVMBGClient(pvmbg.URL, "token", testLogger()),
		Store:    fs,
		Status:   status,
		Interval: time.Hour,
		Logger:   testLogger(),
	}

	ctx := context.Background()
	getPVMBGPoll(p)(ctx)
	assert.False(t, status.IsHealthy())

	getBMKGPoll(p)(ctx)
	assert.Equal(t, int32(1), fs.Puts, "BMKG processing must succeed despite PVMBG outage")
}

func TestPVMBGRenamedRequiredFieldRejectsPoll(t *testing.T) {
	reports := oneVolcanicReport()
	reports[0]["report_code"] = reports[0]["report_id"]
	delete(reports[0], "report_id")
	pvmbg := pvmbgTestServer(0, reports)
	defer pvmbg.Close()
	store := newFakeStore()
	status := poller.NewPVMBGStatus()
	p := &poller.Poller{
		PVMBG: clients.NewPVMBGClient(pvmbg.URL, "token", testLogger()),
		Store: store, Lookup: fakeLookup{}, Status: status, Logger: testLogger(),
	}
	p.PollPVMBGOnce(context.Background())
	assert.Equal(t, int32(0), store.Puts)
	assert.False(t, status.IsHealthy())
}

// TestConcurrency_PVMBGSlow_BMKGStaysFast is the acceptance test for the
// critical concurrency requirement: while PVMBG takes ~3s, BMKG-only
// processing must complete with p95 well under 300ms because the two
// pollers run independently and never share a blocking lock.
func TestConcurrency_PVMBGSlow_BMKGStaysFast(t *testing.T) {
	bmkg := bmkgTestServer(t, 0)
	defer bmkg.Close()
	pvmbg := pvmbgTestServer(3*time.Second, oneVolcanicReport())
	defer pvmbg.Close()

	fs := newFakeStore()
	p := &poller.Poller{
		BMKG:     clients.NewBMKGClient(bmkg.URL, "key", testLogger()),
		PVMBG:    clients.NewPVMBGClient(pvmbg.URL, "token", testLogger()),
		Store:    fs,
		Lookup:   fakeLookup{},
		Status:   poller.NewPVMBGStatus(),
		Interval: time.Hour,
		Logger:   testLogger(),
	}

	ctx := context.Background()

	// Start the slow PVMBG poll concurrently, exactly like the real Run()
	// loop's independent goroutines.
	go getPVMBGPoll(p)(ctx)
	time.Sleep(20 * time.Millisecond) // let the PVMBG call actually start

	const samples = 20
	durations := make([]time.Duration, samples)
	for i := 0; i < samples; i++ {
		start := time.Now()
		getBMKGPoll(p)(ctx)
		durations[i] = time.Since(start)
	}

	p95 := percentile(durations, 0.95)
	assert.Less(t, p95.Milliseconds(), int64(300),
		"BMKG-only path p95 must stay under 300ms while PVMBG is slow, got %v", p95)
}

func percentile(d []time.Duration, p float64) time.Duration {
	sorted := append([]time.Duration{}, d...)
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j] < sorted[i] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func getBMKGPoll(p *poller.Poller) func(context.Context)  { return p.PollBMKGOnce }
func getPVMBGPoll(p *poller.Poller) func(context.Context) { return p.PollPVMBGOnce }
