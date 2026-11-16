package coastal

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/canonical"
	"aggregator/internal/rawinput"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/require"
)

const observation = `{"critical":true,"observation_id":"obs-000120","observed_at":"2026-11-12T00:30:00Z","quality":"ok","sea_level_anomaly_m":1.46,"station_id":"stn-padang-01","wave_height_m":1.11}`
const stationsBody = `{"items":[{"latitude":-0.9492,"longitude":100.3543,"name":"Padang","province":"Sumatera Barat","station_id":"stn-padang-01"}]}`

func TestBreakerOpensProbesOnceAndBacksOff(t *testing.T) {
	now := time.Unix(0, 0)
	b := &Breaker{Name: "test", Threshold: 2, Cooldown: time.Second, MaxCooldown: 3 * time.Second, now: func() time.Time { return now }}
	require.NoError(t, b.Allow())
	b.Failure()
	require.Equal(t, Closed, b.State())
	b.Failure()
	require.Equal(t, Open, b.State())
	require.ErrorIs(t, b.Allow(), ErrOpen)
	now = now.Add(time.Second)
	require.NoError(t, b.Allow())
	require.Equal(t, HalfOpen, b.State())
	require.ErrorIs(t, b.Allow(), ErrOpen)
	b.Failure()
	now = now.Add(1500 * time.Millisecond)
	require.ErrorIs(t, b.Allow(), ErrOpen)
	now = now.Add(time.Second)
	require.NoError(t, b.Allow())
	b.Failure()
	now = now.Add(3 * time.Second)
	require.NoError(t, b.Allow())
	b.Success()
	require.Equal(t, Closed, b.State())
	require.NoError(t, b.Allow())
	require.NoError(t, b.Allow())
}

func TestParseEnforcesTheBasicContract(t *testing.T) {
	o, err := Parse([]byte(observation), nil, 0)
	require.NoError(t, err)
	require.Equal(t, "obs-000120", o.ID)
	require.True(t, o.Critical)
	require.Equal(t, 1.46, o.SeaLevel)
	clock := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	_, err = Parse([]byte(observation), &clock, time.Minute)
	require.ErrorContains(t, err, "later than the sensor clock")
	broken := map[string]string{
		"critical text":   `"critical":true`,
		"missing station": `"station_id":"stn-padang-01"`,
		"local time":      `"observed_at":"2026-11-12T00:30:00Z"`,
		"text wave":       `"wave_height_m":1.11`,
		"negative wave":   `"wave_height_m":1.11`,
		"blank id":        `"observation_id":"obs-000120"`,
	}
	replacement := map[string]string{
		"critical text":   `"critical":"yes"`,
		"missing station": `"station":"stn-padang-01"`,
		"local time":      `"observed_at":"2026-11-12T07:30:00+07:00"`,
		"text wave":       `"wave_height_m":"1.11"`,
		"negative wave":   `"wave_height_m":-1`,
		"blank id":        `"observation_id":""`,
	}
	for name, field := range broken {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(strings.Replace(observation, field, replacement[name], 1)), nil, 0)
			var v *Violation
			require.ErrorAs(t, err, &v)
		})
	}
	for _, raw := range []string{`[1]`, `{"a":1}{"b":2}`, "{\"observation_id\":\"\xff\"}", `{"sea_level_anomaly_m":1e999}`} {
		_, err := Parse([]byte(raw), nil, 0)
		require.Error(t, err)
	}
	id, ok := Identify([]byte(observation))
	require.True(t, ok)
	require.Equal(t, "obs-000120", id)
	_, ok = Identify([]byte(`{"observation_id":42}`))
	require.False(t, ok)
}

func TestParseRejectsTrailingClosingDelimiter(t *testing.T) {
	_, err := Parse([]byte(observation+"]"), nil, 0)
	require.Error(t, err, "the entire payload must be a single JSON object")
}

func sensor(t *testing.T, handler http.HandlerFunc) (*Client, *atomic.Int64) {
	t.Helper()
	calls := &atomic.Int64{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	b := func(name string) *Breaker {
		return &Breaker{Name: name, Threshold: 3, Cooldown: 200 * time.Millisecond, MaxCooldown: time.Second}
	}
	return &Client{BaseURL: server.URL, Token: "lab-token", HTTP: http.DefaultClient, Timeout: 200 * time.Millisecond, Limiter: &Limiter{Rate: 1000, Burst: 1000}, Stations: b("stations"), Observations: b("observations")}, calls
}

func TestClientShortCircuitsAnUnhealthySensorAndRecovers(t *testing.T) {
	var healthy atomic.Bool
	c, calls := sensor(t, func(w http.ResponseWriter, r *http.Request) {
		if !healthy.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"generated_at":"2026-11-12T00:31:00Z","items":[` + observation + `],"next_cursor":"eyJvIjoxfQ"}`))
	})
	ctx := context.Background()
	for range 20 {
		_, _ = c.FetchObservations(ctx, nil, 100)
	}
	require.Equal(t, int64(3), calls.Load())
	require.Equal(t, Open, c.Observations.State())
	started := time.Now()
	_, err := c.FetchObservations(ctx, nil, 100)
	require.ErrorIs(t, err, ErrOpen)
	require.Less(t, time.Since(started), 10*time.Millisecond)
	healthy.Store(true)
	require.Eventually(t, func() bool {
		page, err := c.FetchObservations(ctx, nil, 100)
		return err == nil && len(page.Items) == 1 && *page.NextCursor == "eyJvIjoxfQ"
	}, 2*time.Second, 20*time.Millisecond)
	require.Equal(t, Closed, c.Observations.State())
	require.Equal(t, Closed, c.Stations.State())
}

func TestClientTreatsAuthenticationAndSlownessDifferently(t *testing.T) {
	c, calls := sensor(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer right" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		time.Sleep(time.Second)
	})
	for range 5 {
		_, err := c.FetchStations(context.Background())
		require.ErrorIs(t, err, ErrUnauthorized)
	}
	require.Equal(t, Closed, c.Stations.State())
	require.Equal(t, int64(5), calls.Load())
	c.Token = "right"
	started := time.Now()
	for range 5 {
		_, err := c.FetchStations(context.Background())
		require.Error(t, err)
	}
	require.Less(t, time.Since(started), 900*time.Millisecond)
	require.Equal(t, Open, c.Stations.State())
}

func TestStationCacheServesKnownStationsDuringOutage(t *testing.T) {
	var down atomic.Bool
	c, _ := sensor(t, func(w http.ResponseWriter, r *http.Request) {
		if down.Load() {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte(stationsBody))
	})
	cache := &StationCache{Client: c, TTL: 0, MissDelay: 0}
	s, err := cache.Lookup(context.Background(), "stn-padang-01")
	require.NoError(t, err)
	require.Equal(t, "Sumatera Barat", s.Province)
	down.Store(true)
	s, err = cache.Lookup(context.Background(), "stn-padang-01")
	require.NoError(t, err)
	require.Equal(t, "Padang", s.Name)
	_, err = cache.Lookup(context.Background(), "stn-unknown")
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrUnknownStation))
	down.Store(false)
	_, err = cache.Lookup(context.Background(), "stn-unknown")
	require.ErrorIs(t, err, ErrUnknownStation)
}

func TestStatusReportsStaleHealthySensor(t *testing.T) {
	s := &Status{StaleAfter: time.Minute, Observations: &Breaker{}, Stations: &Breaker{}}
	require.Equal(t, "unavailable", s.Snapshot().State)
	clock := time.Date(2026, 11, 12, 3, 0, 0, 0, time.UTC)
	s.Attempt(nil)
	s.Page(clock, clock.Add(-10*time.Second), 4)
	require.Equal(t, "fresh", s.Snapshot().State)
	s.Page(clock.Add(2*time.Hour), time.Time{}, 0)
	snapshot := s.Snapshot()
	require.Equal(t, "stale", snapshot.State)
	require.NotNil(t, snapshot.StaleSince)
	s.Page(clock.Add(2*time.Hour), clock.Add(2*time.Hour), 1)
	require.Equal(t, "fresh", s.Snapshot().State)
	s.Attempt(errors.New("sensor returned 503"))
	require.Equal(t, "fresh", s.Snapshot().State)
}

func envelope(raw string) rawinput.Envelope {
	position := int64(120)
	ref := "obs-000120"
	trace := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	e := rawinput.Envelope{Version: 1, Source: Source, Namespace: "coastal-fixture", Ref: &ref, Operation: "create", Position: &position, ReceivedAt: "2026-11-16T03:00:00.5Z", Payload: []byte(raw), Size: int64(len(raw)), Digest: rawinput.Hash([]byte(raw)), ContentType: "application/json", CorrelationID: "fixture", Traceparent: &trace, OwnershipScope: "acquisition:SENSOR_PANTAI", OwnershipEpoch: 1, OwnershipHolder: "fixture"}
	e.MessageID = rawinput.ID(e)
	return e
}

func TestWarningMatchesTheSharedSchemaAndIsStable(t *testing.T) {
	schema, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "fixtures", "coastal-warning-v1.avsc"))
	require.NoError(t, err)
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/versions") {
			_, _ = w.Write([]byte(`{"id":9}`))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer registry.Close()
	w := Warnings{Registry: avrowire.NewClient(registry.URL), SchemaText: string(schema)}
	e := envelope(observation)
	o, err := Parse(e.Payload, nil, 0)
	require.NoError(t, err)
	var s Station
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSuffix(strings.TrimPrefix(stationsBody, `{"items":[`), `]}`)), &s))
	first, err := w.Build(context.Background(), e, o, s)
	require.NoError(t, err)
	later := e
	position := int64(9000)
	later.Position = &position
	later.MessageID = rawinput.ID(later)
	again, err := w.Build(context.Background(), later, o, s)
	require.NoError(t, err)
	require.Equal(t, first.NotificationID, again.NotificationID)
	require.True(t, strings.HasPrefix(first.NotificationID, "warning-v1:"))
	require.Equal(t, uint32(9), binary.BigEndian.Uint32(first.Payload[1:5]))
	codec, err := goavro.NewCodec(string(schema))
	require.NoError(t, err)
	native, _, err := codec.NativeFromBinary(first.Payload[5:])
	require.NoError(t, err)
	record := native.(map[string]any)
	require.Equal(t, "TIM_LAPANGAN", record["recipient"])
	require.Equal(t, "Sumatera Barat", record["province"])
	require.Equal(t, "2026-11-16T03:00:00.5Z", record["first_received_at"])
	require.Equal(t, true, record["critical"])
	corrected := strings.Replace(observation, `"wave_height_m":1.11`, `"wave_height_m":1.5`, 1)
	o2, err := Parse([]byte(corrected), nil, 0)
	require.NoError(t, err)
	changed, err := w.Build(context.Background(), envelope(corrected), o2, s)
	require.NoError(t, err)
	require.NotEqual(t, first.NotificationID, changed.NotificationID)
}

func TestMutationIsAValidCanonicalCoastalEvent(t *testing.T) {
	e := envelope(observation)
	o, err := Parse(e.Payload, nil, 0)
	require.NoError(t, err)
	s := Station{ID: "stn-padang-01", Name: "Padang", Province: "Sumatera Barat", Latitude: -0.9492, Longitude: 100.3543}
	m := Mutation(e, o, s, 0)
	require.NoError(t, canonical.Validate(canonical.Batch{ContractVersion: 1, BatchID: "coastal-batch-v1:fixture", Ownership: canonical.Ownership{Scope: "canonical:SENSOR_PANTAI", Epoch: 1, Holder: "fixture"}, Mutations: []canonical.Mutation{m}}))
	require.Equal(t, "AWAS", string(m.Event.Severity))
	require.Equal(t, "COASTAL", string(m.Event.HazardType))
	require.Equal(t, "Sumatera Barat", m.Event.AreaName)
	o.Critical = false
	require.Equal(t, "NORMAL", string(Mutation(e, o, s, 0).Event.Severity))
}
