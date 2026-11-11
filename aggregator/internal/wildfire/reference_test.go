package wildfire

import (
	"aggregator/internal/domain"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func config() Config {
	offset, lateness := int64(1), int64(1800)
	raw := json.RawMessage(`[{"id":"fixture","name":"Fixture Province","latitude":0,"longitude":0}]`)
	return Config{Version: 1, Namespace: "fixture-satellite-session", InitialOffset: &offset, AllowedLatenessSeconds: &lateness, ProvinceJSON: raw, ProvinceSHA256: fmt.Sprintf("%x", sha256.Sum256(raw))}
}
func newModel(t *testing.T) *Model {
	t.Helper()
	m, e := New(config())
	require.NoError(t, e)
	return m
}
func detection(lon, hhmm string) Detection {
	return Detection{Satellite: "N20", AcqDate: "2026-11-12", AcqTime: hhmm, Latitude: "0", Longitude: lon, Confidence: "n", FRPMW: 10}
}
func operation(offset int64, d Detection) Operation {
	return Operation{Version: 1, Namespace: config().Namespace, Offset: offset, Kind: "detection", Detection: &d, Payload: []byte("fixture")}
}
func apply(t *testing.T, m *Model, o Operation) Result {
	t.Helper()
	r, e := m.Apply(o)
	require.NoError(t, e)
	return r
}
func active(m *Model) []Fire {
	out := []Fire{}
	for _, f := range m.Snapshot().Fires {
		if f.Active {
			out = append(out, f)
		}
	}
	return out
}
func code(t *testing.T, e error, want string) {
	t.Helper()
	var p *Error
	require.ErrorAs(t, e, &p)
	require.Equal(t, want, p.Code)
}
func key(d Detection) string {
	return strings.Join([]string{d.Satellite, d.AcqDate, d.AcqTime, d.Latitude, d.Longitude}, "|")
}
func TestConfigurationAndLexicalIdentity(t *testing.T) {
	c := config()
	_, e := New(c)
	require.NoError(t, e)
	bad := c
	bad.InitialOffset = nil
	_, e = New(bad)
	require.Error(t, e)
	bad = c
	bad.AllowedLatenessSeconds = nil
	_, e = New(bad)
	require.Error(t, e)
	bad = c
	bad.ProvinceSHA256 = "wrong"
	_, e = New(bad)
	require.Error(t, e)
	var d Detection
	require.NoError(t, Decode([]byte(`{"satellite":"雪<&>","acq_date":"2026-11-12","acq_time":243,"latitude":-0.00,"longitude":"0.0010","confidence":"h","frp_mw":10}`), &d))
	o, e := parse(d, 1, []byte{0, 255})
	require.NoError(t, e)
	require.Equal(t, "雪<&>|2026-11-12|243|-0.00|0.0010", o.Key)
	require.Equal(t, 2, o.EventTime.Hour())
	require.Equal(t, 43, o.EventTime.Minute())
	require.Equal(t, []byte{0, 255}, o.Payload)
	for _, change := range []func(*Detection){func(d *Detection) { d.AcqTime = "2460" }, func(d *Detection) { d.AcqTime = "+243" }, func(d *Detection) { d.AcqDate = "2026-02-30" }, func(d *Detection) { d.Latitude = "NaN" }, func(d *Detection) { d.Longitude = "181" }, func(d *Detection) { d.Confidence = "high" }, func(d *Detection) { d.FRPMW = math.Inf(1) }} {
		x := d
		change(&x)
		_, e = parse(x, 1, nil)
		require.Error(t, e)
	}
	require.Error(t, Decode([]byte(`{"kind":"heartbeat","kind":"detection"}`), &Operation{}))
	require.Error(t, Decode([]byte(`{"version":1,"namespace":"x","kind":"heartbeat","payload":null}`), &Operation{}))
	m := newModel(t)
	first := operation(1, d)
	apply(t, m, first)
	first.Detection.Longitude = "999"
	require.NotEqual(t, "999", m.Snapshot().Observations[0].Detection.Longitude)
	s := m.Snapshot()
	s.Observations[0].Payload[0] = 99
	require.NotEqual(t, byte(99), m.Snapshot().Observations[0].Payload[0])
}
func TestOrderedMutationAndRedelivery(t *testing.T) {
	m := newModel(t)
	a, b := operation(1, detection("0", "0000")), operation(2, detection("0.001", "0001"))
	first := apply(t, m, a)
	apply(t, m, b)
	state := m.Snapshot()
	duplicate := apply(t, m, a)
	require.Equal(t, first, duplicate)
	require.Equal(t, state, m.Snapshot())
	require.Len(t, active(m), 1)
	changed := a
	changed.Payload = []byte("different")
	_, e := m.Apply(changed)
	code(t, e, "identity_conflict")
	gap := operation(4, detection("0.002", "0002"))
	_, e = m.Apply(gap)
	code(t, e, "source_gap")
	require.Equal(t, state, m.Snapshot())
	identical := b
	identical.Offset = 3
	r := apply(t, m, identical)
	require.Nil(t, r.Batch)
	require.Equal(t, int64(1), active(m)[0].LogicalRevision)
	correction := b
	correction.Offset = 4
	correction.Kind = "update"
	correction.TargetKey = key(*b.Detection)
	replacement := *b.Detection
	replacement.FRPMW = 30
	correction.Detection = &replacement
	r = apply(t, m, correction)
	require.Equal(t, domain.SeveritySiaga, active(m)[0].Event.Severity)
	require.Len(t, r.Batch.Intents, 1)
	before := m.Snapshot()
	unknown := correction
	unknown.Offset = 5
	unknown.TargetKey = "unknown"
	_, e = m.Apply(unknown)
	code(t, e, "unknown_target")
	require.Equal(t, before, m.Snapshot())
	retract := Operation{Version: 1, Namespace: config().Namespace, Offset: 5, Kind: "retract", TargetKey: key(*b.Detection)}
	r = apply(t, m, retract)
	require.Empty(t, active(m))
	require.Equal(t, 1, r.State.Singletons)
	require.Equal(t, "retract", r.Batch.Intents[0].Operation)
	restored := b
	restored.Offset = 6
	apply(t, m, restored)
	require.Len(t, active(m), 1)
	require.Equal(t, state.Fires[0].ID, active(m)[0].ID)
	require.Equal(t, int64(4), active(m)[0].LogicalRevision)
	absent := retract
	absent.Offset = 7
	absent.TargetKey = "unknown-key"
	apply(t, m, absent)
	require.False(t, m.observations["unknown-key"].Active)
	c := config()
	start := int64(10)
	c.InitialOffset = &start
	m, e = New(c)
	require.NoError(t, e)
	_, e = m.Apply(a)
	code(t, e, "stale_redelivery")
}
func TestGraphBoundariesAndTransitivity(t *testing.T) {
	require.True(t, edge(1000, 360*time.Minute))
	require.False(t, edge(math.Nextafter(1000, math.Inf(1)), 0))
	require.True(t, edge(math.Nextafter(1000, 0), 0))
	require.False(t, edge(0, 360*time.Minute+time.Nanosecond))
	require.False(t, edge(0, time.Duration(math.MinInt64)))
	// Equatorial WGS84 distance is analytically a*pi/180 per degree, a=6378137m.
	a, _ := parse(detection("0", "0000"), 1, nil)
	b, _ := parse(detection("0.001", "0600"), 2, nil)
	require.InDelta(t, 111.31949079327357, distance(a, b), 1e-7)
	require.True(t, edge(distance(a, b), a.EventTime.Sub(*b.EventTime)))
	for _, test := range []struct {
		longitude string
		qualifies bool
	}{{"0.008983152", true}, {"0.008983154", false}} {
		m := newModel(t)
		apply(t, m, operation(1, detection("0", "0000")))
		apply(t, m, operation(2, detection(test.longitude, "0600")))
		require.Equal(t, test.qualifies, len(active(m)) == 1)
	}
	m := newModel(t)
	for i, lon := range []string{"0", "0.008", "0.016"} {
		apply(t, m, operation(int64(i+1), detection(lon, "0000")))
	}
	require.Len(t, active(m), 1)
	require.Equal(t, int64(3), active(m)[0].Event.Attributes.DetectionCount)
	low := operation(4, detection("0.004", "0000"))
	low.Detection.Confidence = "l"
	apply(t, m, low)
	require.Equal(t, 1, m.Snapshot().Excluded)
	require.Equal(t, int64(3), active(m)[0].Event.Attributes.DetectionCount)
	low.Offset = 5
	low.Kind = "update"
	low.TargetKey = key(*low.Detection)
	low.Detection.Confidence = "h"
	apply(t, m, low)
	require.Equal(t, int64(4), active(m)[0].Event.Attributes.DetectionCount)
	// A timestamp subtraction spanning centuries must not wrap into the edge budget.
	far := a
	early := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	late := time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)
	a.EventTime = &early
	far.EventTime = &late
	far.Key = "far"
	groups, singletons, _ := graph(map[string]Observation{a.Key: a, far.Key: far})
	require.Empty(t, groups)
	require.Equal(t, 2, singletons)
}
func TestMappingNumbersAndCenter(t *testing.T) {
	for _, tc := range []struct {
		count int
		frp   float64
		want  domain.Severity
	}{{2, 29.999, domain.SeverityWaspada}, {6, 0, domain.SeveritySiaga}, {2, 30, domain.SeveritySiaga}, {20, 0, domain.SeverityAwas}, {2, 100, domain.SeverityAwas}, {19, 99.999, domain.SeveritySiaga}} {
		require.Equal(t, tc.want, severity(tc.count, tc.frp))
	}
	m := newModel(t)
	apply(t, m, operation(1, detection("0", "0000")))
	apply(t, m, operation(2, detection("0.001", "0001")))
	f := active(m)[0]
	require.InDelta(t, 0.0005, f.Event.Longitude, 1e-12)
	require.Zero(t, f.Event.Latitude)
	require.Equal(t, []string{"N20"}, f.Event.Attributes.Satellites)
	var sum kahan
	for _, x := range []float64{1e16, 1, 1} {
		sum.add(x)
	}
	require.Equal(t, float64(10000000000000002), sum.sum)
	obs := map[string]Observation{}
	a, _ := parse(detection("179.999", "0000"), 1, nil)
	b, _ := parse(detection("-179.999", "0001"), 2, nil)
	obs[a.Key] = a
	obs[b.Key] = b
	lat, lon, e := center([]string{a.Key, b.Key}, obs)
	require.NoError(t, e)
	require.Zero(t, lat)
	require.InDelta(t, 180, math.Abs(lon), 1e-10)
	b.Longitude = -0.001
	obs[b.Key] = b
	_, _, e = center([]string{a.Key, b.Key}, obs)
	require.Error(t, e)
	a, _ = parse(detection("0", "0000"), 1, nil)
	b, _ = parse(detection("0", "0001"), 2, nil)
	obs = map[string]Observation{a.Key: a, b.Key: b}
	event, e := mapEvent([]string{a.Key, b.Key}, obs, []Province{{ID: "b", Name: "Z", Longitude: 1}, {ID: "c", Name: "A", Longitude: -1}, {ID: "a", Name: "A", Longitude: 1}})
	require.NoError(t, e)
	require.Equal(t, "A", event.AreaName)
}
func TestClockLateAnchorAndCoordinateUpdate(t *testing.T) {
	m := newModel(t)
	a, b := operation(1, detection("0", "0100")), operation(2, detection("0.001", "0101"))
	apply(t, m, a)
	apply(t, m, b)
	id := active(m)[0].ID
	now := time.Date(2026, 11, 12, 3, 0, 0, 0, time.UTC)
	hb := Operation{Version: 1, Namespace: config().Namespace, Offset: 3, Kind: "heartbeat", VirtualNow: &now}
	apply(t, m, hb)
	r := apply(t, m, operation(4, detection("0.002", "0000")))
	require.True(t, r.Late)
	require.Equal(t, uint64(1), r.State.LateMutations)
	require.Equal(t, id, active(m)[0].ID)
	require.Equal(t, 0, active(m)[0].Event.OccurredAt.Hour())
	old := now.Add(-time.Hour)
	hb.Offset = 5
	hb.VirtualNow = &old
	apply(t, m, hb)
	require.True(t, m.Snapshot().VirtualNow.Equal(now))
	replacement := *b.Detection
	replacement.Longitude = "1"
	r = apply(t, m, Operation{Version: 1, Namespace: config().Namespace, Offset: 6, Kind: "update", TargetKey: key(*b.Detection), Detection: &replacement})
	require.Equal(t, 1, r.State.Singletons)
	require.Equal(t, id, active(m)[0].ID)
	require.Equal(t, int64(2), active(m)[0].Event.Attributes.DetectionCount)
}
func TestGoldenBridgeAndDeterministicReplay(t *testing.T) {
	root := filepath.Join("..", "..", "..", "docs", "fixtures", "wildfire-m5")
	raw, e := os.ReadFile(filepath.Join(root, "bridge.expected.json"))
	require.NoError(t, e)
	var golden struct {
		IDs   map[string]string `json:"ids"`
		Keys  map[string]string `json:"keys"`
		Steps []struct {
			Offset     int64    `json:"offset"`
			Active     []string `json:"active"`
			Singletons int      `json:"singletons"`
		} `json:"steps"`
	}
	require.NoError(t, json.Unmarshal(raw, &golden))
	input, e := os.ReadFile(filepath.Join(root, "bridge.jsonl"))
	require.NoError(t, e)
	var outputs [][]byte
	for run := 0; run < 5; run++ {
		m := newModel(t)
		results := []Result{}
		for i, line := range strings.Split(strings.TrimSpace(string(input)), "\n") {
			var op Operation
			require.NoError(t, Decode([]byte(line), &op))
			r := apply(t, m, op)
			results = append(results, r)
			step := golden.Steps[i]
			require.Equal(t, step.Singletons, r.State.Singletons)
			require.Len(t, active(m), len(step.Active))
			switch i + 1 {
			case 2:
				require.Equal(t, golden.IDs["AB"], active(m)[0].ID)
			case 4, 6:
				ids := []string{}
				for _, f := range active(m) {
					ids = append(ids, f.ID)
				}
				require.ElementsMatch(t, []string{golden.IDs["AB"], golden.IDs["CD"]}, ids)
			case 5:
				require.Equal(t, golden.IDs["AB"], active(m)[0].ID)
				require.Len(t, r.Batch.Intents, 2)
			}
		}
		out, e := json.Marshal(results)
		require.NoError(t, e)
		outputs = append(outputs, out)
	}
	for _, o := range outputs[1:] {
		require.Equal(t, string(outputs[0]), string(o))
	}
}
func TestAtomicRejectionAndKeyCollision(t *testing.T) {
	m := newModel(t)
	a := operation(1, detection("0", "0000"))
	apply(t, m, a)
	before := m.Snapshot()
	bad := operation(2, detection("0", "0001"))
	bad.Detection.FRPMW = -1
	_, e := m.Apply(bad)
	code(t, e, "invalid_input")
	require.Equal(t, before, m.Snapshot())
	o := m.observations[key(*a.Detection)]
	o.Tuple[0] = "different derivation"
	m.observations[o.Key] = o
	a.Offset = 2
	_, e = m.Apply(a)
	code(t, e, "identity_conflict")
}

func TestPriorWatermarkAndLexicalDistinctObservations(t *testing.T) {
	m := newModel(t)
	future := time.Date(2026, 11, 12, 3, 0, 0, 0, time.UTC)
	first := operation(1, detection("0", "0230"))
	first.VirtualNow = &future
	r := apply(t, m, first)
	require.False(t, r.Late)
	second := operation(2, detection("0.0", "0230"))
	r = apply(t, m, second)
	require.False(t, r.Late)
	require.Len(t, m.Snapshot().Observations, 2)
	require.Len(t, active(m), 1)
	older := operation(3, detection("0", "0229"))
	r = apply(t, m, older)
	require.True(t, r.Late)
	require.Equal(t, uint64(1), r.State.LateMutations)
}
func TestSharedCanonicalMappingExample(t *testing.T) {
	raw, e := os.ReadFile(filepath.Join("..", "..", "..", "docs", "fixtures", "m2-contract-examples.json"))
	require.NoError(t, e)
	var fixture struct {
		Wildfire struct {
			ID         string                    `json:"source_ref_id"`
			Latitude   float64                   `json:"latitude"`
			Longitude  float64                   `json:"longitude"`
			Attributes domain.WildfireAttributes `json:"attributes"`
		} `json:"canonical_wildfire_example"`
	}
	require.NoError(t, json.Unmarshal(raw, &fixture))
	m := newModel(t)
	a, b := detection("0", "0000"), detection("0.001", "0001")
	a.Confidence = "h"
	b.Satellite = "N21"
	apply(t, m, operation(1, a))
	apply(t, m, operation(2, b))
	f := active(m)[0]
	require.Equal(t, fixture.Wildfire.ID, f.ID)
	require.Equal(t, fixture.Wildfire.Attributes, f.Event.Attributes)
	require.InDelta(t, fixture.Wildfire.Latitude, f.Event.Latitude, 1e-10)
	require.InDelta(t, fixture.Wildfire.Longitude, f.Event.Longitude, 1e-10)
}

func TestIndependentIdentityDigests(t *testing.T) {
	raw, e := os.ReadFile(filepath.Join("..", "..", "..", "docs", "fixtures", "wildfire-m5", "identity.expected.json"))
	require.NoError(t, e)
	var rows []struct {
		Tuple    []json.RawMessage `json:"tuple"`
		Expected string            `json:"expected"`
	}
	require.NoError(t, json.Unmarshal(raw, &rows))
	for _, r := range rows {
		var d Derivation
		require.NoError(t, json.Unmarshal(r.Tuple[0], &d.Namespace))
		require.NoError(t, json.Unmarshal(r.Tuple[1], &d.EarliestKey))
		require.NoError(t, json.Unmarshal(r.Tuple[2], &d.BirthOffset))
		id, e := Identity(d)
		require.NoError(t, e)
		require.Equal(t, r.Expected, id)
	}
}

func TestPublishedGeographicLibExample(t *testing.T) {
	// Independent published, rounded example, not generated by this Go port:
	// https://geographiclib.sourceforge.io/html/python/examples.html
	a := Observation{Latitude: -41.32, Longitude: 174.81}
	b := Observation{Latitude: 40.96, Longitude: -5.50}
	require.InDelta(t, 19959679.267, distance(a, b), 0.0005)
}
