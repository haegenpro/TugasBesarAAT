package store_test

import (
	"aggregator/internal/clients"
	"aggregator/internal/domain"
	"aggregator/internal/poller"
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPreliminaryRecoveryAndCorrectionAreDurable(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	id := "lifecycle-" + uuid.NewString()
	require.NoError(t, s.RecordAttempt(ctx, "BMKG_TSUNAMI", "test", true, time.Second, time.Minute))
	ev := clients.SeismicEvent{EventID: id, Magnitude: 6.8, DepthKM: 20, EpicenterLat: -6, EpicenterLon: 106, RegionName: "Recovery test", OccurredAt: time.Now().UTC().Truncate(time.Millisecond), PotentialTsunami: true, Attributes: map[string]any{"confidence": 0.5}}
	require.NoError(t, s.CommitSeismic(ctx, []clients.SeismicEvent{ev}, "test", 101))
	notices, err := s.Notices(ctx, "open")
	require.NoError(t, err)
	var nid string
	for _, n := range notices {
		if n.SourceRefID == id {
			nid = n.NoticeID
			require.Equal(t, "pending", n.State)
		}
	}
	require.NotEmpty(t, nid)
	require.NoError(t, s.RecordAttempt(ctx, "BMKG_TSUNAMI", "test", false, time.Second, time.Minute))
	notice, history, err := s.Notice(ctx, nid)
	require.NoError(t, err)
	require.Equal(t, "assessment_unavailable", notice.State)
	require.Len(t, history, 2)
	// Empty failed checks never materialize a magnitude-only hazard.
	var count int
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM hazard_events WHERE source_ref_id=$1`, id).Scan(&count))
	require.Zero(t, count)
	// A definitive successful empty check resolves to the required magnitude mapping.
	require.NoError(t, s.CommitWarnings(ctx, nil, "test", 0, time.Now().Add(time.Second), true))
	notice, history, err = s.Notice(ctx, nid)
	require.NoError(t, err)
	require.Equal(t, "resolved", notice.State)
	require.NotEmpty(t, notice.HazardID)
	require.Len(t, history, 3)
	h, err := s.GetByID(ctx, notice.HazardID)
	require.NoError(t, err)
	require.Equal(t, domain.SeveritySiaga, h.Severity)
	require.Equal(t, 0.5, h.Attributes["confidence"])
	// A late warning corrects the stable hazard and queues an immutable full snapshot.
	warning := clients.TsunamiWarning{WarningID: "warn-" + id, RelatedEventID: id, ThreatLevel: "Awas", AffectedZones: []string{"Coast"}, EstimatedArrival: time.Now().UTC().Add(time.Hour)}
	require.NoError(t, s.CommitWarnings(ctx, []clients.TsunamiWarning{warning}, "test", 102, time.Now(), false))
	h, err = s.GetByID(ctx, notice.HazardID)
	require.NoError(t, err)
	require.Equal(t, domain.SeverityAwas, h.Severity)
	var revision int64
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT revision FROM hazard_events WHERE hazard_id=$1`, h.HazardID).Scan(&revision))
	require.EqualValues(t, 2, revision)
	var payload []byte
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT event_payload FROM report_lifecycle_events WHERE record_id=$1 AND revision=2`, h.HazardID).Scan(&payload))
	var correction domain.LifecycleEvent
	require.NoError(t, json.Unmarshal(payload, &correction))
	require.EqualValues(t, 2, correction.HazardRevision)
	require.Equal(t, h.HazardID, correction.Hazard.HazardID)
	// Repeated polls, failures, and warning omission neither regress nor republish corrections.
	require.NoError(t, s.CommitWarnings(ctx, []clients.TsunamiWarning{warning}, "test", 0, time.Now(), true))
	require.NoError(t, s.CommitWarnings(ctx, nil, "test", 0, time.Now(), true))
	require.NoError(t, s.CommitSeismic(ctx, []clients.SeismicEvent{ev}, "test", 0))
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM report_lifecycle_events WHERE record_id=$1`, h.HazardID).Scan(&count))
	require.Equal(t, 1, count)
	cursor, err := s.HistoryCursor(ctx, "BMKG", "tsunami-warnings")
	require.NoError(t, err)
	require.GreaterOrEqual(t, cursor, int64(102))
}
func TestWarningBeforeObservationAndCursorRollback(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	id := "first-warning-" + uuid.NewString()
	w := clients.TsunamiWarning{WarningID: "w-" + id, RelatedEventID: id, ThreatLevel: "Awas", EstimatedArrival: time.Now().UTC()}
	require.NoError(t, s.CommitWarnings(ctx, []clients.TsunamiWarning{w}, "test", 0, time.Now(), false))
	e := clients.SeismicEvent{EventID: id, Magnitude: 6.8, RegionName: "Coast", OccurredAt: time.Now().UTC(), PotentialTsunami: true}
	require.NoError(t, s.CommitSeismic(ctx, []clients.SeismicEvent{e}, "test", 0))
	var severity string
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT severity FROM hazard_events WHERE source_ref_id=$1`, id).Scan(&severity))
	require.Equal(t, "AWAS", severity)
	before, err := s.HistoryCursor(ctx, "PVMBG", "volcanic-reports")
	require.NoError(t, err)
	bad := sampleEvent(domain.SourcePVMBG, "bad-"+uuid.NewString())
	bad.Attributes = map[string]any{"unsupported": make(chan int)}
	err = s.CommitVolcanic(ctx, []clients.VolcanicReport{{ReportID: bad.SourceRefID}}, []domain.HazardEvent{bad}, "test", before+1000)
	require.Error(t, err)
	after, err := s.HistoryCursor(ctx, "PVMBG", "volcanic-reports")
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestObservationCommitsBeforeBlockedWarningRequest(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	id := "blocked-warning-" + uuid.NewString()
	ev := clients.SeismicEvent{EventID: id, Magnitude: 6.8, RegionName: "Coast", OccurredAt: time.Now().UTC(), PotentialTsunami: true}
	release := make(chan struct{})
	started := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/seismic-events":
			json.NewEncoder(w).Encode([]clients.SeismicEvent{ev})
		case "/tsunami-warnings":
			close(started)
			<-release
			w.WriteHeader(503)
		default:
			w.WriteHeader(503)
		}
	}))
	defer upstream.Close()
	p := &poller.Poller{BMKG: clients.NewBMKGClient(upstream.URL, "test-key", slog.New(slog.NewTextHandler(io.Discard, nil))), Store: s}
	done := make(chan struct{})
	go func() { p.PollBMKGOnce(ctx); close(done) }()
	<-started
	// Keep the warning response blocked until we observe the durable preliminary notice.
	deadline := time.Now().Add(time.Second)
	found := false
	for time.Now().Before(deadline) {
		var n int
		require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM report_notices WHERE source_ref_id=$1`, id).Scan(&n))
		if n == 1 {
			found = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(release)
	<-done
	require.True(t, found, "validated seismic observation must commit without waiting for warning completion")
	var n int
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM hazard_events WHERE source_ref_id=$1`, id).Scan(&n))
	require.Zero(t, n)
}

func TestVolcanicReferenceFailureCreatesAndResolvesGenericNotice(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	id := "unmapped-volcano-" + uuid.NewString()
	r := clients.VolcanicReport{ReportID: id, VolcanoID: "unknown-reference", AlertLevel: "Siaga", ReportedAt: time.Now().UTC()}
	require.NoError(t, s.CommitVolcanic(ctx, []clients.VolcanicReport{r}, []domain.HazardEvent{{}}, "test", 0))
	notices, err := s.Notices(ctx, "open")
	require.NoError(t, err)
	var nid string
	for _, n := range notices {
		if n.SourceRefID == id {
			nid = n.NoticeID
			require.Equal(t, domain.HazardVolcanic, n.ObservationType)
			require.Equal(t, "volcano_reference", n.PendingDependency)
		}
	}
	require.NotEmpty(t, nid)
	n, _, err := s.Notice(ctx, nid)
	require.NoError(t, err)
	h := sampleEvent(domain.SourcePVMBG, id)
	h.HazardType = domain.HazardVolcanic
	h.Severity = domain.SeveritySiaga
	require.NoError(t, s.ResolveVolcanic(ctx, r, h, "test"))
	resolved, _, err := s.Notice(ctx, nid)
	require.NoError(t, err)
	require.Equal(t, "resolved", resolved.State)
	stored, err := s.GetByID(ctx, resolved.HazardID)
	require.NoError(t, err)
	require.WithinDuration(t, n.ReportedAt, stored.IngestedAt.Time, time.Millisecond)
}

func TestSourceSnapshotWatermarkRejectsOlderReplay(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	id := "watermark-" + uuid.NewString()
	e := clients.SeismicEvent{EventID: id, Magnitude: 6.8, RegionName: "Coast", OccurredAt: time.Now().UTC(), PotentialTsunami: true, SourceSequence: 500}
	w := clients.TsunamiWarning{WarningID: "w-" + id, RelatedEventID: id, ThreatLevel: "Awas", EstimatedArrival: time.Now().UTC(), SourceSequence: 500}
	require.NoError(t, s.CommitSeismic(ctx, []clients.SeismicEvent{e}, "test", 0))
	require.NoError(t, s.CommitWarnings(ctx, []clients.TsunamiWarning{w}, "test", 0, time.Now(), false))
	w.ThreatLevel = "Waspada"
	w.SourceSequence = 0
	require.NoError(t, s.CommitWarnings(ctx, []clients.TsunamiWarning{w}, "test", 100, time.Now(), false))
	var severity string
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT severity FROM hazard_events WHERE source_ref_id=$1`, id).Scan(&severity))
	require.Equal(t, "AWAS", severity)
	rid := "volcanic-" + id
	r := clients.VolcanicReport{ReportID: rid, SourceSequence: 500}
	h := sampleEvent(domain.SourcePVMBG, rid)
	h.HazardType = domain.HazardVolcanic
	h.Severity = domain.SeverityAwas
	require.NoError(t, s.CommitVolcanic(ctx, []clients.VolcanicReport{r}, []domain.HazardEvent{h}, "test", 0))
	r.SourceSequence = 0
	h.Severity = domain.SeverityNormal
	require.NoError(t, s.CommitVolcanic(ctx, []clients.VolcanicReport{r}, []domain.HazardEvent{h}, "test", 100))
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT severity FROM hazard_events WHERE source_ref_id=$1`, rid).Scan(&severity))
	require.Equal(t, "AWAS", severity)
}

func TestLifecycleOutboxSurvivesMissingAcknowledgement(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	require.NoError(t, s.RecordAttempt(ctx, "delivery-test-"+uuid.NewString(), "test", true, time.Second, time.Minute))
	var first domain.LifecycleEvent
	found, err := s.DeliverLifecycle(ctx, func(_ context.Context, e domain.LifecycleEvent) error {
		first = e
		return errors.New("broker acknowledgement missing")
	})
	require.True(t, found)
	require.Error(t, err)
	require.NotEmpty(t, first.EventID)
	found, err = s.DeliverLifecycle(ctx, func(_ context.Context, e domain.LifecycleEvent) error {
		require.Equal(t, first.EventID, e.EventID)
		require.Equal(t, first.Revision, e.Revision)
		return nil
	})
	require.True(t, found)
	require.NoError(t, err)
	var count int
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM report_lifecycle_outbox WHERE event_id=$1`, first.EventID).Scan(&count))
	require.Zero(t, count)
}
