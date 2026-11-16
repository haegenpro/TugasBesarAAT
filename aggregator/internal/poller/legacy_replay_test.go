package poller

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"aggregator/internal/clients"
	"aggregator/internal/domain"
	"aggregator/internal/volcano"
	"github.com/stretchr/testify/require"
)

type replayStore struct {
	queue     []clients.ParkedPayload
	finished  map[string]error
	committed [][]string
	commitErr error
}

func (s *replayStore) NextLegacyReplay(context.Context) (*clients.ParkedPayload, error) {
	for _, p := range s.queue {
		if _, done := s.finished[p.ID]; !done {
			return &p, nil
		}
	}
	return nil, nil
}

func (s *replayStore) FinishLegacyReplay(_ context.Context, id string, err error) error {
	s.finished[id] = err
	return nil
}

func (s *replayStore) CommitVolcanic(_ context.Context, reports []clients.VolcanicReport, _ []domain.HazardEvent, _ string, _ int64) error {
	if s.commitErr != nil {
		return s.commitErr
	}
	ids := []string{}
	for _, r := range reports {
		ids = append(ids, r.ReportID)
	}
	s.committed = append(s.committed, ids)
	return nil
}

type knownVolcano struct{}

func (knownVolcano) Get(context.Context, string) (volcano.Reference, bool, error) {
	return volcano.Reference{AreaName: "Merapi", Latitude: -7.5, Longitude: 110.4}, true, nil
}

func replayPoller() *Poller {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &Poller{PVMBG: clients.NewPVMBGClient("http://unused", "token", logger), Lookup: knownVolcano{}, Logger: logger}
}

func TestParkedVolcanicReplayRecoversInOrderAfterAdaptation(t *testing.T) {
	previous := clients.VolcanicAdapter()
	t.Cleanup(func() { _ = clients.SetVolcanicAdapter(previous) })
	require.NoError(t, clients.SetVolcanicAdapter(clients.VolcanicAdapterV2))
	st := &replayStore{finished: map[string]error{}, queue: []clients.ParkedPayload{
		{ID: "a", Entity: "/volcanic-reports", Payload: []byte(`[{"report_id":"rep-1","volcano_id":"merapi","level":"Siaga","eruption_count_24h":"2","reported_at":"2026-11-10T00:00:00Z"}]`)},
		{ID: "b", Entity: "history:volcanic-reports", Payload: []byte(`{"data":[{"cursor":4,"payload":{"report_id":"rep-2","volcano_id":"merapi","alert_level":"Awas","eruption_count_24h":1,"ash_column_height_m":10,"reported_at":"2026-11-10T00:01:00Z"}},{"cursor":5,"payload":{"report_id":"rep-1","volcano_id":"merapi","level":"Siaga","eruption_count_24h":"2","reported_at":"2026-11-10T00:00:00Z"}}],"next_cursor":5,"has_more":false}`)},
	}}
	require.NoError(t, replayPoller().replayParkedVolcanic(context.Background(), st, "cid"))
	require.Equal(t, [][]string{{"rep-1"}, {"rep-2", "rep-1"}}, st.committed)
	require.NoError(t, st.finished["a"])
	require.NoError(t, st.finished["b"])
}

func TestParkedVolcanicReplayKeepsUnreadableInputParked(t *testing.T) {
	previous := clients.VolcanicAdapter()
	t.Cleanup(func() { _ = clients.SetVolcanicAdapter(previous) })
	require.NoError(t, clients.SetVolcanicAdapter(clients.VolcanicAdapterV1))
	st := &replayStore{finished: map[string]error{}, queue: []clients.ParkedPayload{
		{ID: "renamed", Entity: "/volcanic-reports", Payload: []byte(`[{"report_id":"rep-1","volcano_id":"merapi","level":"Siaga","eruption_count_24h":2,"ash_column_height_m":1,"reported_at":"2026-11-10T00:00:00Z"}]`)},
		{ID: "cut", Entity: "/volcanic-reports", Payload: []byte(`[{"report_id"`), Truncated: true},
	}}
	p := replayPoller()
	require.NoError(t, p.replayParkedVolcanic(context.Background(), st, "cid"))
	require.ErrorIs(t, st.finished["renamed"], clients.ErrContractViolation)
	require.NoError(t, p.replayParkedVolcanic(context.Background(), st, "cid"))
	require.ErrorIs(t, st.finished["cut"], clients.ErrContractViolation)
	require.Empty(t, st.committed)
}

func TestParkedVolcanicReplayRetriesTransientCommitFailure(t *testing.T) {
	st := &replayStore{finished: map[string]error{}, commitErr: errors.New("database unavailable"), queue: []clients.ParkedPayload{
		{ID: "a", Entity: "/volcanic-reports", Payload: []byte(`[{"report_id":"rep-1","volcano_id":"merapi","alert_level":"Normal","eruption_count_24h":0,"ash_column_height_m":0,"reported_at":"2026-11-10T00:00:00Z"}]`)},
	}}
	require.Error(t, replayPoller().replayParkedVolcanic(context.Background(), st, "cid"))
	require.NotContains(t, st.finished, "a")
}
