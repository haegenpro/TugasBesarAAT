package poller

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"aggregator/internal/avrowire"
	"aggregator/internal/clients"
	"aggregator/internal/domain"
	"aggregator/internal/volcano"
	"github.com/stretchr/testify/require"
)

type blockingReaderStore struct {
	*incrementalStore
	entered chan struct{}
	finish  chan struct{}
}

func (s *blockingReaderStore) CommitVolcanic(ctx context.Context, _ []clients.VolcanicReport, _ []domain.HazardEvent, _ string, _ int64) error {
	close(s.entered)
	select {
	case <-s.finish:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type readerSwitchLookup struct{}

func (readerSwitchLookup) Get(context.Context, string) (volcano.Reference, bool, error) {
	return volcano.Reference{AreaName: "Merapi", Latitude: -7.5, Longitude: 110.4}, true, nil
}

func TestReaderToggleWaitsForLiveAndHistoryDatabaseCommits(t *testing.T) {
	for _, mode := range []string{"live", "history"} {
		t.Run(mode, func(t *testing.T) {
			v1, err := os.ReadFile("../../schemas/volcanic-v1.avsc")
			require.NoError(t, err)
			v2, err := os.ReadFile("../../schemas/volcanic-v2.avsc")
			require.NoError(t, err)
			baseline, err := avrowire.NewSourceReader(string(v1), "VolcanicHistoryPage")
			require.NoError(t, err)
			readers, err := avrowire.NewReaderSwitch(baseline, "VolcanicHistoryPage")
			require.NoError(t, err)
			require.NoError(t, readers.Observe(&avrowire.Schema{Text: string(v2)}))
			// JSON transport isolates the gate-through-commit behavior; Avro
			// discovery and projection are covered by client matrix tests.
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				record := `{"report_id":"one","volcano_id":"V001","alert_level":"Normal","eruption_count_24h":1,"ash_column_height_m":500,"reported_at":"2026-10-10T00:00:00Z"}`
				if r.URL.Path == "/history" {
					io.WriteString(w, `{"data":[{"cursor":1,"payload":`+record+`}],"next_cursor":1,"has_more":false}`)
				} else {
					io.WriteString(w, `[`+record+`]`)
				}
			}))
			defer source.Close()
			client := clients.NewPVMBGClient(source.URL, "token", slog.New(slog.NewTextHandler(io.Discard, nil)))
			client.ReaderSwitch = readers
			store := &blockingReaderStore{newIncrementalStore(), make(chan struct{}), make(chan struct{})}
			p := &Poller{PVMBG: client, Store: store, Lookup: readerSwitchLookup{}}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			committed := make(chan error, 1)
			go func() {
				if mode == "live" {
					committed <- p.pollVolcanic(ctx, "test", time.Time{})
				} else {
					committed <- p.replay(ctx, "test", "volcanic-reports")
				}
			}()
			select {
			case <-store.entered:
			case <-ctx.Done():
				t.Fatal("commit was not reached")
			}
			switched := make(chan error, 1)
			go func() { _, err := readers.Toggle(ctx); switched <- err }()
			select {
			case err := <-switched:
				t.Fatalf("toggle passed an unfinished %s commit: %v", mode, err)
			case <-time.After(30 * time.Millisecond):
			}
			close(store.finish)
			select {
			case err := <-committed:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("commit did not finish")
			}
			select {
			case err := <-switched:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("toggle did not finish")
			}
		})
	}
}
