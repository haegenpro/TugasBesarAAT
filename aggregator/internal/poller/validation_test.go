package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"aggregator/internal/avrowire"
	"aggregator/internal/clients"
	"aggregator/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestValidationFailureCannotCommitOrAdvancePollingProgress(t *testing.T) {
	payload := `{"type":"record","name":"Report","fields":[{"name":"report_id","type":"string"},{"name":"volcano_id","type":"string"},{"name":"alert_level","type":"string"},{"name":"eruption_count_24h","type":"int"},{"name":"ash_column_height_m","type":"double"},{"name":"reported_at","type":"string"},{"name":"quality","type":"double","x-validation":{"maximum":1}}]}`
	for _, mode := range []string{"live", "history"} {
		t.Run(mode, func(t *testing.T) {
			text := `{"type":"array","items":` + payload + `}`
			if mode == "history" {
				text = `{"type":"record","name":"Page","fields":[{"name":"data","type":{"type":"array","items":{"type":"record","name":"Item","fields":[{"name":"cursor","type":"long"},{"name":"payload","type":` + payload + `}]}}},{"name":"next_cursor","type":"long"},{"name":"has_more","type":"boolean"}]}`
			}
			regServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case "PUT":
					fmt.Fprint(w, `{}`)
				case "POST":
					fmt.Fprint(w, `{"id":31}`)
				default:
					json.NewEncoder(w).Encode(map[string]any{"schema": text})
				}
			}))
			defer regServer.Close()
			registry := avrowire.NewClient(regServer.URL)
			schema, e := registry.Register(context.Background(), "test", text)
			require.NoError(t, e)
			record := map[string]any{"report_id": "bad", "volcano_id": "V001", "alert_level": "Normal", "eruption_count_24h": int32(0), "ash_column_height_m": 0.0, "reported_at": time.Now().UTC().Format(time.RFC3339Nano), "quality": 1.5}
			var native any = []any{record}
			if mode == "history" {
				native = map[string]any{"data": []any{map[string]any{"cursor": int64(1), "payload": record}}, "next_cursor": int64(1), "has_more": false}
			}
			framed, e := avrowire.Encode(schema, native)
			require.NoError(t, e)
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", avrowire.MediaType)
				w.Write(framed)
			}))
			defer source.Close()
			st := newIncrementalStore()
			last := time.Now().UTC().Add(-time.Minute)
			st.status["PVMBG"] = domain.EndpointStatus{LastSuccess: &last}
			c := clients.NewPVMBGClient(source.URL, "token", slog.New(slog.NewTextHandler(io.Discard, nil)))
			c.Registry = avrowire.NewClient(regServer.URL)
			reader, readerErr := avrowire.NewReader(text)
			require.NoError(t, readerErr)
			c.Reader = &avrowire.SourceReader{Live: reader, History: reader}
			p := &Poller{Store: st, PVMBG: c}
			if mode == "live" {
				e = p.pollOnce(context.Background(), "PVMBG", "test", p.pollVolcanic)
			} else {
				e = p.replay(context.Background(), "test", "volcanic-reports")
			}
			require.ErrorContains(t, e, "quality: maximum")
			require.Zero(t, st.commits["PVMBG"])
			require.Equal(t, last, *st.status["PVMBG"].LastSuccess)
		})
	}
}
