package clients

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"aggregator/internal/avrowire"
	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/require"
)

func TestSourceReaderPolicyMatchesLiveAndHistory(t *testing.T) {
	v1, err := os.ReadFile("../../schemas/volcanic-v1.avsc")
	require.NoError(t, err)
	v2, err := os.ReadFile("../../schemas/volcanic-v2.avsc")
	require.NoError(t, err)
	for _, reverse := range []bool{false, true} {
		texts := []string{string(v1), string(v2)}
		if reverse {
			texts[0], texts[1] = texts[1], texts[0]
		}
		for wi, wtext := range texts {
			for ri, rtext := range texts {
				t.Run(fmt.Sprintf("reverse=%t/w%d/r%d", reverse, wi+1, ri+1), func(t *testing.T) {
					writerHas := wi == 1
					readerHas := ri == 1
					if reverse {
						writerHas = !writerHas
						readerHas = !readerHas
					}
					htext, err := avrowire.HistorySchema(wtext, "VolcanicHistoryPage")
					require.NoError(t, err)
					registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						text := wtext
						if r.URL.Path == "/schemas/ids/2" {
							text = htext
						}
						json.NewEncoder(w).Encode(map[string]string{"schema": text})
					}))
					defer registry.Close()
					record := map[string]any{"report_id": "one", "volcano_id": "merapi", "alert_level": "Normal", "eruption_count_24h": int32(1), "ash_column_height_m": 500.0, "reported_at": "2026-10-10T00:00:00Z"}
					if writerHas {
						record["confidence_level"] = goavro.Union("double", 0.9)
					}
					frame := func(text string, id uint32, value any) []byte {
						codec, err := goavro.NewCodec(text)
						require.NoError(t, err)
						body, err := codec.BinaryFromNative(nil, value)
						require.NoError(t, err)
						b := make([]byte, 5)
						binary.BigEndian.PutUint32(b[1:], id)
						return append(b, body...)
					}
					live := frame(wtext, 1, []any{record})
					history := frame(htext, 2, map[string]any{"data": []any{map[string]any{"cursor": int64(1), "payload": record}}, "next_cursor": int64(1), "has_more": false})
					source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						require.Equal(t, "Bearer token", r.Header.Get("Authorization"))
						w.Header().Set("Content-Type", avrowire.MediaType)
						if r.URL.Path == "/history" {
							w.Write(history)
						} else {
							w.Write(live)
						}
					}))
					defer source.Close()
					client := NewPVMBGClient(source.URL, "token", slog.New(slog.NewTextHandler(io.Discard, nil)))
					client.Registry = avrowire.NewClient(registry.URL)
					client.Reader, err = avrowire.NewSourceReader(rtext, "VolcanicHistoryPage")
					require.NoError(t, err)
					reports, err := client.FetchVolcanicReports(context.Background(), "test", time.Time{})
					require.NoError(t, err)
					require.Len(t, reports, 1)
					page, err := client.FetchHistory(context.Background(), "test", "volcanic-reports", 0)
					require.NoError(t, err)
					require.Len(t, page.Data, 1)
					replayed, err := DecodeVolcanic(page.Data[0].Payload)
					require.NoError(t, err)
					for _, r := range []VolcanicReport{reports[0], replayed} {
						require.Equal(t, float64(500), r.AshColumnHeightM)
						if writerHas && readerHas {
							value, err := requiredNumber(r.Attributes, "confidence_level")
							require.NoError(t, err)
							require.Equal(t, 0.9, value)
						} else {
							require.NotContains(t, r.Attributes, "confidence_level")
						}
					}
				})
			}
		}
	}
}

func TestPVMBGRuntimeReaderDiscoveryAndHistory(t *testing.T) {
	v1, err := os.ReadFile("../../schemas/volcanic-v1.avsc")
	require.NoError(t, err)
	v2, err := os.ReadFile("../../schemas/volcanic-v2.avsc")
	require.NoError(t, err)
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reverse=%t", reverse), func(t *testing.T) {
			texts := []string{string(v1), string(v2)}
			if reverse {
				texts[0], texts[1] = texts[1], texts[0]
			}
			frames := [2][2][]byte{}
			schemas := map[string]string{}
			ids := [2][2]uint32{{99, 100}, {7, 8}}
			for wi, text := range texts {
				record := map[string]any{"report_id": "one", "volcano_id": "V001", "alert_level": "Normal", "eruption_count_24h": int32(1), "ash_column_height_m": 500.0, "reported_at": "2026-10-10T00:00:00Z"}
				if (wi == 1) != reverse {
					record["confidence_level"] = goavro.Union("double", 0.9)
				}
				history, err := avrowire.HistorySchema(text, "VolcanicHistoryPage")
				require.NoError(t, err)
				for hi, schemaText := range []string{text, history} {
					id := ids[wi][hi]
					schemas[fmt.Sprintf("/schemas/ids/%d", id)] = schemaText
					codec, err := goavro.NewCodec(schemaText)
					require.NoError(t, err)
					var value any = []any{record}
					if hi == 1 {
						value = map[string]any{"data": []any{map[string]any{"cursor": int64(1), "payload": record}}, "next_cursor": int64(1), "has_more": false}
					}
					body, err := codec.BinaryFromNative(nil, value)
					require.NoError(t, err)
					frame := make([]byte, 5)
					binary.BigEndian.PutUint32(frame[1:], id)
					frames[wi][hi] = append(frame, body...)
				}
			}
			var lookups atomic.Int32
			registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				lookups.Add(1)
				json.NewEncoder(w).Encode(map[string]string{"schema": schemas[r.URL.Path]})
			}))
			defer registry.Close()
			var writer atomic.Int32
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hi := 0
				if r.URL.Path == "/history" {
					hi = 1
				}
				w.Header().Set("Content-Type", avrowire.MediaType)
				w.Write(frames[writer.Load()][hi])
			}))
			defer source.Close()
			c := NewPVMBGClient(source.URL, "token", slog.New(slog.NewTextHandler(io.Discard, nil)))
			c.Registry = avrowire.NewClient(registry.URL)
			baseline, err := avrowire.NewSourceReader(texts[0], "VolcanicHistoryPage")
			require.NoError(t, err)
			c.ReaderSwitch, err = avrowire.NewReaderSwitch(baseline, "VolcanicHistoryPage")
			require.NoError(t, err)
			ctx := context.Background()
			// A history page schema is not a live reader candidate.
			writer.Store(1)
			_, err = c.FetchHistory(ctx, "test", "volcanic-reports", 0)
			require.NoError(t, err)
			_, err = c.ReaderSwitch.Toggle(ctx)
			require.ErrorIs(t, err, avrowire.ErrReaderCandidateUnavailable)
			for step := 0; step < 5; step++ {
				ri := step % 2
				for wi := 0; wi < 2; wi++ {
					writer.Store(int32(wi))
					reports, err := c.FetchVolcanicReports(ctx, "test", time.Time{})
					require.NoError(t, err)
					page, err := c.FetchHistory(ctx, "test", "volcanic-reports", 0)
					require.NoError(t, err)
					replayed, err := DecodeVolcanic(page.Data[0].Payload)
					require.NoError(t, err)
					for _, r := range []VolcanicReport{reports[0], replayed} {
						if ((wi == 1) != reverse) && ((ri == 1) != reverse) {
							value, err := requiredNumber(r.Attributes, "confidence_level")
							require.NoError(t, err)
							require.Equal(t, 0.9, value)
						} else {
							require.NotContains(t, r.Attributes, "confidence_level")
						}
					}
				}
				version, err := c.ReaderSwitch.Toggle(ctx)
				require.NoError(t, err)
				require.Equal(t, fmt.Sprintf("v%d", 2-ri), version)
			}
			require.EqualValues(t, 4, lookups.Load(), "each writer schema is fetched once, including history envelopes")
		})
	}
}
