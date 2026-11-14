package store

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"aggregator/internal/ownership"
	"aggregator/internal/rawinput"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/require"
)

func TestM3HardeningCompatibilityAndRollback(t *testing.T) {
	for _, citus := range []bool{false, true} {
		t.Run(fmt.Sprint(citus), func(t *testing.T) {
			dsn, c := m2Database(t, citus)
			target := "postgres"
			if citus {
				target = "citus"
			}
			m2Schema(t, c, target, false)
			s := m2Store(t, dsn)
			s.Canonical.Durability = m3LabDurability{pool: s.Pool}
			ctx := context.Background()
			p, e := ownership.NewProcessor(s.Pool, 30*time.Second)
			require.NoError(t, e)
			s.Processor = p
			pl, e := p.Acquire(ctx, canonical.AcquireOwnership{Scope: "processor:BMKG", Holder: "compatibility"})
			require.NoError(t, e)
			d, e := ownership.New(s.Pool, 30*time.Second, canonical.Deadline)
			require.NoError(t, e)
			s.Canonical.Fence = d
			dl, e := d.Acquire(ctx, canonical.AcquireOwnership{Scope: "canonical:BMKG", Holder: "delivery"})
			require.NoError(t, e)
			texts := map[int]string{}
			readers := map[string]*avrowire.SourceReader{}
			codecs := map[int]*goavro.Codec{}
			for i, name := range []string{"seismic", "tsunami"} {
				raw, e := os.ReadFile(filepath.Join("..", "..", "schemas", name+".avsc"))
				require.NoError(t, e)
				id := i + 1
				texts[id] = string(raw)
				codecs[id], e = goavro.NewCodec(string(raw))
				require.NoError(t, e)
				entity, page := "seismic-events", "SeismicHistoryPage"
				if i == 1 {
					entity, page = "tsunami-warnings", "TsunamiHistoryPage"
				}
				readers[entity], e = avrowire.NewSourceReader(string(raw), page)
				require.NoError(t, e)
				history, e := avrowire.HistorySchema(string(raw), page)
				require.NoError(t, e)
				texts[id+2] = history
				codecs[id+2], e = goavro.NewCodec(history)
				require.NoError(t, e)
			}
			var outage atomic.Bool
			registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if outage.Load() {
					w.WriteHeader(503)
					return
				}
				var id int
				_, _ = fmt.Sscanf(r.URL.Path, "/schemas/ids/%d", &id)
				if texts[id] == "" {
					w.WriteHeader(404)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]string{"schema": texts[id]})
			}))
			defer registry.Close()
			client := avrowire.NewClient(registry.URL)
			client.SetCache(SchemaCache{Pool: s.Pool})
			namespace := "compatibility-v1"
			s.Inputs.Starts = map[string]int64{InputSession("BMKG", namespace): 1}
			quake := func(id string) map[string]any {
				return map[string]any{"event_id": id, "magnitude": 6.8, "depth_km": 10.0, "epicenter_lat": -6.0, "epicenter_lon": 107.0, "region_name": "Synthetic", "occurred_at": "2026-01-01T00:00:00Z", "potential_tsunami": true}
			}
			warning := func(level string) map[string]any {
				return map[string]any{"warning_id": "warning-early", "related_event_id": "early", "threat_level": level, "affected_zones": []any{"A"}, "estimated_arrival": "2026-01-01T00:30:00Z"}
			}
			type input struct {
				entity        string
				schema        int
				value         any
				full, history bool
				sequence      int64
			}
			cases := []input{{"tsunami-warnings", 2, []any{warning("Waspada")}, false, false, 2}, {"seismic-events", 1, []any{quake("early")}, false, false, 2}, {"seismic-events", 1, []any{quake("pending")}, false, false, 3}, {"tsunami-warnings", 2, []any{}, false, false, 3}, {"tsunami-warnings", 2, []any{}, true, false, 4}, {"tsunami-warnings", 4, map[string]any{"data": []any{}, "next_cursor": int64(0), "has_more": false}, false, true, 0}, {"tsunami-warnings", 2, []any{warning("Awas")}, false, false, 5}, {"seismic-events", 3, map[string]any{"data": []any{map[string]any{"cursor": int64(1), "payload": quake("early")}}, "next_cursor": int64(1), "has_more": false}, false, true, 1}}
			ids := map[string]string{}
			times := map[string]time.Time{}
			for index, tc := range cases {
				position := int64(index + 1)
				body, e := avrowire.Encode(&avrowire.Schema{ID: tc.schema, Text: texts[tc.schema], Codec: codecs[tc.schema]}, tc.value)
				require.NoError(t, e)
				cursor := rawinput.CaptureCursor{Version: 1, Entity: tc.entity, Full: tc.full, History: tc.history, SourceSequence: tc.sequence, RequestStarted: time.Date(2026, 1, 1, 1, index, 0, 0, time.UTC)}.Encode()
				env := rawinput.Envelope{Version: 1, Source: "BMKG", Namespace: namespace, Position: &position, Operation: "update", Cursor: &cursor, Payload: body, Size: int64(len(body)), Digest: rawinput.Hash(body), ReceivedAt: "2026-01-01T02:00:00Z", ContentType: avrowire.MediaType, CorrelationID: "compatibility", OwnershipScope: "acquisition:BMKG", OwnershipEpoch: 1, OwnershipHolder: "producer"}
				env.MessageID = rawinput.ID(env)
				request := rawinput.AppendRequest{Version: 1, Ownership: pl.Ownership, Messages: []rawinput.Envelope{env}}
				if index == 0 {
					s.Inputs.testBeforeCommit = func(_ context.Context, stage string, _ pgx.Tx) error {
						if stage == "receipt" {
							return errors.New("synchronized before receipt commit")
						}
						return nil
					}
					_, e = s.AppendHistory(ctx, request)
					require.Error(t, e)
					var n int
					require.NoError(t, c.QueryRow(ctx, "SELECT count(*) FROM input_history").Scan(&n))
					require.Zero(t, n)
					s.Inputs.testBeforeCommit = nil
				}
				_, e = s.AppendHistory(ctx, request)
				require.NoError(t, e)
				receipt, e := s.AppendHistory(ctx, request)
				require.NoError(t, e)
				require.Equal(t, "duplicate", receipt.Items[0].Outcome)
				if index == 1 {
					s.Inputs.testBeforeCommit = func(_ context.Context, stage string, _ pgx.Tx) error {
						if stage == "preparation" {
							return errors.New("synchronized distributed preparation rollback")
						}
						return nil
					}
					_, e = s.PrepareBMKG(ctx, pl.Ownership, namespace, client, readers)
					require.Error(t, e)
					var prepared int64
					require.NoError(t, c.QueryRow(ctx, "SELECT prepared_through FROM input_sessions").Scan(&prepared))
					require.Equal(t, position-1, prepared)
					s.Inputs.testBeforeCommit = nil
				}
				_, e = s.PrepareBMKG(ctx, pl.Ownership, namespace, client, readers)
				require.NoError(t, e)
				if index == 1 {
					var original []byte
					require.NoError(t, c.QueryRow(ctx, `SELECT convert_to(batch::text,'UTF8') FROM input_outputs WHERE namespace=$1 AND position=$2 ORDER BY ordinal LIMIT 1`, namespace, position).Scan(&original))
					// Discard the processing Store after committed preparation. The
					// replacement receives no process-local intent or mapping state.
					replacement, err := New(ctx, dsn, s.logger)
					require.NoError(t, err)
					t.Cleanup(replacement.Close)
					replacement.Processor, replacement.Canonical, replacement.Inputs = p, s.Canonical, s.Inputs
					s = replacement
					var retained []byte
					require.NoError(t, s.Pool.QueryRow(ctx, `SELECT convert_to(batch::text,'UTF8') FROM input_outputs WHERE namespace=$1 AND position=$2 ORDER BY ordinal LIMIT 1`, namespace, position).Scan(&retained))
					require.Equal(t, original, retained, "prepared operation identities and expected revisions survive replacement")
				}
				for tries := 0; tries < 5; tries++ {
					_, e = s.ApplyInputOutput(ctx, pl.Ownership, dl.Ownership, namespace)
					require.NoError(t, e)
					var applied int64
					require.NoError(t, c.QueryRow(ctx, "SELECT applied_through FROM input_sessions").Scan(&applied))
					if applied == position {
						break
					}
					require.Less(t, tries, 4)
				}
				for _, ref := range []string{"early", "pending"} {
					v, e := s.Reconcile(ctx, domain.SourceBMKG, ref)
					if e != nil {
						continue
					}
					if prior := ids[ref]; prior != "" {
						require.Equal(t, prior, v.HazardID)
						require.True(t, times[ref].Equal(v.Event.IngestedAt.Time))
					}
					ids[ref] = v.HazardID
					times[ref] = v.Event.IngestedAt.Time
				}
				if index == 3 {
					_, e = s.Reconcile(ctx, domain.SourceBMKG, "pending")
					m2Error(t, e, "not_found")
				}
				if index == 5 {
					v, e := s.Reconcile(ctx, domain.SourceBMKG, "pending")
					require.NoError(t, e)
					require.Equal(t, domain.SeveritySiaga, v.Event.Severity)
				}
			}
			v, e := s.Reconcile(ctx, domain.SourceBMKG, "early")
			require.NoError(t, e)
			require.Equal(t, domain.SeverityAwas, v.Event.Severity)
			require.Equal(t, int64(2), v.Revision)
			require.Equal(t, "warning-early", v.Event.Attributes["warning_id"])
			var fresh time.Time
			require.NoError(t, c.QueryRow(ctx, "SELECT last_success FROM source_poll_state WHERE endpoint='BMKG'").Scan(&fresh))
			require.True(t, fresh.Equal(time.Date(2026, 1, 1, 1, 2, 0, 0, time.UTC)), "old history cannot advance source freshness")
			var received, prepared, applied int64
			require.NoError(t, c.QueryRow(ctx, "SELECT received_through,prepared_through,applied_through FROM input_sessions").Scan(&received, &prepared, &applied))
			require.Equal(t, int64(8), received)
			require.Equal(t, received, prepared)
			require.Equal(t, received, applied)
			// New client uses durable known-schema cache while the registry is down;
			// an unknown identity is refused, never decoded under an arbitrary schema.
			outage.Store(true)
			restarted := avrowire.NewClient(registry.URL)
			restarted.SetCache(SchemaCache{Pool: s.Pool})
			_, e = restarted.Lookup(ctx, 1)
			require.NoError(t, e)
			_, e = restarted.Lookup(ctx, 999)
			require.Error(t, e)
		})
	}
}
