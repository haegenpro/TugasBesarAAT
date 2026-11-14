package wildfireprocessor

import (
	"aggregator/internal/domain"
	"aggregator/internal/migrations"
	"aggregator/internal/store"
	"aggregator/internal/wildfire"
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// The abstract agreement fixtures seed the retained lineage boundary directly.
// They do not pretend labels A/B are actual feed keys or fabricate a live graph.
func TestM6SharedHistoricalRestorationFixtures(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "fixtures", "m2-contract-examples.json"))
	require.NoError(t, err)
	type historical struct {
		Identity   string   `json:"identity"`
		Membership []string `json:"membership"`
		At         int64    `json:"deactivated_position"`
		Birth      int64    `json:"birth_offset"`
	}
	var data struct {
		Cases []struct {
			Name   string `json:"name"`
			Before struct {
				Parent     string       `json:"active_parent"`
				Membership []string     `json:"membership"`
				History    []historical `json:"inactive_history"`
			} `json:"before"`
			Children [][]string     `json:"split_children"`
			History  []historical   `json:"inactive_history"`
			Restored []string       `json:"restored_membership"`
			Expected map[string]any `json:"expected"`
			Identity string         `json:"expected_identity"`
		} `json:"wildfire_lifecycle_cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &data))
	for _, tc := range data.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			if tc.Name == "bridge_merge_then_retract" {
				l := databaseLab(t, false)
				m := oracle(t, l.e.Config)
				for _, op := range fixtureOperations(t, "bridge") {
					applyBoth(t, l, m, op)
				}
				return
			}
			l := databaseLab(t, false)
			c := l.e.Config
			start := int64(20)
			c.InitialOffset = &start
			l.s.Inputs.Starts[store.InputSession("SATELIT", c.Namespace)] = 20
			l.e, err = New(l.s, c)
			require.NoError(t, err)
			l.append(t, wildfire.Operation{Version: 1, Namespace: c.Namespace, Offset: 20, Kind: "heartbeat"})
			_, err = l.e.Step(context.Background(), l.p)
			require.NoError(t, err)
			children := tc.Children
			if len(tc.Restored) > 0 {
				children = append(children, tc.Restored)
			}
			members := []string{}
			observations := map[string]wildfire.Observation{}
			for _, group := range children {
				for _, key := range group {
					detection := wildfire.Detection{Satellite: "N20", AcqDate: "2026-11-12", AcqTime: "0000", Latitude: "0", Longitude: "0", Confidence: "n", FRPMW: 10}
					o, err := wildfire.ParseDetection(detection, 19, nil)
					require.NoError(t, err)
					at := time.Date(2026, 11, 12, 0, int(key[0]-'A'), 0, 0, time.UTC)
					o.EventTime = &at
					o.Key = key
					observations[key] = o
					members = append(members, key)
				}
			}
			parent := tc.Before.Parent
			if parent == "" && tc.Name == "singleton_does_not_keep_active_identity" {
				parent = "current-parent"
			}
			parentMembers := tc.Before.Membership
			if len(parentMembers) == 0 {
				parentMembers = append([]string{}, members...)
			}
			names := map[string]string{}
			old := map[string]wildfire.Fire{}
			seed := func(name string, keys []string, active bool, at, birth int64) {
				d := wildfire.Derivation{Namespace: c.Namespace, EarliestKey: "abstract-fixture:" + name, BirthOffset: birth}
				id, err := wildfire.Identity(d)
				require.NoError(t, err)
				names[name] = id
				sort.Strings(keys)
				f := wildfire.Fire{Source: domain.SourceSatelit, ID: id, Derivation: d, Members: keys, Active: active, LogicalRevision: 2, Related: []string{}}
				if !active {
					f.DeactivatedAt = &at
				}
				old[id] = f
			}
			if parent != "" {
				seed(parent, parentMembers, true, 0, 1)
			}
			for _, h := range append(tc.Before.History, tc.History...) {
				seed(h.Identity, h.Membership, false, h.At, h.Birth)
			}
			for id, f := range old {
				if parent != "" && id != names[parent] {
					f.Related = []string{names[parent]}
					p := old[names[parent]]
					p.Related = append(p.Related, id)
					sort.Strings(p.Related)
					old[p.ID] = p
				}
				old[id] = f
			}
			tx, err := l.e.begin(context.Background(), l.p)
			require.NoError(t, err)
			previous, current := job{Position: 19}, job{Position: 20}
			for key, o := range observations {
				require.NoError(t, l.e.put(context.Background(), tx, &previous, "observation", key, o, o.EventTime, true, ""))
			}
			for id, f := range old {
				require.NoError(t, l.e.put(context.Background(), tx, &previous, "fire", id, f, nil, f.Active, ""))
			}
			for _, group := range children {
				for _, key := range group {
					require.NoError(t, l.e.enqueue(context.Background(), tx, &current, key, group[0]))
				}
			}
			_, err = tx.Exec(context.Background(), `UPDATE wildfire_work SET done=true WHERE namespace=$1 AND position=20`, c.Namespace)
			require.NoError(t, err)
			_, err = tx.Exec(context.Background(), `UPDATE wildfire_jobs SET phase='resolve',staged_bytes=$2 WHERE namespace=$1 AND position=20`, c.Namespace, current.Bytes)
			require.NoError(t, err)
			require.NoError(t, tx.Commit(context.Background()))
			l.prepare(t, 20)
			l.deliver(t, 20)
			got := map[string]wildfire.Fire{}
			for _, f := range l.fires(t) {
				got[f.ID] = f
			}
			if tc.Identity != "" {
				require.True(t, got[names[tc.Identity]].Active)
			}
			for key, want := range tc.Expected {
				switch key {
				case "inactive":
					for _, name := range want.([]any) {
						require.False(t, got[names[name.(string)]].Active)
					}
				case "extra_active_parent", "singletons", "active_fire_count":
					continue
				default:
					if label, ok := want.(string); ok {
						require.True(t, got[names[label]].Active)
						require.Equal(t, strings.Split(key, ""), got[names[label]].Members)
					}
				}
			}
		})
	}
}

func TestM6PopulatedHistoryUpgradePreservesPendingWork(t *testing.T) {
	for _, citus := range []bool{false, true} {
		t.Run(map[bool]string{false: "postgres", true: "citus"}[citus], func(t *testing.T) {
			l := databaseLab(t, citus)
			ctx := context.Background()
			op := fixtureOperations(t, "bridge")[0]
			l.append(t, op)
			// Recreate the recognized previous journal boundary in this unique lab DB;
			// no application or unrelated database is accessed.
			_, err := l.c.Exec(ctx, `DROP TABLE wildfire_output_chunks,wildfire_work,wildfire_state,wildfire_jobs,wildfire_runs; DROP FUNCTION wildfire_route(text,text,text); DELETE FROM canonical_schema_migrations WHERE version='0009_wildfire_processor.sql'`)
			require.NoError(t, err)
			var bytesBefore, bytesAfter []byte
			var envelopeBefore, envelopeAfter string
			require.NoError(t, l.c.QueryRow(ctx, `SELECT original_payload,envelope::text FROM input_history`).Scan(&bytesBefore, &envelopeBefore))
			role := "wildfire_runtime_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			_, err = l.c.Exec(ctx, "CREATE ROLE "+pgx.Identifier{role}.Sanitize()+" NOLOGIN")
			require.NoError(t, err)
			target := "postgres"
			if citus {
				target = "citus"
			}
			require.NoError(t, migrations.Up(ctx, l.c, filepath.Join("..", "..", "..", "store", "migrations"), target, role))
			require.NoError(t, l.c.QueryRow(ctx, `SELECT original_payload,envelope::text FROM input_history`).Scan(&bytesAfter, &envelopeAfter))
			require.Equal(t, bytesBefore, bytesAfter)
			require.Equal(t, envelopeBefore, envelopeAfter)
			var received, prepared, applied int64
			require.NoError(t, l.c.QueryRow(ctx, `SELECT received_through,prepared_through,applied_through FROM input_sessions WHERE source='SATELIT'`).Scan(&received, &prepared, &applied))
			require.Equal(t, int64(1), received)
			require.Zero(t, prepared)
			require.Zero(t, applied)
			restricted, err := pgx.Connect(ctx, l.c.Config().ConnString())
			require.NoError(t, err)
			defer restricted.Close(ctx)
			_, err = restricted.Exec(ctx, "SET ROLE "+pgx.Identifier{role}.Sanitize())
			require.NoError(t, err)
			_, err = restricted.Exec(ctx, `SELECT namespace,position FROM wildfire_jobs LIMIT 0`)
			require.NoError(t, err)
			_, err = restricted.Exec(ctx, `SELECT route_key,payload FROM wildfire_state LIMIT 0`)
			require.NoError(t, err)
			_, err = restricted.Exec(ctx, `ALTER TABLE wildfire_jobs ADD forbidden integer`)
			require.Error(t, err)
			_, err = restricted.Exec(ctx, `SELECT * FROM canonical_schema_migrations`)
			require.Error(t, err)
		})
	}
}
