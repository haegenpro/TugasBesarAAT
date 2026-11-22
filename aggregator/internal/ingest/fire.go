package ingest

import (
	"aggregator/internal/domain"
	"aggregator/internal/ownership"
	"aggregator/internal/platform"
	"aggregator/internal/satellite"
	"aggregator/internal/store"
	"aggregator/internal/timeutil"
	"aggregator/internal/wildfire"
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"sort"
	"time"
)

type observationRow struct {
	Observation    wildfire.Observation
	Fire, Previous *string
	Bytes          int
}

func readObservation(row pgx.Row) (observationRow, error) {
	var out observationRow
	var raw []byte
	e := row.Scan(&raw, &out.Fire, &out.Previous)
	if e == nil {
		out.Bytes = len(raw)*3 + 512
		e = json.Unmarshal(raw, &out.Observation)
	}
	return out, e
}

func (s *Store) prepareFire(ctx context.Context, i Input, lateness time.Duration) error {
	defer stage("prepare_fire")()
	tx, e := s.begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if e = ownership.Guard(ctx, tx); e != nil {
		return e
	}
	return s.prepareFireIn(ctx, tx, i, lateness, &fireProgress{single: true})
}

// prepareFireIn prepares one input inside tx and finishes with tx.Commit. When tx is a
// savepoint of a larger transaction, that commit releases the savepoint, so several inputs
// can be prepared in source order within one real commit while an input that fails rolls
// back only its own mutations. The state transitions are identical in both modes. p carries the
// source progress for the transaction (see fireProgress); inside a batch the caller writes it.
func (s *Store) prepareFireIn(ctx context.Context, tx pgx.Tx, i Input, lateness time.Duration, p *fireProgress) error {
	var e error
	var state string
	if e = tx.QueryRow(ctx, `SELECT state FROM raw_inputs WHERE message_id=$1 FOR UPDATE`, i.ID).Scan(&state); e != nil {
		return e
	}
	if state != "pending" {
		return p.finish(ctx, tx)
	}
	if e = p.load(ctx, tx, i); e != nil {
		return e
	}
	inOrder := func() bool {
		return i.Position == p.prepared+1 || (replayGeneration(i) > 0 && i.Position <= p.prepared)
	}
	if !inOrder() {
		// Confirm against the inputs' states before calling it a gap.
		if e = p.refreshPrepared(ctx, tx); e != nil {
			return e
		}
		if !inOrder() {
			return ErrGap
		}
	}
	raw, e := json.Marshal(i.Metadata["operation"])
	if e != nil {
		return e
	}
	var op wildfire.Operation
	if typ, _ := i.Metadata["event_type"].(string); typ != "" {
		eventID, _ := i.Metadata["event_id"].(string)
		normalized, err := satellite.Normalize(i.Namespace, satellite.Event{ID: eventID, Type: typ, Data: i.Payload})
		if err != nil {
			return p.park(ctx, tx, i, err.Error())
		}
		raw = normalized.Operation
	} else if reason, _ := i.Metadata["rejection"].(string); reason != "" {
		return p.park(ctx, tx, i, reason)
	}
	if e = wildfire.Decode(raw, &op); e != nil {
		return p.park(ctx, tx, i, e.Error())
	}
	if op.Namespace != i.Namespace || op.Offset != i.Position || op.Version != 1 {
		return p.park(ctx, tx, i, "source envelope and operation identity disagree")
	}
	if i.Entity == "" {
		i.Entity = op.TargetKey
		if i.Entity == "" && op.Detection != nil {
			i.Entity = op.Detection.Satellite + "|" + op.Detection.AcqDate + "|" + op.Detection.AcqTime + "|" + op.Detection.Latitude + "|" + op.Detection.Longitude
		}
		if i.Entity != "" {
			if _, e = tx.Exec(ctx, `UPDATE raw_inputs SET entity_key=$2 WHERE message_id=$1`, i.ID, i.Entity); e != nil {
				return e
			}
		}
	}
	holder, e := blockHolder(ctx, tx, i)
	if e != nil {
		return e
	}
	if holder != "" && holder != i.ID {
		return p.park(ctx, tx, i, "blocked by an earlier unprocessed change of this entity")
	}
	if op.Kind != "heartbeat" && op.Kind != "detection" && op.Kind != "update" && op.Kind != "retract" {
		return p.park(ctx, tx, i, "unsupported wildfire operation")
	}
	groups := map[string]bool{}
	oldRows := map[string]observationRow{}
	maxRegion := platform.Int("WILDFIRE_REGION_MAX_BYTES", 32<<20)
	maxRows := max(1, maxRegion/2048)
	workingBytes := 0
	charge := func(size int) error {
		workingBytes += size
		if workingBytes > maxRegion {
			return errors.New("wildfire working-memory allowance exceeded; source progress unchanged")
		}
		return nil
	}
	aliases := map[string]string{}
	load := func(key string) (observationRow, error) {
		row, e := readObservation(tx.QueryRow(ctx, `SELECT payload,fire_id,previous_fire_id FROM wildfire_observations WHERE namespace=$1 AND key=$2`, i.Namespace, key))
		if e == nil {
			e = charge(row.Bytes)
		}
		return row, e
	}
	var old observationRow
	if op.Kind == "update" || op.Kind == "retract" {
		if op.TargetKey == "" {
			return p.park(ctx, tx, i, "target_key required")
		}
		old, e = load(op.TargetKey)
		if errors.Is(e, pgx.ErrNoRows) {
			return p.park(ctx, tx, i, "target_key is unknown")
		}
		if e != nil {
			return e
		}
		if op.Offset < old.Observation.LastOffset {
			return p.superseded(ctx, tx, i, holder)
		}
		oldRows[op.TargetKey] = old
		if op.Kind == "update" && old.Observation.Detection == nil {
			return p.park(ctx, tx, i, "update target has no retained detection")
		}
		if op.Kind == "update" && len(op.Changes) != 0 {
			merged, err := wildfire.MergeDetection(*old.Observation.Detection, op.Changes)
			if err != nil {
				return p.park(ctx, tx, i, err.Error())
			}
			op.Detection = &merged
		}
		if old.Fire != nil {
			groups[*old.Fire] = true
		}
	}
	var incoming *wildfire.Observation
	if op.Kind == "detection" || op.Kind == "update" {
		if op.Detection == nil {
			return p.park(ctx, tx, i, "complete detection required")
		}
		ob, e := wildfire.ParseDetection(*op.Detection, op.Offset, nil)
		if e != nil {
			return p.park(ctx, tx, i, e.Error())
		}
		// A valid detection or update asserts active state at its source
		// offset. Acquisition time remains the grouping clock.
		incoming = &ob
		if e = charge(len(ob.Key)*3 + 2048); e != nil {
			return e
		}
		existing, e := load(ob.Key)
		if e == nil {
			if op.Offset < existing.Observation.LastOffset {
				return p.superseded(ctx, tx, i, holder)
			}
			if op.Kind == "update" && ob.Key != op.TargetKey && existing.Observation.Active {
				return p.park(ctx, tx, i, "replacement key is already active")
			}
			oldRows[ob.Key] = existing
			if existing.Fire != nil {
				groups[*existing.Fire] = true
			}
		} else if !errors.Is(e, pgx.ErrNoRows) {
			return e
		}
		// Only the changed detection can add edges between old components. Fetch its
		// indexed candidates, then load their complete existing components once.
		// Retractions/splits use the old component; no session-wide graph is loaded.
		if ob.Active && ob.Detection.Confidence != "l" {
			rows, e := tx.Query(ctx, `SELECT payload,fire_id,previous_fire_id FROM wildfire_observations WHERE namespace=$1 AND active AND confidence<>'l' AND latitude BETWEEN $2-.01 AND $2+.01 AND at BETWEEN $3::timestamptz-interval '6 hours' AND $3::timestamptz+interval '6 hours' ORDER BY key LIMIT $4`, i.Namespace, ob.Latitude, *ob.EventTime, maxRows+1)
			if e != nil {
				return e
			}
			candidates := 0
			for rows.Next() {
				candidates++
				if candidates > maxRows {
					rows.Close()
					return errors.New("wildfire candidate allowance exceeded; source progress unchanged")
				}
				r, e := readObservation(rows)
				if e != nil {
					rows.Close()
					return e
				}
				if e = charge(r.Bytes); e != nil {
					rows.Close()
					return e
				}
				if wildfire.Connected(ob, r.Observation) {
					oldRows[r.Observation.Key] = r
					if r.Fire != nil {
						groups[*r.Fire] = true
					}
				}
			}
			e = rows.Err()
			rows.Close()
			if e != nil {
				return e
			}
		}
	}
	for group := range groups {
		rows, e := tx.Query(ctx, `SELECT payload,fire_id,previous_fire_id FROM wildfire_observations WHERE namespace=$1 AND fire_id=$2 ORDER BY key LIMIT $3`, i.Namespace, group, maxRows+1)
		if e != nil {
			return e
		}
		for rows.Next() {
			r, e := readObservation(rows)
			if e != nil {
				rows.Close()
				return e
			}
			if e = charge(r.Bytes); e != nil {
				rows.Close()
				return e
			}
			oldRows[r.Observation.Key] = r
			if len(oldRows) > maxRows {
				rows.Close()
				return errors.New("wildfire component allowance exceeded; source progress unchanged")
			}
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return e
		}
	}
	oldSingletons, oldExcluded := regionCounts(oldRows, nil)
	priorWatermark := p.watermark
	late := incoming != nil && priorWatermark != nil && incoming.EventTime.Before(*priorWatermark)
	if incoming == nil && old.Observation.EventTime != nil && priorWatermark != nil {
		late = old.Observation.EventTime.Before(*priorWatermark)
	}
	if op.Kind == "update" || op.Kind == "retract" {
		ob := old.Observation
		ob.Active = false
		ob.LastOffset = op.Offset
		raw, _ := json.Marshal(ob)
		if _, e = tx.Exec(ctx, `UPDATE wildfire_observations SET active=false,payload=$3,last_position=$4 WHERE namespace=$1 AND key=$2`, i.Namespace, op.TargetKey, raw, op.Offset); e != nil {
			return e
		}
		r := oldRows[op.TargetKey]
		r.Observation = ob
		oldRows[op.TargetKey] = r
	}
	if incoming != nil {
		ob := *incoming
		body, _ := json.Marshal(ob)
		if _, e = tx.Exec(ctx, `INSERT INTO wildfire_observations(namespace,key,latitude,longitude,at,confidence,active,payload,last_position) VALUES($1,$2,$3,$4,$5,$6,$9,$7,$8) ON CONFLICT(namespace,key) DO UPDATE SET latitude=excluded.latitude,longitude=excluded.longitude,at=excluded.at,confidence=excluded.confidence,active=excluded.active,payload=excluded.payload,last_position=excluded.last_position`, i.Namespace, ob.Key, ob.Latitude, ob.Longitude, *ob.EventTime, ob.Detection.Confidence, body, ob.LastOffset, ob.Active); e != nil {
			return e
		}
		r := oldRows[ob.Key]
		r.Observation = ob
		oldRows[ob.Key] = r
		if op.Kind == "update" && op.TargetKey != ob.Key {
			aliases[op.TargetKey] = ob.Key
		}
	}
	// Bound the affected region by bytes, with backpressure instead of dropping
	// operations. This is not a cap on total history retained in PostgreSQL.
	estimated := 0
	observations := map[string]wildfire.Observation{}
	previous := map[string]bool{}
	for key, r := range oldRows {
		observations[key] = r.Observation
		estimated += len(key) + 512
		if r.Fire != nil {
			previous[*r.Fire] = true
		}
		if r.Previous != nil {
			previous[*r.Previous] = true
		}
	}
	if estimated > maxRegion {
		return errors.New("affected wildfire region exceeds configured working-memory allowance")
	}
	predecessors := map[string]wildfire.Fire{}
	for id := range previous {
		var b []byte
		e = tx.QueryRow(ctx, `SELECT payload FROM wildfire_fires WHERE namespace=$1 AND fire_id=$2`, i.Namespace, id).Scan(&b)
		if errors.Is(e, pgx.ErrNoRows) {
			continue
		}
		if e != nil {
			return e
		}
		if e = charge(len(b) * 3); e != nil {
			return e
		}
		var f wildfire.Fire
		if e = json.Unmarshal(b, &f); e != nil {
			return e
		}
		predecessors[id] = f
	}
	components := fireComponents(observations)
	provinces, e := p.regionProvinces(ctx, tx)
	if e != nil {
		return e
	}
	fires, intents, _ := wildfire.ResolveRegion(i.Namespace, op.Offset, components, observations, predecessors, aliases, provinces)
	changes := []store.CanonicalChange{}
	for _, intent := range intents {
		change := store.CanonicalChange{Ref: intent.SourceRefID, Delete: intent.Operation == "retract"}
		if intent.Event != nil {
			ev := intent.Event
			attrs := map[string]any{}
			b, _ := json.Marshal(ev.Attributes)
			_ = json.Unmarshal(b, &attrs)
			change.Event = &domain.HazardEvent{Source: domain.SourceSatelit, SourceRefID: intent.SourceRefID, HazardType: domain.HazardWildfire, Severity: ev.Severity, AreaName: ev.AreaName, Latitude: ev.Latitude, Longitude: ev.Longitude, OccurredAt: timeutil.New(ev.OccurredAt), Attributes: attrs}
		}
		changes = append(changes, change)
	}
	// Membership: every loaded row first leaves its fire (remembering it as the previous fire),
	// then each member of an active fire joins it. Only the end state is written, and only for rows
	// whose fire_id or previous_fire_id actually changes, so an unchanged member of a large fire is
	// not rewritten for every detection that touches the fire.
	type memberState struct{ fire, previous *string }
	final := make(map[string]memberState, len(oldRows))
	for key, r := range oldRows {
		prev := r.Previous
		if r.Fire != nil {
			prev = r.Fire
		}
		final[key] = memberState{nil, prev}
	}
	for id, f := range fires {
		body, _ := json.Marshal(f)
		if _, e = tx.Exec(ctx, `INSERT INTO wildfire_fires(namespace,fire_id,birth_position,earliest_key,active,payload) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(namespace,fire_id) DO UPDATE SET active=excluded.active,payload=excluded.payload`, i.Namespace, id, f.Derivation.BirthOffset, f.Derivation.EarliestKey, f.Active, body); e != nil {
			return e
		}
		if f.Active {
			fireID := id
			for _, key := range f.Members {
				r := oldRows[key]
				prev := r.Previous
				if r.Fire != nil && *r.Fire != id {
					prev = r.Fire
				}
				final[key] = memberState{&fireID, prev}
			}
		}
	}
	same := func(a, b *string) bool { return a == nil && b == nil || a != nil && b != nil && *a == *b }
	var changedKeys []string
	var changedFires, changedPrevious []*string
	for key, m := range final {
		if r, loaded := oldRows[key]; loaded && same(r.Fire, m.fire) && same(r.Previous, m.previous) {
			continue
		}
		changedKeys = append(changedKeys, key)
		changedFires = append(changedFires, m.fire)
		changedPrevious = append(changedPrevious, m.previous)
	}
	if len(changedKeys) > 0 {
		if _, e = tx.Exec(ctx, `UPDATE wildfire_observations o SET fire_id=v.fire,previous_fire_id=v.previous FROM unnest($2::text[],$3::text[],$4::text[]) AS v(key,fire,previous) WHERE o.namespace=$1 AND o.key=v.key`, i.Namespace, changedKeys, changedFires, changedPrevious); e != nil {
			return e
		}
	}
	membership := map[string]bool{}
	for _, component := range components {
		for _, key := range component {
			membership[key] = true
		}
	}
	singletons, excluded := regionCounts(oldRows, membership)
	p.singletons += int64(singletons - oldSingletons)
	p.excluded += int64(excluded - oldExcluded)
	if late {
		p.late++
	}
	// Original source positions remain distinct from prepared/applied frontiers.
	batch := store.CanonicalBatch{ID: wildfire.BatchIdentity(i.Namespace, i.Position), Source: i.Source, Namespace: i.Namespace, Previous: i.Position - 1, Position: i.Position, Changes: changes}
	if e = p.queue(ctx, tx, i, batch); e != nil {
		return e
	}
	if op.VirtualNow != nil {
		p.clock(*op.VirtualNow, lateness)
	}
	if holder == i.ID {
		if e = releaseEntity(ctx, tx, i); e != nil {
			return e
		}
	}
	p.done(i.Position)
	return p.finish(ctx, tx)
}

func regionCounts(rows map[string]observationRow, membership map[string]bool) (singletons, excluded int) {
	for key, row := range rows {
		ob := row.Observation
		if !ob.Active || ob.Detection == nil {
			continue
		}
		if ob.Detection.Confidence == "l" {
			excluded++
			continue
		}
		inFire := row.Fire != nil
		if membership != nil {
			inFire = membership[key]
		}
		if !inFire {
			singletons++
		}
	}
	return
}
func readProvinces(ctx context.Context, tx pgx.Tx, namespace string) ([]wildfire.Province, error) {
	rows, e := tx.Query(ctx, `SELECT payload FROM provinces WHERE namespace=$1 ORDER BY id`, namespace)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []wildfire.Province{}
	for rows.Next() {
		var b []byte
		var p wildfire.Province
		if e = rows.Scan(&b); e != nil {
			return nil, e
		}
		if e = json.Unmarshal(b, &p); e != nil {
			return nil, e
		}
		out = append(out, p)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	if len(out) == 0 {
		return nil, errors.New("verified province reference has not been installed")
	}
	return out, nil
}
func fireComponents(obs map[string]wildfire.Observation) [][]string {
	keys := []string{}
	for k, o := range obs {
		if o.Active && o.Detection != nil && o.Detection.Confidence != "l" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	seen := map[string]bool{}
	out := [][]string{}
	for _, root := range keys {
		if seen[root] {
			continue
		}
		seen[root] = true
		queue := []string{root}
		for head := 0; head < len(queue); head++ {
			a := obs[queue[head]]
			for _, candidate := range keys {
				if seen[candidate] {
					continue
				}
				b := obs[candidate]
				if a.Latitude-b.Latitude > .01 || b.Latitude-a.Latitude > .01 {
					continue
				}
				if wildfire.Connected(a, b) {
					seen[candidate] = true
					queue = append(queue, candidate)
				}
			}
		}
		if len(queue) >= 2 {
			sort.Strings(queue)
			out = append(out, queue)
		}
	}
	earliest := func(component []string) string {
		best := component[0]
		for _, key := range component[1:] {
			if obs[key].EventTime.Before(*obs[best].EventTime) || obs[key].EventTime.Equal(*obs[best].EventTime) && key < best {
				best = key
			}
		}
		return best
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := earliest(out[i]), earliest(out[j])
		if obs[a].EventTime.Equal(*obs[b].EventTime) {
			return a < b
		}
		return obs[a].EventTime.Before(*obs[b].EventTime)
	})
	return out
}
