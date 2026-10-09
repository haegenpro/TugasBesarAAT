package store

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"aggregator/internal/clients"
	"aggregator/internal/domain"
	"aggregator/internal/mapping"
	"aggregator/internal/middleware"
	"aggregator/internal/timeutil"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

//go:embed lifecycle.sql
var lifecycleMigration string

func (s *Store) Migrate(ctx context.Context) error {
	ctx = middleware.EnsureCorrelationID(ctx)
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(4031001)`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, lifecycleMigration); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func queueLifecycle(ctx context.Context, tx pgx.Tx, e domain.LifecycleEvent) error {
	e.EventID = uuid.NewString()
	e.PublishedAt = domain.Now()
	if e.Hazard != nil {
		if err := tx.QueryRow(ctx, `SELECT revision FROM hazard_events WHERE hazard_id=$1`, e.Hazard.HazardID).Scan(&e.HazardRevision); err != nil {
			return err
		}
	}
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO report_lifecycle_events(event_id,record_id,revision,event_payload) VALUES ($1,$2,$3,$4)`, e.EventID, e.RecordID, e.Revision, b); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO report_lifecycle_outbox(event_id) VALUES ($1)`, e.EventID)
	return err
}

func (s *Store) saveHazardTx(ctx context.Context, tx pgx.Tx, e domain.HazardEvent, cid string, live bool) (domain.HazardEvent, bool, error) {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, string(e.Source)+":"+e.SourceRefID); err != nil {
		return e, false, err
	}
	old, err := scanRow(tx.QueryRow(ctx, `SELECT `+selectColumns+` FROM hazard_events WHERE source=$1 AND source_ref_id=$2 FOR UPDATE`, e.Source, e.SourceRefID))
	inserted := errors.Is(err, pgx.ErrNoRows)
	if err != nil && !inserted {
		return e, false, err
	}
	revision := int64(1)
	changed := true
	if !inserted {
		e.HazardID = old.HazardID
		e.IngestedAt = old.IngestedAt
		if err = tx.QueryRow(ctx, `SELECT revision FROM hazard_events WHERE hazard_id=$1`, e.HazardID).Scan(&revision); err != nil {
			return e, false, err
		}
		// Compare canonical JSON: normalize time zones and source numeric representation.
		a, _ := json.Marshal(e)
		b, _ := json.Marshal(old)
		var av, bv any
		json.Unmarshal(a, &av)
		json.Unmarshal(b, &bv)
		changed = !reflect.DeepEqual(av, bv)
		if changed {
			revision++
		}
	}
	attrs, err := json.Marshal(e.Attributes)
	if err != nil {
		return e, false, err
	}
	var seen any
	if live {
		seen = domain.Now()
	}
	_, err = tx.Exec(ctx, `INSERT INTO hazard_events(hazard_id,source,source_ref_id,hazard_type,severity,area_name,latitude,longitude,occurred_at,ingested_at,attributes,revision,last_seen_at)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
 ON CONFLICT(source,source_ref_id) DO UPDATE SET severity=excluded.severity,area_name=excluded.area_name,latitude=excluded.latitude,longitude=excluded.longitude,occurred_at=excluded.occurred_at,attributes=excluded.attributes,revision=excluded.revision,last_seen_at=COALESCE(excluded.last_seen_at,hazard_events.last_seen_at)`, e.HazardID, e.Source, e.SourceRefID, e.HazardType, e.Severity, e.AreaName, e.Latitude, e.Longitude, e.OccurredAt.Time, e.IngestedAt.Time, attrs, revision, seen)
	if err != nil {
		return e, false, err
	}
	// A deferred mapping inherits the last real live observation time, never the replay time.
	if _, err = tx.Exec(ctx, `UPDATE hazard_events h SET last_seen_at=GREATEST(h.last_seen_at,o.last_seen_at) FROM source_observations o WHERE h.hazard_id=$1 AND o.source=h.source AND o.record_id=h.source_ref_id AND o.entity=CASE WHEN h.source='BMKG' THEN 'seismic-events' ELSE 'volcanic-reports' END`, e.HazardID); err != nil {
		return e, false, err
	}
	if inserted {
		b, err := json.Marshal(e)
		if err != nil {
			return e, false, err
		}
		_, err = tx.Exec(ctx, `INSERT INTO hazard_outbox(hazard_id,event_payload,correlation_id) VALUES($1,$2,$3)`, e.HazardID, b, cid)
		if err != nil {
			return e, false, err
		}
	}
	if changed && !inserted {
		err = queueLifecycle(ctx, tx, domain.LifecycleEvent{EventType: "hazard_corrected", RecordID: e.HazardID, Revision: revision, CorrelationID: cid, Hazard: &e})
	}
	return e, inserted, err
}

func observation(ctx context.Context, tx pgx.Tx, source, entity, id string, payload any, seq int64) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	live := seq == 0
	var seen any
	if live {
		seen = domain.Now()
		switch r := payload.(type) {
		case clients.SeismicEvent:
			seq = r.SourceSequence
		case clients.TsunamiWarning:
			seq = r.SourceSequence
		case clients.VolcanicReport:
			seq = r.SourceSequence
		}
	}

	_, err = tx.Exec(ctx, `INSERT INTO source_observations(source,entity,record_id,payload,history_sequence,last_seen_at) VALUES($1,$2,$3,$4,$5,$6)
 ON CONFLICT(source,entity,record_id) DO UPDATE SET payload=excluded.payload,history_sequence=GREATEST(source_observations.history_sequence,excluded.history_sequence),last_seen_at=COALESCE(excluded.last_seen_at,source_observations.last_seen_at)
 WHERE ($7 AND (excluded.history_sequence=0 OR excluded.history_sequence>=source_observations.history_sequence)) OR excluded.history_sequence>source_observations.history_sequence`, source, entity, id, b, seq, seen, live)
	return err
}
func (s *Store) sourceTx(ctx context.Context, source string) (pgx.Tx, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	// Serialize source commits, keeping network calls outside transactions.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,1))`, source); err != nil {
		tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}
func advanceCursor(ctx context.Context, tx pgx.Tx, source, entity string, seq int64) error {
	if seq == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `INSERT INTO source_history_cursors(source,entity,cursor) VALUES($1,$2,$3) ON CONFLICT(source,entity) DO UPDATE SET cursor=GREATEST(source_history_cursors.cursor,excluded.cursor)`, source, entity, seq)
	return err
}
func (s *Store) HistoryCursor(ctx context.Context, source, entity string) (int64, error) {
	ctx = middleware.EnsureCorrelationID(ctx)
	var n int64
	err := s.Pool.QueryRow(ctx, `SELECT cursor FROM source_history_cursors WHERE source=$1 AND entity=$2`, source, entity).Scan(&n)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return n, err
}
func (s *Store) CommitSeismic(ctx context.Context, events []clients.SeismicEvent, cid string, seq int64) error {
	ctx = middleware.WithCorrelationID(ctx, cid)
	tx, err := s.sourceTx(ctx, "BMKG")
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, e := range events {
		if err = observation(ctx, tx, "BMKG", "seismic-events", e.EventID, e, seq); err != nil {
			return err
		}
		incoming := seq
		if incoming == 0 {
			incoming = e.SourceSequence
		}
		if incoming > 0 {
			var current int64
			if err = tx.QueryRow(ctx, `SELECT history_sequence FROM source_observations WHERE source='BMKG' AND entity='seismic-events' AND record_id=$1`, e.EventID).Scan(&current); err != nil {
				return err
			}
			if current > incoming {
				continue
			}
		}
		if err = s.assessSeismic(ctx, tx, e.EventID, cid, false, seq == 0); err != nil {
			return err
		}
	}
	if err = advanceCursor(ctx, tx, "BMKG", "seismic-events", seq); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Store) CommitWarnings(ctx context.Context, warnings []clients.TsunamiWarning, cid string, seq int64, checkedBefore time.Time, complete bool) error {
	ctx = middleware.WithCorrelationID(ctx, cid)
	tx, err := s.sourceTx(ctx, "BMKG")
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	ids := map[string]bool{}
	for _, w := range warnings {
		if err = observation(ctx, tx, "BMKG", "tsunami-warnings", w.WarningID, w, seq); err != nil {
			return err
		}
		ids[w.RelatedEventID] = true
	}
	if complete {
		rows, err := tx.Query(ctx, `SELECT record_id FROM source_observations WHERE source='BMKG' AND entity='seismic-events' AND (payload->>'potential_tsunami')::boolean AND observed_at <= $1 AND EXISTS(SELECT 1 FROM report_notices n WHERE n.source='BMKG' AND n.source_ref_id=record_id AND n.state<>'resolved')`, checkedBefore)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids[id] = true
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
	}
	for id := range ids {
		if err = s.assessSeismic(ctx, tx, id, cid, complete, false); err != nil {
			return err
		}
	}
	if err = advanceCursor(ctx, tx, "BMKG", "tsunami-warnings", seq); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Store) assessSeismic(ctx context.Context, tx pgx.Tx, id, cid string, complete, live bool) error {
	var b []byte
	var observedAt time.Time
	err := tx.QueryRow(ctx, `SELECT payload,observed_at FROM source_observations WHERE source='BMKG' AND entity='seismic-events' AND record_id=$1`, id).Scan(&b, &observedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var ev clients.SeismicEvent
	if err = json.Unmarshal(b, &ev); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `SELECT payload FROM source_observations WHERE source='BMKG' AND entity='tsunami-warnings' AND payload->>'related_event_id'=$1 ORDER BY record_id`, id)
	if err != nil {
		return err
	}
	warnings := []clients.TsunamiWarning{}
	for rows.Next() {
		var b []byte
		var w clients.TsunamiWarning
		if err = rows.Scan(&b); err != nil {
			rows.Close()
			return err
		}
		if err = json.Unmarshal(b, &w); err != nil {
			rows.Close()
			return err
		}
		warnings = append(warnings, w)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	var notice domain.Notice
	var nb []byte
	err = tx.QueryRow(ctx, `SELECT payload FROM report_notices WHERE source='BMKG' AND source_ref_id=$1 FOR UPDATE`, id).Scan(&nb)
	exists := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if exists {
		if err = json.Unmarshal(nb, &notice); err != nil {
			return err
		}
	}
	if !ev.PotentialTsunami || len(warnings) > 0 || complete {
		candidate := mapping.SeismicToHazard(ev, warnings)
		candidate.IngestedAt = timeutil.New(observedAt)
		h, _, err := s.saveHazardTx(ctx, tx, candidate, cid, live)
		if err != nil {
			return err
		}
		if exists && notice.State != "resolved" {
			notice.State = "resolved"
			notice.HazardID = h.HazardID
			notice.PendingDependency = ""
			notice.Revision++
			now := domain.Now()
			notice.LastCheckedAt = &now
			return saveNotice(ctx, tx, notice, cid, &h)
		}
		return nil
	}
	// Preserve a previously mapped hazard when an assessment subsequently fails.
	if exists && notice.State == "resolved" {
		if live {
			_, err = tx.Exec(ctx, `UPDATE hazard_events SET last_seen_at=now() WHERE source='BMKG' AND source_ref_id=$1`, id)
			return err
		}
		return nil
	}
	if !exists {
		var raw map[string]any
		json.Unmarshal(b, &raw)
		delete(raw, "_source_sequence")
		delete(raw, "_extra_attributes")
		for k, v := range ev.Attributes {
			raw[k] = v
		}
		notice = domain.Notice{NoticeID: uuid.NewString(), Source: domain.SourceBMKG, SourceRefID: id, ObservationType: domain.HazardSeismic, AreaName: ev.RegionName, OccurredAt: ev.OccurredAt, ReportedAt: observedAt, Revision: 1, State: "pending", PendingDependency: "warning_assessment", Observation: raw}
		var state string
		if err = tx.QueryRow(ctx, `SELECT state FROM source_poll_state WHERE endpoint='BMKG_TSUNAMI'`).Scan(&state); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if state == "stale" || state == "unavailable" {
			notice.State = "assessment_unavailable"
		}
		return saveNotice(ctx, tx, notice, cid, nil)
	}
	raw := map[string]any{}
	if err = json.Unmarshal(b, &raw); err != nil {
		return err
	}
	delete(raw, "_source_sequence")
	delete(raw, "_extra_attributes")
	for k, v := range ev.Attributes {
		raw[k] = v
	}
	if notice.AreaName != ev.RegionName || !notice.OccurredAt.Equal(ev.OccurredAt) || !reflect.DeepEqual(notice.Observation, raw) {
		notice.AreaName = ev.RegionName
		notice.OccurredAt = ev.OccurredAt
		notice.Observation = raw
		notice.Revision++
		return saveNotice(ctx, tx, notice, cid, nil)
	}
	return nil
}
func saveNotice(ctx context.Context, tx pgx.Tx, n domain.Notice, cid string, h *domain.HazardEvent) error {
	b, err := json.Marshal(n)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO report_notices(notice_id,source,source_ref_id,payload,state,revision) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(source,source_ref_id) DO UPDATE SET payload=excluded.payload,state=excluded.state,revision=excluded.revision`, n.NoticeID, n.Source, n.SourceRefID, b, n.State, n.Revision)
	if err != nil {
		return err
	}
	return queueLifecycle(ctx, tx, domain.LifecycleEvent{EventType: "notice_" + n.State, RecordID: n.NoticeID, Revision: n.Revision, CorrelationID: cid, Notice: &n, Hazard: h})
}
func (s *Store) CommitVolcanic(ctx context.Context, reports []clients.VolcanicReport, hazards []domain.HazardEvent, cid string, seq int64) error {
	ctx = middleware.WithCorrelationID(ctx, cid)
	tx, err := s.sourceTx(ctx, "PVMBG")
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if len(reports) != len(hazards) {
		return errors.New("volcanic observation/hazard count mismatch")
	}
	for i, r := range reports {
		if err = observation(ctx, tx, "PVMBG", "volcanic-reports", r.ReportID, r, seq); err != nil {
			return err
		}
		candidate := hazards[i]
		// Replays older than a previously committed source revision only advance the cursor.
		if seq > 0 || r.SourceSequence > 0 {
			var current int64
			if err = tx.QueryRow(ctx, `SELECT history_sequence FROM source_observations WHERE source='PVMBG' AND entity='volcanic-reports' AND record_id=$1`, r.ReportID).Scan(&current); err != nil {
				return err
			}
			incoming := seq
			if incoming == 0 {
				incoming = r.SourceSequence
			}
			if current > incoming {
				continue
			}
		}
		if err = s.assessVolcanic(ctx, tx, r, candidate, cid, seq == 0); err != nil {
			return err
		}
	}
	if err = advanceCursor(ctx, tx, "PVMBG", "volcanic-reports", seq); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Missing reference data produces a generic preliminary notice without discarding valid reports.
func (s *Store) assessVolcanic(ctx context.Context, tx pgx.Tx, r clients.VolcanicReport, h domain.HazardEvent, cid string, live bool) error {
	var b []byte
	var observedAt time.Time
	if err := tx.QueryRow(ctx, `SELECT observed_at FROM source_observations WHERE source='PVMBG' AND entity='volcanic-reports' AND record_id=$1`, r.ReportID).Scan(&observedAt); err != nil {
		return err
	}
	var n domain.Notice
	err := tx.QueryRow(ctx, `SELECT payload FROM report_notices WHERE source='PVMBG' AND source_ref_id=$1 FOR UPDATE`, r.ReportID).Scan(&b)
	exists := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if exists {
		if err = json.Unmarshal(b, &n); err != nil {
			return err
		}
	}
	now := domain.Now()
	if h.HazardID != "" {
		h.IngestedAt = timeutil.New(observedAt)
		saved, _, err := s.saveHazardTx(ctx, tx, h, cid, live)
		if err != nil {
			return err
		}
		if exists && n.State != "resolved" {
			n.State = "resolved"
			n.PendingDependency = ""
			n.HazardID = saved.HazardID
			n.Revision++
			n.LastCheckedAt = &now
			return saveNotice(ctx, tx, n, cid, &saved)
		}
		return nil
	}
	if exists {
		n.LastCheckedAt = &now
		b, _ := json.Marshal(n)
		_, err = tx.Exec(ctx, `UPDATE report_notices SET payload=$1 WHERE notice_id=$2`, b, n.NoticeID)
		return err
	}
	raw := map[string]any{"report_id": r.ReportID, "volcano_id": r.VolcanoID, "alert_level": r.AlertLevel, "reported_at": r.ReportedAt, "eruption_count_24h": r.EruptionCount24h, "ash_column_height_m": r.AshColumnHeightM}
	for k, v := range r.Attributes {
		raw[k] = v
	}
	n = domain.Notice{NoticeID: uuid.NewString(), Source: domain.SourcePVMBG, SourceRefID: r.ReportID, ObservationType: domain.HazardVolcanic, OccurredAt: r.ReportedAt, ReportedAt: observedAt, LastCheckedAt: &now, State: "assessment_unavailable", PendingDependency: "volcano_reference", Revision: 1, Observation: raw}
	return saveNotice(ctx, tx, n, cid, nil)
}
func (s *Store) PendingVolcanic(ctx context.Context) ([]clients.VolcanicReport, error) {
	ctx = middleware.EnsureCorrelationID(ctx)
	rows, err := s.Pool.Query(ctx, `SELECT o.payload FROM source_observations o JOIN report_notices n ON n.source=o.source AND n.source_ref_id=o.record_id WHERE o.source='PVMBG' AND o.entity='volcanic-reports' AND n.state<>'resolved' LIMIT 100`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []clients.VolcanicReport{}
	for rows.Next() {
		var b []byte
		var r clients.VolcanicReport
		if err = rows.Scan(&b); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(b, &r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) ResolveVolcanic(ctx context.Context, r clients.VolcanicReport, h domain.HazardEvent, cid string) error {
	ctx = middleware.WithCorrelationID(ctx, cid)
	tx, err := s.sourceTx(ctx, "PVMBG")
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = s.assessVolcanic(ctx, tx, r, h, cid, false); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
