package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"net/http"
	"time"
)

type lifecycleNotice struct {
	NoticeID        string `json:"notice_id"`
	Source          string `json:"source"`
	ObservationType string `json:"observation_type"`
	AreaName        string `json:"area_name"`
	OccurredAt      string `json:"occurred_at"`
	State           string `json:"state"`
	Revision        int64  `json:"revision"`
	HazardID        string `json:"hazard_id,omitempty"`
}
type lifecycleEvent struct {
	EventID        string           `json:"event_id"`
	EventType      string           `json:"event_type"`
	RecordID       string           `json:"record_id"`
	Revision       int64            `json:"revision"`
	HazardRevision int64            `json:"hazard_revision,omitempty"`
	CorrelationID  string           `json:"correlation_id"`
	PublishedAt    string           `json:"published_at"`
	Notice         *lifecycleNotice `json:"notice,omitempty"`
	Hazard         *hazardEvent     `json:"hazard,omitempty"`
	Status         map[string]any   `json:"source_status,omitempty"`
	Raw            json.RawMessage  `json:"-"`
}

func decodeLifecycle(outer map[string]any, cid string) (envelope, error) {
	b, ok := outer["snapshot"].([]byte)
	if !ok {
		return envelope{}, fmt.Errorf("%w: invalid lifecycle snapshot", errConsumerIncompatible)
	}
	var e lifecycleEvent
	if err := json.Unmarshal(b, &e); err != nil {
		return envelope{}, fmt.Errorf("%w: lifecycle JSON: %v", errCorruptPayload, err)
	}
	if e.EventID == "" || e.RecordID == "" || e.Revision < 1 || e.CorrelationID != cid || outer["event_id"] != e.EventID || outer["record_id"] != e.RecordID || outer["revision"] != e.Revision || outer["event_type"] != e.EventType {
		return envelope{}, fmt.Errorf("%w: lifecycle envelope mismatch", errCorruptPayload)
	}
	if _, err := time.Parse(time.RFC3339Nano, e.PublishedAt); err != nil {
		return envelope{}, fmt.Errorf("%w: lifecycle timestamp", errCorruptPayload)
	}
	switch e.EventType {
	case "notice_pending", "notice_assessment_unavailable", "notice_resolved":
		if e.Notice == nil || e.Notice.NoticeID != e.RecordID || e.Notice.Revision != e.Revision {
			return envelope{}, fmt.Errorf("%w: notice identity", errCorruptPayload)
		}
	case "hazard_corrected":
		if e.Hazard == nil || e.Hazard.HazardID != e.RecordID || e.HazardRevision != e.Revision {
			return envelope{}, fmt.Errorf("%w: correction identity", errCorruptPayload)
		}
	case "source_status_changed":
		if e.Status == nil || e.Status["endpoint"] != e.RecordID {
			return envelope{}, fmt.Errorf("%w: source status identity", errCorruptPayload)
		}
	default:
		return envelope{}, fmt.Errorf("%w: unknown lifecycle event type", errConsumerIncompatible)
	}
	if e.Hazard != nil {
		h := e.Hazard
		if h.HazardID == "" || h.SourceRefID == "" || h.Attributes == nil || h.AreaName == "" || h.OccurredAt == "" || h.IngestedAt == "" || e.HazardRevision < 1 {
			return envelope{}, fmt.Errorf("%w: incomplete hazard snapshot", errCorruptPayload)
		}
		switch h.Source {
		case "BMKG", "PVMBG":
		default:
			return envelope{}, fmt.Errorf("%w: hazard source", errCorruptPayload)
		}
		switch h.HazardType {
		case "SEISMIC", "VOLCANIC":
		default:
			return envelope{}, fmt.Errorf("%w: hazard type", errCorruptPayload)
		}
		switch h.Severity {
		case "NORMAL", "WASPADA", "SIAGA", "AWAS":
		default:
			return envelope{}, fmt.Errorf("%w: hazard severity", errCorruptPayload)
		}
	}
	e.Raw = append([]byte(nil), b...)
	return envelope{Lifecycle: &e, CorrelationID: cid, HazardID: e.RecordID}, nil
}
func currentVersion(ctx context.Context, tx *sql.Tx, id string) (int64, error) {
	var v int64
	err := tx.QueryRowContext(ctx, `SELECT revision FROM record_versions WHERE record_id=?`, id).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return v, err
}
func setVersion(ctx context.Context, tx *sql.Tx, id string, v int64) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO record_versions(record_id,revision) VALUES(?,?) ON CONFLICT(record_id) DO UPDATE SET revision=excluded.revision`, id, v)
	return err
}
func (a *app) saveLifecycle(ctx context.Context, meta *jetstream.MsgMetadata, raw []byte, e lifecycleEvent) error {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	// Detect sequence reuse even across duplicate event IDs.
	var old string
	err = tx.QueryRowContext(ctx, `SELECT digest FROM processed_messages WHERE stream=? AND sequence=?`, meta.Stream, meta.Sequence.Stream).Scan(&old)
	if err == nil {
		if old != messageDigest(raw) {
			return fmt.Errorf("%w: reused lifecycle sequence", errConsumerIncompatible)
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO processed_messages(stream,sequence,digest,processed_at) VALUES(?,?,?,?)`, meta.Stream, meta.Sequence.Stream, messageDigest(raw), now); err != nil {
		return err
	}
	encoded := string(e.Raw)
	if encoded == "" {
		b, _ := json.Marshal(e)
		encoded = string(b)
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO lifecycle_events(event_id,record_id,revision,payload,processed_at) VALUES(?,?,?,?,?) ON CONFLICT(event_id) DO NOTHING`, e.EventID, e.RecordID, e.Revision, encoded, now)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		var previous string
		if err = tx.QueryRowContext(ctx, `SELECT payload FROM lifecycle_events WHERE event_id=?`, e.EventID).Scan(&previous); err != nil {
			return err
		}
		if previous != encoded {
			return fmt.Errorf("%w: event ID reused with changed snapshot", errConsumerIncompatible)
		}
		return tx.Commit()
	}
	material := false
	if e.Hazard != nil {
		key := "hazard:" + e.Hazard.HazardID
		v, err := currentVersion(ctx, tx, key)
		if err != nil {
			return err
		}
		if e.HazardRevision > v {
			var output any = e.Hazard
			if a.cfg.Role == "notifier" {
				h := e.Hazard
				output = map[string]any{"hazard_id": h.HazardID, "source": h.Source, "severity": h.Severity, "area_name": h.AreaName, "occurred_at": h.OccurredAt}
			}
			b, err := json.Marshal(output)
			if err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO records(hazard_id,payload,correlation_id,processed_at) VALUES(?,?,?,?) ON CONFLICT(hazard_id) DO UPDATE SET payload=excluded.payload,correlation_id=excluded.correlation_id,processed_at=excluded.processed_at`, e.Hazard.HazardID, string(b), e.CorrelationID, now); err != nil {
				return err
			}
			if err = setVersion(ctx, tx, key, e.HazardRevision); err != nil {
				return err
			}
			material = true
		}
	}
	if e.Notice != nil {
		key := "notice:" + e.Notice.NoticeID
		v, err := currentVersion(ctx, tx, key)
		if err != nil {
			return err
		}
		if e.Revision > v {
			var snapshot struct {
				Notice json.RawMessage `json:"notice"`
			}
			if err = json.Unmarshal([]byte(encoded), &snapshot); err != nil {
				return err
			}
			if len(snapshot.Notice) == 0 {
				snapshot.Notice, _ = json.Marshal(e.Notice)
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO notice_records(notice_id,revision,payload,processed_at) VALUES(?,?,?,?) ON CONFLICT(notice_id) DO UPDATE SET revision=excluded.revision,payload=excluded.payload,processed_at=excluded.processed_at`, e.Notice.NoticeID, e.Revision, string(snapshot.Notice), now); err != nil {
				return err
			}
			if err = setVersion(ctx, tx, key, e.Revision); err != nil {
				return err
			}
			material = true
		}
	}
	if e.Status != nil {
		key := "source:" + e.RecordID
		v, err := currentVersion(ctx, tx, key)
		if err != nil {
			return err
		}
		if e.Revision > v {
			b, _ := json.Marshal(e.Status)
			if _, err = tx.ExecContext(ctx, `INSERT INTO source_status_records(endpoint,revision,payload) VALUES(?,?,?) ON CONFLICT(endpoint) DO UPDATE SET revision=excluded.revision,payload=excluded.payload`, e.RecordID, e.Revision, string(b)); err != nil {
				return err
			}
			if err = setVersion(ctx, tx, key, e.Revision); err != nil {
				return err
			}
		}
	}
	if material && a.cfg.Role == "notifier" {
		if _, err = tx.ExecContext(ctx, `INSERT INTO notifications(event_id,kind,payload,processed_at) VALUES(?,?,?,?) ON CONFLICT(event_id) DO NOTHING`, e.EventID, e.EventType, encoded, now); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func (a *app) lifecycleRecords(w http.ResponseWriter, r *http.Request, view string) {
	query := ""
	switch view {
	case "notices":
		query = `SELECT notice_id,payload,processed_at FROM notice_records ORDER BY processed_at DESC LIMIT 100`
	case "history":
		query = `SELECT event_id,payload,processed_at FROM lifecycle_events ORDER BY processed_at DESC LIMIT 100`
	case "notifications":
		query = `SELECT event_id,payload,processed_at FROM notifications ORDER BY processed_at DESC LIMIT 100`
	default:
		http.Error(w, "invalid view", 400)
		return
	}
	rows, err := a.db.QueryContext(r.Context(), query)
	if err != nil {
		http.Error(w, "store unavailable", 503)
		return
	}
	defer rows.Close()
	items := []map[string]any{}
	for rows.Next() {
		var id, payload, at string
		if err = rows.Scan(&id, &payload, &at); err != nil {
			http.Error(w, "store unavailable", 503)
			return
		}
		items = append(items, map[string]any{"id": id, "payload": json.RawMessage(payload), "processed_at": at})
	}
	if rows.Err() != nil {
		http.Error(w, "store unavailable", 503)
		return
	}
	writeJSON(w, map[string]any{"data": items, "count": len(items)})
}
