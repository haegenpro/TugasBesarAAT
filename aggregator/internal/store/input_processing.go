package store

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/canonical"
	"aggregator/internal/clients"
	"aggregator/internal/domain"
	"aggregator/internal/mapping"
	"aggregator/internal/middleware"
	"aggregator/internal/platform"
	"aggregator/internal/rawinput"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"net/http"
	"sort"
	"time"
)

func mapHeader(tp string) http.Header { return http.Header{"Traceparent": []string{tp}} }

// PrepareBMKG processes exactly the next received input. Serial preparation
// behind applied progress deliberately bounds M3; parallel graph state is M6.
// Canonical mutations are NOT made in this transaction.
func (s *Store) PrepareBMKG(ctx context.Context, owner canonical.Ownership, namespace string, registry *avrowire.Client, readers map[string]*avrowire.SourceReader) (bool, error) {
	topology, _, err := s.primaryTopology(ctx)
	if err != nil {
		return false, err
	}
	if err = s.distributedSettled(ctx, topology); err != nil {
		return false, canonical.Unavailable()
	}
	var position, received, prepared, applied int64
	if err := s.Pool.QueryRow(ctx, `SELECT received_through,prepared_through,applied_through FROM input_sessions WHERE source='BMKG' AND namespace=$1`, namespace).Scan(&received, &prepared, &applied); err != nil {
		return false, err
	}
	if prepared != applied || prepared >= received {
		return false, nil
	}
	position = prepared + 1
	var envBytes, payload []byte
	route := string(rawinput.Compact("BMKG", namespace, position))
	if err := s.Pool.QueryRow(ctx, `SELECT envelope,original_payload FROM input_history WHERE route_key=$1 AND source='BMKG' AND namespace=$2 AND position=$3`, route, namespace, position).Scan(&envBytes, &payload); err != nil {
		return false, err
	}
	var envelope rawinput.Envelope
	if err := json.Unmarshal(envBytes, &envelope); err != nil {
		return false, err
	}
	if err := rawinput.Validate(envelope); err != nil {
		return false, err
	}
	if envelope.Operation != "update" {
		return false, canonical.Fail(400, "invalid_request", "bmkg-capture-v1 requires whole-response update operations")
	}
	if rawinput.Hash(payload) != envelope.Digest {
		return false, fmt.Errorf("input payload corrupted")
	}
	capture, err := rawinput.ParseCursor(envelope.Cursor)
	if err != nil {
		return false, err
	}
	ctx = middleware.WithCorrelationID(ctx, envelope.CorrelationID)
	if envelope.Traceparent != nil {
		ctx = platform.Extract(ctx, mapHeader(*envelope.Traceparent))
	}
	ctx, span := platform.Start(ctx, "prepare BMKG input")
	defer span.End()
	records, capture, err := rawinput.DecodeBMKG(ctx, registry, readers[capture.Entity], capture, payload)
	if err != nil {
		return false, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.Background())
	if err = s.ProtectProcessorState(ctx, tx, owner); err != nil {
		return false, err
	}
	if err = tx.QueryRow(ctx, `SELECT received_through,prepared_through,applied_through FROM input_sessions WHERE source='BMKG' AND namespace=$1 FOR UPDATE`, namespace).Scan(&received, &prepared, &applied); err != nil {
		return false, err
	}
	if prepared+1 != position || prepared != applied {
		return false, nil
	}
	affected := map[string]bool{}
	for _, record := range records {
		var id string
		if capture.Entity == "seismic-events" {
			e, err := clients.DecodeSeismic(record.Payload)
			if err != nil {
				return false, err
			}
			id = e.EventID
		} else {
			w, err := clients.DecodeWarning(record.Payload)
			if err != nil {
				return false, err
			}
			id = w.WarningID
			affected[w.RelatedEventID] = true
		}
		key, _ := canonical.Route("BMKG", id)
		tag, err := tx.Exec(ctx, `INSERT INTO input_bmkg_observations(route_key,namespace,entity,record_id,history_sequence,payload,observed_at) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(route_key,namespace,entity,record_id) DO UPDATE SET history_sequence=excluded.history_sequence,payload=excluded.payload,observed_at=CASE WHEN input_bmkg_observations.payload IS DISTINCT FROM excluded.payload THEN excluded.observed_at ELSE input_bmkg_observations.observed_at END WHERE input_bmkg_observations.history_sequence<=excluded.history_sequence`, key, namespace, capture.Entity, id, record.Cursor, record.Payload, capture.RequestStarted)
		if err != nil {
			return false, err
		}
		if capture.Entity == "seismic-events" && tag.RowsAffected() > 0 {
			affected[id] = true
		}
	}
	// Only a full warning snapshot plus a caught-up archive proves a negative
	// assessment. An empty incremental page can never resolve a notice.
	var completeBefore *time.Time
	if !capture.History && capture.Full && capture.Entity == "tsunami-warnings" {
		_, err = tx.Exec(ctx, `INSERT INTO input_bmkg_checks(namespace,snapshot_position,snapshot_started) VALUES($1,$2,$3) ON CONFLICT(namespace) DO UPDATE SET snapshot_position=excluded.snapshot_position,snapshot_started=excluded.snapshot_started,validated_position=NULL`, namespace, position, capture.RequestStarted)
		if err != nil {
			return false, err
		}
	}
	if capture.History && capture.Entity == "tsunami-warnings" && len(records) == 0 && !capture.HasMore {
		var cut time.Time
		err = tx.QueryRow(ctx, `UPDATE input_bmkg_checks SET validated_position=$2 WHERE namespace=$1 AND snapshot_position<$2 AND snapshot_started<=$3 RETURNING snapshot_started`, namespace, position, capture.RequestStarted).Scan(&cut)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return false, err
		}
		if err == nil {
			completeBefore = &cut
			rows, err := tx.Query(ctx, `SELECT record_id FROM input_bmkg_observations WHERE namespace=$1 AND entity='seismic-events' AND (payload->>'potential_tsunami')::boolean AND observed_at<=$2`, namespace, cut)
			if err != nil {
				return false, err
			}
			for rows.Next() {
				var id string
				if err = rows.Scan(&id); err != nil {
					rows.Close()
					return false, err
				}
				affected[id] = true
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return false, err
			}
		}
	}
	ids := make([]string, 0, len(affected))
	for id := range affected {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	ordinal := 0
	for _, id := range ids {
		key, _ := canonical.Route("BMKG", id)
		var b []byte
		var observedAt time.Time
		err = tx.QueryRow(ctx, `SELECT payload,observed_at FROM input_bmkg_observations WHERE route_key=$1 AND namespace=$2 AND entity='seismic-events' AND record_id=$3`, key, namespace, id).Scan(&b, &observedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return false, err
		}
		event, err := clients.DecodeSeismic(b)
		if err != nil {
			return false, err
		}
		rows, err := tx.Query(ctx, `SELECT payload FROM input_bmkg_observations WHERE namespace=$1 AND entity='tsunami-warnings' AND payload->>'related_event_id'=$2 ORDER BY record_id COLLATE "C"`, namespace, id)
		if err != nil {
			return false, err
		}
		warnings := []clients.TsunamiWarning{}
		for rows.Next() {
			var w []byte
			if err = rows.Scan(&w); err != nil {
				rows.Close()
				return false, err
			}
			v, err := clients.DecodeWarning(w)
			if err != nil {
				rows.Close()
				return false, err
			}
			warnings = append(warnings, v)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return false, err
		}
		complete := completeBefore != nil && !observedAt.After(*completeBefore)
		if event.PotentialTsunami && len(warnings) == 0 && !complete {
			// Preserve resolved canonical events; do not regress one to a notice when
			// an assessment source is unavailable.
			var nb []byte
			err = tx.QueryRow(ctx, `SELECT payload FROM report_notices WHERE source='BMKG' AND source_ref_id=$1 FOR UPDATE`, id).Scan(&nb)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return false, err
			}
			var notice domain.Notice
			if err == nil {
				if err = json.Unmarshal(nb, &notice); err != nil {
					return false, err
				}
				if notice.State == "resolved" {
					continue
				}
			} else {
				var observation map[string]any
				_ = json.Unmarshal(b, &observation)
				observed, err := time.Parse(time.RFC3339Nano, envelope.ReceivedAt)
				if err != nil {
					return false, err
				}
				notice = domain.Notice{NoticeID: uuid.NewString(), Source: domain.SourceBMKG, SourceRefID: id, ObservationType: domain.HazardSeismic, AreaName: event.RegionName, OccurredAt: event.OccurredAt, ReportedAt: observed, Revision: 1, State: "pending", PendingDependency: "warning_assessment", Observation: observation}
				if err = saveNotice(ctx, tx, notice, envelope.CorrelationID, nil); err != nil {
					return false, err
				}
			}
			continue
		}
		candidate := mapping.SeismicToHazard(event, warnings)
		var revision int64
		var currentNS *string
		err = tx.QueryRow(ctx, `SELECT revision,source_namespace FROM canonical_entities WHERE route_key=$1 AND source='BMKG' AND source_ref_id=$2`, key, id).Scan(&revision, &currentNS)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return false, err
		}
		if err == nil && (currentNS == nil || *currentNS != "raw:"+namespace) {
			return false, canonical.Fail(409, "revision_conflict", "explicit legacy namespace transition required")
		}
		mutation := canonical.Mutation{OperationID: rawinput.Identity("operation-v1", envelope.MessageID, "BMKG", id), Source: domain.SourceBMKG, SourceRefID: id, Operation: "upsert", SourceOrder: canonical.SourceOrder{Namespace: "raw:" + namespace, Position: position}, ExpectedRevision: revision, Event: &canonical.MutableEvent{HazardType: candidate.HazardType, Severity: candidate.Severity, AreaName: candidate.AreaName, Latitude: candidate.Latitude, Longitude: candidate.Longitude, OccurredAt: candidate.OccurredAt.Time, Attributes: candidate.Attributes}}
		batch := canonical.Batch{ContractVersion: 1, BatchID: rawinput.Identity("batch-v1", envelope.MessageID, "BMKG", id), Mutations: []canonical.Mutation{mutation}}
		// Delivery ownership is supplied by the successor; it is deliberately absent
		// from this immutable preparation. Its content is not regenerated on retry.
		bb, _ := json.Marshal(batch)
		if _, err = tx.Exec(ctx, `INSERT INTO input_outputs(route_key,source,namespace,position,ordinal,batch_id,batch) VALUES($1,'BMKG',$2,$3,$4,$5,$6)`, route, namespace, position, ordinal, batch.BatchID, bb); err != nil {
			return false, err
		}
		ordinal++
	}
	if _, err = tx.Exec(ctx, `UPDATE input_history SET state='prepared',output_count=$4,preparation_topology=$5 WHERE route_key=$1 AND source='BMKG' AND namespace=$2 AND position=$3`, route, namespace, position, ordinal, topology); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `UPDATE input_sessions SET prepared_through=$2 WHERE source='BMKG' AND namespace=$1`, namespace, position); err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, canonical.Unavailable()
	}
	if err = s.confirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM input_sessions WHERE source='BMKG' AND namespace=$1 AND prepared_through>=$2)`, namespace, position); err != nil {
		return false, err
	}
	platform.Gauge("raw_bmkg_prepared_through", float64(position))
	return true, nil
}

// ApplyInputOutput retries one persisted output identity. The canonical commit
// and progress transaction are separate; a lost reply is recovered by ApplyBatch.
func (s *Store) ApplyInputOutput(ctx context.Context, processor, delivery canonical.Ownership, namespace string) (bool, error) {
	var prepared, applied int64
	if err := s.Pool.QueryRow(ctx, `SELECT prepared_through,applied_through FROM input_sessions WHERE source='BMKG' AND namespace=$1`, namespace).Scan(&prepared, &applied); err != nil {
		return false, err
	}
	if prepared <= applied {
		return false, nil
	}
	position := applied + 1
	route := string(rawinput.Compact("BMKG", namespace, position))
	var inputState string
	var expected *int
	var preparedTopology *string
	if err := s.Pool.QueryRow(ctx, `SELECT state,output_count,preparation_topology FROM input_history WHERE route_key=$1 AND source='BMKG' AND namespace=$2 AND position=$3`, route, namespace, position).Scan(&inputState, &expected, &preparedTopology); err != nil {
		return false, err
	}
	if inputState != "prepared" || expected == nil || preparedTopology == nil {
		return false, canonical.Unavailable()
	}
	if err := s.distributedSettled(ctx, *preparedTopology); err != nil {
		return false, canonical.Unavailable()
	}
	var actual int
	if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM input_outputs WHERE route_key=$1 AND source='BMKG' AND namespace=$2 AND position=$3`, route, namespace, position).Scan(&actual); err != nil || actual != *expected {
		return false, canonical.Unavailable()
	}
	var ordinal int
	var b []byte
	err := s.Pool.QueryRow(ctx, `SELECT ordinal,batch FROM input_outputs WHERE route_key=$1 AND source='BMKG' AND namespace=$2 AND position=$3 AND state='pending' ORDER BY ordinal LIMIT 1`, route, namespace, position).Scan(&ordinal, &b)
	var result *canonical.Result
	if err == nil {
		var batch canonical.Batch
		if err = json.Unmarshal(b, &batch); err != nil {
			return false, err
		}
		batch.Ownership = delivery
		var env []byte
		if err = s.Pool.QueryRow(ctx, `SELECT envelope FROM input_history WHERE route_key=$1 AND source='BMKG' AND namespace=$2 AND position=$3`, route, namespace, position).Scan(&env); err != nil {
			return false, err
		}
		var e rawinput.Envelope
		_ = json.Unmarshal(env, &e)
		ctx = middleware.WithCorrelationID(ctx, e.CorrelationID)
		if e.Traceparent != nil {
			ctx = platform.Extract(ctx, mapHeader(*e.Traceparent))
		}
		applyCtx, span := platform.Start(ctx, "canonical output delivery")
		r, err := s.ApplyBatch(applyCtx, batch)
		span.End()
		if err != nil {
			return false, err
		}
		result = &r
		if _, pending, err := s.visibility(ctx); err != nil || pending {
			return false, canonical.Unavailable()
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return false, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.Background())
	if err = s.ProtectProcessorState(ctx, tx, processor); err != nil {
		return false, err
	}
	var current int64
	if err = tx.QueryRow(ctx, `SELECT applied_through FROM input_sessions WHERE source='BMKG' AND namespace=$1 FOR UPDATE`, namespace).Scan(&current); err != nil {
		return false, err
	}
	if current >= position {
		return true, nil
	}
	if current != position-1 {
		return false, canonical.Unavailable()
	}
	if result != nil {
		rb, _ := json.Marshal(result)
		tag, err := tx.Exec(ctx, `UPDATE input_outputs SET state='applied',result=$5 WHERE route_key=$1 AND source='BMKG' AND namespace=$2 AND position=$3 AND ordinal=$4 AND state='pending'`, route, namespace, position, ordinal, rb)
		if err != nil {
			return false, err
		}
		if tag.RowsAffected() > 0 {
			for _, item := range result.Items {
				var nb []byte
				err := tx.QueryRow(ctx, `SELECT payload FROM report_notices WHERE source='BMKG' AND source_ref_id=$1 FOR UPDATE`, item.SourceRefID).Scan(&nb)
				if errors.Is(err, pgx.ErrNoRows) {
					continue
				}
				if err != nil {
					return false, err
				}
				var n domain.Notice
				if err = json.Unmarshal(nb, &n); err != nil {
					return false, err
				}
				if n.State != "resolved" {
					n.State = "resolved"
					n.HazardID = item.HazardID
					n.PendingDependency = ""
					n.Revision++
					now := domain.Now()
					n.LastCheckedAt = &now
					key, _ := canonical.Route("BMKG", item.SourceRefID)
					hazard, err := scanRow(tx.QueryRow(ctx, `SELECT `+selectColumns+` FROM hazard_events WHERE route_key=$1 AND hazard_id=$2 AND active`, key, item.HazardID))
					if err != nil {
						return false, err
					}
					if err = saveNotice(ctx, tx, n, middleware.CorrelationFromContext(ctx), &hazard); err != nil {
						return false, err
					}
				}
			}
		}
	}
	var pending bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM input_outputs WHERE route_key=$1 AND source='BMKG' AND namespace=$2 AND position=$3 AND state='pending')`, route, namespace, position).Scan(&pending); err != nil {
		return false, err
	}
	if !pending {
		var raw []byte
		if err = tx.QueryRow(ctx, `SELECT envelope FROM input_history WHERE route_key=$1 AND source='BMKG' AND namespace=$2 AND position=$3`, route, namespace, position).Scan(&raw); err != nil {
			return false, err
		}
		var e rawinput.Envelope
		if err = json.Unmarshal(raw, &e); err != nil {
			return false, err
		}
		capture, err := rawinput.ParseCursor(e.Cursor)
		if err != nil {
			return false, err
		}
		if !capture.History {
			endpoint := "BMKG"
			if capture.Entity == "tsunami-warnings" {
				endpoint = "BMKG_TSUNAMI"
			}
			if _, err = tx.Exec(ctx, `INSERT INTO source_poll_state(endpoint,state,last_success,last_attempt,failures,revision) VALUES($1,'fresh',$2,$2,0,1) ON CONFLICT(endpoint) DO UPDATE SET state='fresh',last_success=greatest(source_poll_state.last_success,excluded.last_success),last_attempt=excluded.last_attempt,failures=0,stale_since=NULL,revision=source_poll_state.revision+1`, endpoint, capture.RequestStarted); err != nil {
				return false, err
			}
		}
		if _, err = tx.Exec(ctx, `UPDATE input_history SET state='applied' WHERE route_key=$1 AND source='BMKG' AND namespace=$2 AND position=$3`, route, namespace, position); err != nil {
			return false, err
		}
		if _, err = tx.Exec(ctx, `UPDATE input_sessions SET applied_through=$2 WHERE source='BMKG' AND namespace=$1`, namespace, position); err != nil {
			return false, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return false, canonical.Unavailable()
	}
	if err = s.confirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM input_history WHERE route_key=$1 AND source='BMKG' AND namespace=$2 AND position=$3 AND state IN ('prepared','applied'))`, route, namespace, position); err != nil {
		return false, err
	}
	if !pending {
		platform.Gauge("raw_bmkg_applied_through", float64(position))
	}
	return true, nil
}
