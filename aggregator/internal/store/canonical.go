package store

import (
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"aggregator/internal/middleware"
	"aggregator/internal/platform"
	"aggregator/internal/timeutil"
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"net/http"
	"sort"
	"time"
)

// Fence must hold authoritative ownership protection through the transaction's
// commit and validate authority over every target. A token preflight is insufficient. No production implementation is
// invented here; tests inject a row-lock/epoch adapter.
type Fence interface {
	Protect(context.Context, pgx.Tx, canonical.Ownership, []canonical.Mutation) error
}

// Durability configures the protected commit and verifies its acknowledged
// outcome across every participant and decision store. Production is fail closed.
type Durability interface {
	Prepare(context.Context, pgx.Tx) error
	Confirm(context.Context, string) error
}
type CanonicalOptions struct {
	Fence                Fence
	Durability           Durability
	MultiEntityValidated bool
}

func mustRoute(source domain.Source, ref string) string {
	r, _ := canonical.Route(string(source), ref)
	return r
}
func materialChange(a, b domain.HazardEvent) (bool, error) {
	// Compare timestamps at database precision, not millisecond wire precision.
	if a.HazardType != b.HazardType || a.Severity != b.Severity || a.AreaName != b.AreaName || a.Latitude != b.Latitude || a.Longitude != b.Longitude || !a.OccurredAt.Time.Equal(b.OccurredAt.Time) {
		return true, nil
	}
	af, err := canonical.Fingerprint(a.Attributes)
	if err != nil {
		return false, err
	}
	bf, err := canonical.Fingerprint(b.Attributes)
	if err != nil {
		return false, err
	}
	return af != bf, nil
}
func (s *Store) ApplyBatch(ctx context.Context, b canonical.Batch) (canonical.Result, error) {
	ctx, cancel := context.WithTimeout(middleware.EnsureCorrelationID(ctx), canonical.Deadline)
	defer cancel()
	if err := canonical.Validate(b); err != nil {
		return canonical.Result{}, err
	}
	fp, err := canonical.Fingerprint(b.Mutations)
	if err != nil {
		return canonical.Result{}, canonical.Fail(400, "invalid_request", "invalid logical content")
	}
	// Committed identity resolution precedes current authority/revision checks.
	var prior canonical.Result
	var priorFP string
	err = s.validatedRead(ctx, func(ctx context.Context) error {
		var raw []byte
		e := s.Pool.QueryRow(ctx, "SELECT fingerprint,result FROM canonical_batches WHERE batch_id=$1", b.BatchID).Scan(&priorFP, &raw)
		if e == nil {
			e = json.Unmarshal(raw, &prior)
		}
		return e
	})
	if err == nil {
		if priorFP != fp {
			return canonical.Result{}, canonical.Fail(409, "identity_conflict", "batch identity has different content")
		}
		if s.Canonical.Durability == nil {
			return canonical.Result{}, canonical.Unavailable()
		}
		if err = s.Canonical.Durability.Confirm(ctx, b.BatchID); err != nil {
			return canonical.Result{}, canonical.Unavailable()
		}
		prior.Outcome = "duplicate"
		return prior, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return canonical.Result{}, canonical.Unavailable()
	}
	if s.Canonical.Fence == nil || s.Canonical.Durability == nil || (len(b.Mutations) > 1 && !s.Canonical.MultiEntityValidated) {
		return canonical.Result{}, canonical.Unavailable()
	}
	tx, err := s.beginMutation(ctx)
	if err != nil {
		return canonical.Result{}, canonical.Unavailable()
	}
	defer tx.Rollback(context.Background())
	// Lock batch, source-scoped operation identities and target identities in one
	// deterministic order. Unique constraints remain the authority, not hash equality.
	keys := []string{"batch:" + b.BatchID}
	for _, m := range b.Mutations {
		keys = append(keys, "entity:"+mustRoute(m.Source, m.SourceRefID), "operation:"+mustRoute(m.Source, m.OperationID))
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,726104))", key); err != nil {
			return canonical.Result{}, canonical.Unavailable()
		}
	}
	var raw []byte
	err = tx.QueryRow(ctx, "SELECT fingerprint,result FROM canonical_batches WHERE batch_id=$1", b.BatchID).Scan(&priorFP, &raw)
	if err == nil {
		if priorFP != fp {
			return canonical.Result{}, canonical.Fail(409, "identity_conflict", "batch identity has different content")
		}
		if err = json.Unmarshal(raw, &prior); err != nil {
			return canonical.Result{}, err
		}
		prior.Outcome = "duplicate"
		// No mutation is needed; release the attempt before confirming old durability.
		if err = tx.Rollback(ctx); err != nil {
			return canonical.Result{}, canonical.Unavailable()
		}
		if err = s.Canonical.Durability.Confirm(ctx, b.BatchID); err != nil {
			return canonical.Result{}, canonical.Unavailable()
		}
		return prior, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return canonical.Result{}, canonical.Unavailable()
	}
	if err = s.Canonical.Fence.Protect(ctx, tx, b.Ownership, b.Mutations); err != nil {
		return canonical.Result{}, err
	}
	if err = s.Canonical.Durability.Prepare(ctx, tx); err != nil {
		return canonical.Result{}, canonical.Unavailable()
	}
	result := canonical.Result{BatchID: b.BatchID, Outcome: "no_change", Items: []canonical.Item{}}
	for _, m := range b.Mutations {
		item, e := s.applyMutation(ctx, tx, b.BatchID, m)
		if e != nil {
			return canonical.Result{}, e
		}
		result.Items = append(result.Items, item)
		if item.Outcome == "applied" {
			result.Outcome = "applied"
		}
	}
	raw, err = json.Marshal(result)
	if err != nil {
		return canonical.Result{}, err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO canonical_batches(batch_id,fingerprint,result) VALUES($1,$2,$3)", b.BatchID, fp, raw); err != nil {
		return canonical.Result{}, canonical.Unavailable()
	}
	if err = tx.Commit(ctx); err != nil {
		return canonical.Result{}, canonical.Unavailable()
	}
	if err = s.Canonical.Durability.Confirm(ctx, b.BatchID); err != nil {
		return canonical.Result{}, canonical.Unavailable()
	}
	return result, nil
}
func (s *Store) applyMutation(ctx context.Context, tx pgx.Tx, batchID string, m canonical.Mutation) (canonical.Item, error) {
	fp, err := canonical.Fingerprint(m)
	if err != nil {
		return canonical.Item{}, err
	}
	opkey := mustRoute(m.Source, m.OperationID)
	route := mustRoute(m.Source, m.SourceRefID)
	var oldFP string
	var raw []byte
	err = tx.QueryRow(ctx, "SELECT fingerprint,result FROM canonical_operations WHERE operation_key=$1", opkey).Scan(&oldFP, &raw)
	if err == nil {
		if oldFP != fp {
			return canonical.Item{}, canonical.Fail(409, "identity_conflict", "operation identity has different content")
		}
		var item canonical.Item
		if err = json.Unmarshal(raw, &item); err != nil {
			return item, err
		}
		item.Outcome = "duplicate"
		return item, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return canonical.Item{}, canonical.Unavailable()
	}
	var id string
	var rev int64
	var active bool
	var ingested time.Time
	var ns *string
	var pos *int64
	err = tx.QueryRow(ctx, "SELECT hazard_id::text,revision,active,ingested_at,source_namespace,source_position FROM canonical_entities WHERE route_key=$1 AND source=$2 AND source_ref_id=$3 FOR UPDATE", route, m.Source, m.SourceRefID).Scan(&id, &rev, &active, &ingested, &ns, &pos)
	exists := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return canonical.Item{}, canonical.Unavailable()
	}
	if exists && ns == nil {
		return canonical.Item{}, canonical.Fail(409, "stale_operation", "explicit legacy identity namespace handoff required")
	}
	if ns != nil && *ns != m.SourceOrder.Namespace {
		return canonical.Item{}, canonical.Fail(409, "stale_operation", "explicit source namespace transition required")
	}
	if pos != nil && m.SourceOrder.Position < *pos {
		return canonical.Item{}, canonical.Fail(409, "stale_operation", "operation precedes stored source order")
	}
	if rev != m.ExpectedRevision {
		return canonical.Item{}, &canonical.Error{Status: 409, Code: "revision_conflict", Message: "canonical revision changed", Details: []map[string]any{{"source": m.Source, "source_ref_id": m.SourceRefID}}}
	}
	if !exists {
		id = uuid.NewString()
		ingested = domain.Now()
	}
	changed := !exists
	var event *domain.HazardEvent
	if m.Operation == "upsert" {
		e := m.Event
		event = &domain.HazardEvent{HazardID: id, Source: m.Source, SourceRefID: m.SourceRefID, HazardType: e.HazardType, Severity: e.Severity, AreaName: e.AreaName, Latitude: e.Latitude, Longitude: e.Longitude, OccurredAt: timeutil.New(e.OccurredAt), IngestedAt: timeutil.New(ingested), Attributes: e.Attributes}
		if exists && active {
			attrs, e2 := json.Marshal(e.Attributes)
			if e2 != nil {
				return canonical.Item{}, e2
			}
			var equal bool
			err = tx.QueryRow(ctx, "SELECT hazard_type=$2 AND severity=$3 AND area_name=$4 AND latitude=$5 AND longitude=$6 AND occurred_at=$7 AND attributes=$8::jsonb FROM hazard_events WHERE route_key=$1 AND hazard_id=$9", route, e.HazardType, e.Severity, e.AreaName, e.Latitude, e.Longitude, e.OccurredAt, attrs, id).Scan(&equal)
			if err != nil {
				return canonical.Item{}, canonical.Unavailable()
			}
			changed = !equal
		} else {
			changed = true
		}
		active = true
	} else {
		changed = changed || active
		active = false
	}
	if changed {
		rev++
	}
	if _, err = tx.Exec(ctx, `INSERT INTO canonical_entities(route_key,hazard_id,source,source_ref_id,revision,active,ingested_at,source_namespace,source_position) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
 ON CONFLICT(route_key,source,source_ref_id) DO UPDATE SET revision=excluded.revision,active=excluded.active,source_namespace=excluded.source_namespace,source_position=excluded.source_position`, route, id, m.Source, m.SourceRefID, rev, active, ingested, m.SourceOrder.Namespace, m.SourceOrder.Position); err != nil {
		return canonical.Item{}, canonical.Unavailable()
	}
	if event != nil {
		attrs, _ := json.Marshal(event.Attributes)
		_, err = tx.Exec(ctx, `INSERT INTO hazard_events(route_key,hazard_id,source,source_ref_id,hazard_type,severity,area_name,latitude,longitude,occurred_at,ingested_at,attributes,revision,active) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,true)
 ON CONFLICT(route_key,hazard_id) DO UPDATE SET hazard_type=excluded.hazard_type,severity=excluded.severity,area_name=excluded.area_name,latitude=excluded.latitude,longitude=excluded.longitude,occurred_at=excluded.occurred_at,attributes=excluded.attributes,revision=excluded.revision,active=true`, route, id, m.Source, m.SourceRefID, event.HazardType, event.Severity, event.AreaName, event.Latitude, event.Longitude, event.OccurredAt.Time, ingested, attrs, rev)
	} else {
		_, err = tx.Exec(ctx, "UPDATE hazard_events SET active=false,revision=$3 WHERE route_key=$1 AND hazard_id=$2", route, id, rev)
	}
	if err != nil {
		return canonical.Item{}, canonical.Unavailable()
	}
	if changed {
		cid := middleware.CorrelationFromContext(ctx)
		if !exists && event != nil {
			raw, _ := json.Marshal(event)
			h := http.Header{}
			platform.Inject(ctx, h)
			_, err = tx.Exec(ctx, "INSERT INTO hazard_outbox(route_key,hazard_id,event_payload,correlation_id,traceparent,tracestate,attempt_id) VALUES($1,$2,$3,$4,$5,$6,$7)", route, id, raw, cid, h.Get("traceparent"), h.Get("tracestate"), attemptID(tx))
		} else {
			kind := "hazard_corrected"
			if event == nil {
				kind = "hazard_retracted"
			}
			e := domain.LifecycleEvent{EventID: uuid.NewString(), EventType: kind, RecordID: id, Revision: rev, HazardRevision: rev, PublishedAt: domain.Now(), CorrelationID: cid, Hazard: event}
			raw, _ := json.Marshal(e)
			err = queueCanonicalLifecycle(ctx, tx, e, raw)
		}
		if err != nil {
			return canonical.Item{}, canonical.Unavailable()
		}
	}
	outcome := "no_change"
	if changed {
		outcome = "applied"
	}
	item := canonical.Item{Source: m.Source, SourceRefID: m.SourceRefID, HazardID: id, Revision: rev, Outcome: outcome}
	raw, _ = json.Marshal(item)
	_, err = tx.Exec(ctx, "INSERT INTO canonical_operations(operation_key,source,operation_id,fingerprint,batch_id,result) VALUES($1,$2,$3,$4,$5,$6)", opkey, m.Source, m.OperationID, fp, batchID, raw)
	if err != nil {
		return canonical.Item{}, canonical.Unavailable()
	}
	return item, nil
}
func queueCanonicalLifecycle(ctx context.Context, tx pgx.Tx, e domain.LifecycleEvent, raw []byte) error {
	var route string
	if err := tx.QueryRow(ctx, "SELECT route_key FROM canonical_entities WHERE hazard_id=$1", e.RecordID).Scan(&route); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "INSERT INTO canonical_lifecycle_events(route_key,event_id,hazard_id,event_payload) VALUES($1,$2,$3,$4)", route, e.EventID, e.RecordID, raw); err != nil {
		return err
	}
	h := http.Header{}
	platform.Inject(ctx, h)
	_, err := tx.Exec(ctx, "INSERT INTO canonical_lifecycle_outbox(route_key,event_id,traceparent,tracestate,attempt_id) VALUES($1,$2,$3,$4,$5)", route, e.EventID, h.Get("traceparent"), h.Get("tracestate"), attemptID(tx))
	return err
}
func (s *Store) Reconcile(ctx context.Context, source domain.Source, ref string) (out canonical.Entity, err error) {
	route, e := canonical.Route(string(source), ref)
	if e != nil || !domain.ValidSource(string(source)) {
		return out, canonical.Fail(400, "invalid_request", "invalid source identity")
	}
	out.Source = source
	out.SourceRefID = ref
	err = s.validatedRead(ctx, func(ctx context.Context) error {
		var ns *string
		var pos *int64
		e := s.Pool.QueryRow(ctx, "SELECT hazard_id::text,revision,active,source_namespace,source_position FROM canonical_entities WHERE route_key=$1 AND source=$2 AND source_ref_id=$3", route, source, ref).Scan(&out.HazardID, &out.Revision, &out.Active, &ns, &pos)
		if errors.Is(e, pgx.ErrNoRows) {
			return canonical.Fail(404, "not_found", "canonical entity absent")
		}
		if e != nil {
			return canonical.Unavailable()
		}
		out.SourceOrder = nil
		out.Event = nil
		if ns != nil && pos != nil {
			out.SourceOrder = &canonical.SourceOrder{Namespace: *ns, Position: *pos}
		}
		if out.Active {
			event, _, e := s.getByIDWithMetadata(ctx, out.HazardID)
			if e != nil {
				return canonical.Unavailable()
			}
			out.Event = &event
		}
		return nil
	})
	return
}

// TransitionNamespace is an internal owner-authorized handoff, not automatic
// offset comparison or a public admin endpoint. M3 must integrate source drain.
func (s *Store) TransitionNamespace(ctx context.Context, owner canonical.Ownership, source domain.Source, ref, oldNS, newNS string) error {
	if newNS == "" || newNS == oldNS || s.Canonical.Fence == nil || s.Canonical.Durability == nil {
		return canonical.Unavailable()
	}
	tx, err := s.beginMutation(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err = s.Canonical.Fence.Protect(ctx, tx, owner, []canonical.Mutation{{Source: source, SourceRefID: ref}}); err != nil {
		return err
	}
	if err = s.Canonical.Durability.Prepare(ctx, tx); err != nil {
		return err
	}
	route := mustRoute(source, ref)
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,726104))", "entity:"+route); err != nil {
		return err
	}
	var current string
	if err = tx.QueryRow(ctx, "SELECT coalesce(source_namespace,'') FROM canonical_entities WHERE route_key=$1 AND source=$2 AND source_ref_id=$3 FOR UPDATE", route, source, ref).Scan(&current); err != nil {
		return err
	}
	if current == newNS {
		if err = tx.Rollback(ctx); err != nil {
			return err
		}
		return s.Canonical.Durability.Confirm(ctx, "namespace:"+route+":"+newNS)
	}
	tag, err := tx.Exec(ctx, "UPDATE canonical_entities SET source_namespace=$4,source_position=0 WHERE route_key=$1 AND source=$2 AND source_ref_id=$3 AND coalesce(source_namespace,'')=$5", route, source, ref, newNS, oldNS)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return canonical.Fail(409, "stale_operation", "namespace transition does not match current binding")
	}
	if err = tx.Commit(ctx); err != nil {
		return canonical.Unavailable()
	}
	return s.Canonical.Durability.Confirm(ctx, "namespace:"+route+":"+newNS)
}
