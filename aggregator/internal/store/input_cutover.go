package store

import (
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"context"
)

// CutoverBMKG is an offline administrator operation: ALL old BMKG writers and
// publishers must be stopped. It preserves UUID/time/revision/outbox contents.
// Each identity handoff is restartable; no unjournalled reversal is attempted.
func (s *Store) CutoverBMKG(ctx context.Context, processor, delivery canonical.Ownership, namespace string) error {
	if namespace == "" {
		return canonical.Fail(400, "invalid_request", "namespace required")
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err = s.ProtectProcessorState(ctx, tx, processor); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO input_cutovers(namespace,state) VALUES($1,'building') ON CONFLICT DO NOTHING`, namespace); err != nil {
		return err
	}
	// Observe original first-seen times; source headers/local receipt positions
	// remain independent. Import contains no copy of the canonical payload set.
	if _, err = tx.Exec(ctx, `INSERT INTO input_bmkg_observations(route_key,namespace,entity,record_id,history_sequence,payload,observed_at) SELECT canonical_route('BMKG',record_id),$1,entity,record_id,history_sequence,(payload-'_source_sequence'-'_extra_attributes')||coalesce(payload->'_extra_attributes','{}'::jsonb),observed_at FROM source_observations WHERE source='BMKG' AND entity IN ('seismic-events','tsunami-warnings') ON CONFLICT DO NOTHING`, namespace); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	last := ""
	for {
		rows, err := s.Pool.Query(ctx, `SELECT source_ref_id,coalesce(source_namespace,'') FROM canonical_entities WHERE source='BMKG' AND source_ref_id COLLATE "C">$1 COLLATE "C" ORDER BY source_ref_id COLLATE "C" LIMIT 100`, last)
		if err != nil {
			return err
		}
		type binding struct{ ref, namespace string }
		items := []binding{}
		for rows.Next() {
			var item binding
			if err = rows.Scan(&item.ref, &item.namespace); err != nil {
				rows.Close()
				return err
			}
			items = append(items, item)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if len(items) == 0 {
			break
		}
		for _, item := range items {
			last = item.ref
			if item.namespace == "raw:"+namespace {
				continue
			}
			if item.namespace != "" {
				return canonical.Fail(409, "stale_operation", "existing ordered namespace needs its own explicit transition")
			}
			if err = s.TransitionNamespace(ctx, delivery, domain.SourceBMKG, item.ref, "", "raw:"+namespace); err != nil {
				return err
			}
		}
	}
	tx, err = s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err = s.ProtectProcessorState(ctx, tx, processor); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE input_cutovers SET state='ready' WHERE namespace=$1`, namespace); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	return s.confirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM input_cutovers WHERE namespace=$1 AND state='ready')`, namespace)
}
