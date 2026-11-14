package wildfireprocessor

import (
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"aggregator/internal/rawinput"
	"aggregator/internal/wildfire"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"

	"strconv"
)

// Each mutation is reconciled and encoded once. Prefix chunks and their hash,
// byte/count cursor commit together under processor authority. No process-local
// body, oracle state or previous expected revision is needed after restart.
func (e *Engine) sealPaged(ctx context.Context, owner canonical.Ownership, o output) (bool, error) {
	tx, err := e.begin(ctx, owner)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var after string
	var count, size, staged int64
	var chunks int
	var state []byte
	var sealed bool
	err = tx.QueryRow(ctx, `SELECT seal_after,seal_count,coalesce(output_bytes,0),coalesce(output_chunks,0),seal_hash,sealed,staged_bytes FROM wildfire_jobs WHERE namespace=$1 AND position=$2 AND phase='active' FOR UPDATE`, e.Config.Namespace, o.Position).Scan(&after, &count, &size, &chunks, &state, &sealed, &staged)
	if err != nil {
		return false, err
	}
	if sealed {
		return true, nil
	}
	var through int64
	if err = tx.QueryRow(ctx, "SELECT applied_through FROM wildfire_runs WHERE namespace=$1 FOR UPDATE", e.Config.Namespace).Scan(&through); err != nil {
		return false, err
	}
	if through != o.Position-1 {
		return false, canonical.Unavailable()
	}
	h, err := restoreHash(state)
	if err != nil {
		return false, err
	}
	rows, err := tx.Query(ctx, `SELECT entity_key,payload,sha256 FROM wildfire_state WHERE namespace=$1 AND kind='intent' AND position=$2 AND entity_key COLLATE "C">$3 COLLATE "C" ORDER BY entity_key COLLATE "C" LIMIT 1`, e.Config.Namespace, o.Position, after)
	if err != nil {
		return false, err
	}
	var key, sum string
	var raw []byte
	if !rows.Next() {
		rows.Close()
		return false, canonical.Unavailable()
	}
	err = rows.Scan(&key, &raw, &sum)
	rows.Close()
	if err != nil {
		return false, err
	}
	if rawinput.Hash(raw) != sum || int64(len(raw)) > e.Config.PreparedBytes {
		return false, &Pending{"intent integrity or output quota"}
	}
	var intent wildfire.Intent
	if err = json.Unmarshal(raw, &intent); err != nil {
		return false, err
	}
	entity, err := e.Store.Reconcile(ctx, domain.SourceSatelit, intent.SourceRefID)
	revision := int64(0)
	if err != nil {
		var p *canonical.Error
		if !errors.As(err, &p) || p.Status != 404 {
			return false, err
		}
	} else {
		revision = entity.Revision
	}
	m := canonical.Mutation{OperationID: intent.OperationID, Source: domain.SourceSatelit, SourceRefID: intent.SourceRefID, Operation: intent.Operation, SourceOrder: canonical.SourceOrder{Namespace: intent.SourceOrder.Namespace, Position: intent.SourceOrder.Position}, ExpectedRevision: revision}
	if intent.Event != nil {
		v := intent.Event
		b, err := json.Marshal(v.Attributes)
		if err != nil {
			return false, err
		}
		var attributes map[string]any
		d := json.NewDecoder(bytes.NewReader(b))
		d.UseNumber()
		if err = d.Decode(&attributes); err != nil {
			return false, err
		}
		m.Event = &canonical.MutableEvent{HazardType: v.HazardType, Severity: v.Severity, AreaName: v.AreaName, Latitude: v.Latitude, Longitude: v.Longitude, OccurredAt: v.OccurredAt, Attributes: attributes}
	}
	b, err := json.Marshal(m)
	if err != nil {
		return false, err
	}
	prefix := byte(',')
	if count == 0 {
		prefix = '['
	}
	body := append([]byte{prefix}, b...)
	count++
	if count > o.Count {
		return false, canonical.Unavailable()
	}
	sealed = count == o.Count
	if sealed {
		body = append(body, ']')
	}
	reserve := int64(0)
	if !sealed {
		reserve = 1
	}
	if size+int64(len(body))+reserve > e.Config.PreparedBytes || staged+int64(len(body)) > e.Config.StagingBytes {
		return false, &Pending{"complete correction exceeds prepared or staging quota"}
	}
	for start := 0; start < len(body); start += 64 << 10 {
		chunk := body[start:min(start+(64<<10), len(body))]
		identity := strconv.FormatInt(o.Position, 10) + ":" + strconv.Itoa(chunks)
		_, err = tx.Exec(ctx, `INSERT INTO wildfire_output_chunks VALUES(wildfire_route($1,'output',$2),$1,$3,$4,$5,$6)`, e.Config.Namespace, identity, o.Position, chunks, chunk, rawinput.Hash(chunk))
		if err != nil {
			return false, err
		}
		chunks++
	}
	_, _ = h.Write(body)
	size += int64(len(body))
	var digest *string
	if sealed {
		s := hex.EncodeToString(h.Sum(nil))
		digest = &s
	}
	_, err = tx.Exec(ctx, `UPDATE wildfire_jobs SET seal_after=$3,seal_count=$4,seal_hash=$5,output_sha256=$6,output_bytes=$7,output_chunks=$8,seal_cursor=$8,sealed=$9,staged_bytes=staged_bytes+$10 WHERE namespace=$1 AND position=$2`, e.Config.Namespace, o.Position, key, count, saveHash(h), digest, size, chunks, sealed, len(body))
	if err != nil {
		return false, err
	}
	if err = e.hook(ctx, "before_seal_commit"); err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, canonical.Unavailable()
	}
	return true, e.Store.ConfirmProcessorRecord(ctx, `SELECT EXISTS(SELECT 1 FROM wildfire_jobs WHERE namespace=$1 AND position=$2 AND seal_count>=$3 AND seal_after COLLATE "C">=$4 COLLATE "C")`, e.Config.Namespace, o.Position, count, key)
}
