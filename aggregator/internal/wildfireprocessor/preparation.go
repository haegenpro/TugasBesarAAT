package wildfireprocessor

import (
	"aggregator/internal/canonical"
	"aggregator/internal/rawinput"
	"aggregator/internal/wildfire"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"hash"
)

// region reads only the affected transition, bounded by the explicit cache
// quota. Graph traversal/checkpoints remain distributed; no Model/history blob
// or unbounded result ledger is reconstructed.
func (e *Engine) region(ctx context.Context, q queryer, j *job) (map[string]wildfire.Fire, []wildfire.Intent, error) {
	observations := map[string]wildfire.Observation{}
	keys := map[string]bool{}
	groups := map[string][]string{}
	var used int64
	budget := func(n int) error {
		used += int64(n + 256)
		if used > e.Config.CacheBytes/2 {
			return &Pending{"affected transition exceeds validated cache quota"}
		}
		return nil
	}
	cursor := ""
	for {
		rows, err := q.Query(ctx, `SELECT node_key,component,done FROM wildfire_work WHERE namespace=$1 AND position=$2 AND node_key COLLATE "C">$3 COLLATE "C" ORDER BY node_key COLLATE "C" LIMIT $4`, e.Config.Namespace, j.Position, cursor, e.Config.PageRows)
		if err != nil {
			return nil, nil, err
		}
		type item struct {
			key, component string
			done           bool
		}
		page := []item{}
		for rows.Next() {
			var x item
			if err = rows.Scan(&x.key, &x.component, &x.done); err != nil {
				rows.Close()
				return nil, nil, err
			}
			page = append(page, x)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, nil, err
		}
		for _, x := range page {
			if !x.done {
				return nil, nil, canonical.Unavailable()
			}
			if err = budget(len(x.key) * 3); err != nil {
				return nil, nil, err
			}
			keys[x.key] = true
			o, find := e.observation(ctx, q, x.key, j.Position)
			if find != nil && !errors.Is(find, pgx.ErrNoRows) {
				return nil, nil, find
			}
			if find == nil {
				b, _ := json.Marshal(o)
				if err = budget(len(b)); err != nil {
					return nil, nil, err
				}
				observations[x.key] = o
			}
			if x.component != "" && eligible(o) {
				groups[x.component] = append(groups[x.component], x.key)
			}
			cursor = x.key
		}
		if len(page) < e.Config.PageRows {
			break
		}
	}
	aliases := map[string]string{}
	cursor = ""
	for {
		rows, err := e.page(ctx, q, "alias", j.Position, cursor)
		if err != nil {
			return nil, nil, err
		}
		for _, r := range rows {
			if r.Active && keys[r.Ref] {
				if err = budget(len(r.Key) + len(r.Ref)); err != nil {
					return nil, nil, err
				}
				aliases[r.Key] = r.Ref
			}
			cursor = r.Key
		}
		if len(rows) < e.Config.PageRows {
			break
		}
	}
	related := map[string]bool{}
	old := map[string]wildfire.Fire{}
	cursor = ""
	// Scan pages of small fire records, retaining only overlapping predecessors
	// and directly relevant inactive history; never collect the complete session.
	for {
		rows, err := e.page(ctx, q, "fire", j.Position-1, cursor)
		if err != nil {
			return nil, nil, err
		}
		for _, r := range rows {
			var f wildfire.Fire
			if err = json.Unmarshal(r.Payload, &f); err != nil {
				return nil, nil, err
			}
			hit := false
			for _, key := range f.Members {
				if keys[key] || (aliases[key] != "" && keys[aliases[key]]) {
					hit = true
					break
				}
			}
			if hit {
				if err = budget(len(r.Payload)); err != nil {
					return nil, nil, err
				}
				old[f.ID] = f
				related[f.ID] = true
				for _, id := range f.Related {
					related[id] = true
				}
			}
			cursor = r.Key
		}
		if len(rows) < e.Config.PageRows {
			break
		}
	}
	inspected := map[string]bool{}
	for {
		changed := false
		for id := range related {
			if inspected[id] {
				continue
			}
			inspected[id] = true
			changed = true
			r, err := e.one(ctx, q, "fire", id, j.Position-1)
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			if err != nil {
				return nil, nil, err
			}
			var f wildfire.Fire
			if err = json.Unmarshal(r.Payload, &f); err != nil {
				return nil, nil, err
			}
			if !f.Active {
				if _, exists := old[id]; !exists {
					if err = budget(len(r.Payload)); err != nil {
						return nil, nil, err
					}
					old[id] = f
				}
			}
			for _, next := range f.Related {
				if !related[next] {
					if err = budget(len(next)); err != nil {
						return nil, nil, err
					}
					related[next] = true
				}
			}
		}
		if !changed {
			break
		}
	}
	fires, intents, diagnostics := wildfire.ResolveRegion(e.Config.Namespace, j.Position, componentOrder(groups, observations), observations, old, aliases, e.provinces)
	if len(diagnostics) > 0 {
		b, _ := json.Marshal(diagnostics)
		return nil, nil, &Pending{string(b)}
	}
	// Include existing derivation bindings even when outside this transition.
	for id, f := range fires {
		r, err := e.one(ctx, q, "fire", id, j.Position-1)
		if err == nil {
			var prior wildfire.Fire
			if err = json.Unmarshal(r.Payload, &prior); err != nil {
				return nil, nil, err
			}
			if prior.Derivation != f.Derivation {
				return nil, nil, fmt.Errorf("fire derivation integrity conflict")
			}
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, err
		}
	}
	return fires, intents, nil
}

func (e *Engine) resolve(ctx context.Context, tx pgx.Tx, j *job) error {
	fires, intents, err := e.region(ctx, tx, j)
	if err != nil {
		return err
	}
	byID := map[string]wildfire.Intent{}
	for _, intent := range intents {
		byID[intent.SourceRefID] = intent
	}
	changed := 0
	last := j.CP.Cursor
	for _, id := range sortedFireIDs(fires) {
		if id <= j.CP.Cursor {
			continue
		}
		f := fires[id]
		b, err := json.Marshal(f)
		if err != nil {
			return err
		}
		prior, find := e.one(ctx, tx, "fire", id, j.Position-1)
		if find != nil && !errors.Is(find, pgx.ErrNoRows) {
			return find
		}
		if find != nil || !bytes.Equal(prior.Payload, b) {
			if err = e.put(ctx, tx, j, "fire", id, f, nil, f.Active, ""); err != nil {
				return err
			}
		}
		if intent, ok := byID[id]; ok {
			b, _ := json.Marshal(intent)
			if int64(len(b)) > e.Config.PreparedBytes {
				return &Pending{"logical output exceeds prepared quota"}
			}
			if err = e.put(ctx, tx, j, "intent", id, intent, nil, true, ""); err != nil {
				return err
			}
		}
		last = id
		changed++
		if changed == e.Config.PageRows {
			j.CP.Cursor = last
			return nil
		}
	}
	j.OutputCount = int64(len(intents))
	j.Phase = "verify"
	j.CP = checkpoint{}
	return nil
}

func restoreHash(raw []byte) (hash.Hash, error) {
	h := sha256.New()
	if len(raw) > 0 {
		if err := h.(encoding.BinaryUnmarshaler).UnmarshalBinary(raw); err != nil {
			return nil, err
		}
	}
	return h, nil
}
func saveHash(h hash.Hash) []byte { b, _ := h.(encoding.BinaryMarshaler).MarshalBinary(); return b }
func (e *Engine) verify(ctx context.Context, tx pgx.Tx, j *job) error {
	h, err := restoreHash(j.CP.Hash)
	if err != nil {
		return err
	}
	count := 0
	if j.Phase == "verify" {
		rows, err := tx.Query(ctx, `SELECT kind,entity_key,payload,sha256 FROM wildfire_state WHERE namespace=$1 AND position=$2 AND (kind,entity_key)>($3,$4) ORDER BY kind COLLATE "C",entity_key COLLATE "C" LIMIT $5`, e.Config.Namespace, j.Position, j.CP.Kind, j.CP.Cursor, e.Config.PageRows)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var kind, key, sum string
			var b []byte
			if err = rows.Scan(&kind, &key, &b, &sum); err != nil {
				return err
			}
			if rawinput.Hash(b) != sum {
				return fmt.Errorf("staged checksum mismatch")
			}
			_, _ = h.Write(rawinput.Compact(kind, key, sum))
			_, _ = h.Write([]byte{'\n'})
			j.CP.Kind, j.CP.Cursor = kind, key
			j.CP.Verified++
			count++
		}
		if err = rows.Err(); err != nil {
			return err
		}
		rows.Close()
		if count < e.Config.PageRows {
			if j.CP.Verified != j.Records {
				return canonical.Unavailable()
			}
			sum := hex.EncodeToString(h.Sum(nil))
			j.Digest = &sum
			j.CP = checkpoint{}
			j.Phase = "verify_nodes"
			return nil
		}
	} else {
		rows, err := tx.Query(ctx, `SELECT node_key,component,done FROM wildfire_work WHERE namespace=$1 AND position=$2 AND node_key COLLATE "C">$3 COLLATE "C" ORDER BY node_key COLLATE "C" LIMIT $4`, e.Config.Namespace, j.Position, j.CP.Cursor, e.Config.PageRows)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var key, component string
			var done bool
			if err = rows.Scan(&key, &component, &done); err != nil {
				return err
			}
			if !done {
				return canonical.Unavailable()
			}
			_, _ = h.Write(rawinput.Compact(key, component, done))
			_, _ = h.Write([]byte{'\n'})
			j.CP.Cursor = key
			count++
		}
		if err = rows.Err(); err != nil {
			return err
		}
		rows.Close()
		if count < e.Config.PageRows {
			sum := hex.EncodeToString(h.Sum(nil))
			j.NodeDigest = &sum
			j.Phase = "activate"
			j.CP = checkpoint{}
			return nil
		}
	}
	j.CP.Hash = saveHash(h)
	return nil
}
func (e *Engine) activate(ctx context.Context, tx pgx.Tx, j *job) error {
	if j.Digest == nil || j.NodeDigest == nil {
		return canonical.Unavailable()
	}
	if _, err := e.Store.ProcessorStorageBoundary(ctx, j.Topology); err != nil {
		return err
	}
	if err := e.Store.ConfirmProcessorRecord(ctx, `SELECT EXISTS(SELECT 1 FROM wildfire_jobs WHERE namespace=$1 AND position=$2 AND phase='activate' AND state_sha256=$3 AND node_sha256=$4)`, e.Config.Namespace, j.Position, *j.Digest, *j.NodeDigest); err != nil {
		return err
	}
	if err := e.hook(ctx, "before_activation"); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE wildfire_runs SET prepared_through=$2,virtual_now=$3,watermark=$4,late_count=late_count+$5 WHERE namespace=$1 AND prepared_through=$2-1 AND config_sha256=$6`, e.Config.Namespace, j.Position, j.Clock, j.Watermark, boolInt(j.Late), e.fingerprint)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return canonical.Unavailable()
	}
	tag, err = tx.Exec(ctx, `UPDATE input_sessions SET prepared_through=$2 WHERE source='SATELIT' AND namespace=$1 AND prepared_through=$2-1`, e.Config.Namespace, j.Position)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return canonical.Unavailable()
	}
	tag, err = tx.Exec(ctx, `UPDATE input_history SET state='prepared',output_count=$3,preparation_topology=$4 WHERE route_key=input_route('SATELIT',$1,$2) AND source='SATELIT' AND namespace=$1 AND position=$2 AND state='received'`, e.Config.Namespace, j.Position, j.OutputCount, j.Topology)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return canonical.Unavailable()
	}
	j.Phase = "active"
	return nil
}
func boolInt(v bool) int64 {
	if v {
		return 1
	}
	return 0
}
