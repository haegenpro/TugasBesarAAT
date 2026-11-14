package wildfireprocessor

import (
	"aggregator/internal/canonical"
	"aggregator/internal/rawinput"
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"fmt"
	"github.com/jackc/pgx/v5"
	"hash"
)

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
