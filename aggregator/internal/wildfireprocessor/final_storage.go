package wildfireprocessor

import (
	"aggregator/internal/canonical"
	"aggregator/internal/rawinput"
	"aggregator/internal/wildfire"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
)

type finalRow struct {
	Key, Ref, Second string
	Data             json.RawMessage
	Done             bool
}

func (e *Engine) stage(ctx context.Context, tx pgx.Tx, j *job, kind, key, ref, second string, value any, done bool) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	size := int64(len(b) + len(key) + len(ref) + len(second) + 512)
	if size > e.Config.CacheBytes/8 {
		return &Pending{"finalization record cache quota"}
	}
	var previous int64
	err = tx.QueryRow(ctx, `SELECT charged_bytes FROM wildfire_finalize WHERE route_key=wildfire_route($1,'final:'||$2,$3) AND namespace=$1 AND position=$4 AND kind=$2 AND entity_key=$3`, e.Config.Namespace, kind, key, j.Position).Scan(&previous)
	if err != nil && err != pgx.ErrNoRows {
		return err
	}
	if j.Bytes+size-previous > e.Config.StagingBytes {
		return &Pending{"finalization staging quota"}
	}
	_, err = tx.Exec(ctx, `INSERT INTO wildfire_finalize(route_key,namespace,position,kind,entity_key,reference,secondary,payload,done,charged_bytes) VALUES(wildfire_route($1,'final:'||$2,$3),$1,$4,$2,$3,$5,$6,$7,$8,$9) ON CONFLICT(route_key,namespace,position,kind,entity_key) DO UPDATE SET reference=excluded.reference,secondary=excluded.secondary,payload=excluded.payload,done=excluded.done,charged_bytes=excluded.charged_bytes`, e.Config.Namespace, kind, key, j.Position, ref, second, b, done, size)
	if err == nil {
		j.Bytes += size - previous
	}
	return err
}
func (e *Engine) staged(ctx context.Context, q queryer, pos int64, kind, key string) (finalRow, error) {
	r := finalRow{Key: key}
	err := q.QueryRow(ctx, `SELECT reference,secondary,payload,done FROM wildfire_finalize WHERE route_key=wildfire_route($1,'final:'||$2,$3) AND namespace=$1 AND position=$4 AND kind=$2 AND entity_key=$3`, e.Config.Namespace, kind, key, pos).Scan(&r.Ref, &r.Second, &r.Data, &r.Done)
	return r, err
}
func (e *Engine) stagePage(ctx context.Context, q queryer, pos int64, kind, after, ref, second string) ([]finalRow, error) {
	rows, err := q.Query(ctx, `SELECT entity_key,reference,secondary,payload,done FROM wildfire_finalize WHERE namespace=$1 AND position=$2 AND kind=$3 AND entity_key COLLATE "C">$4 COLLATE "C" AND ($5='' OR reference=$5) AND ($6='' OR secondary=$6) ORDER BY entity_key COLLATE "C" LIMIT $7`, e.Config.Namespace, pos, kind, after, ref, second, e.Config.PageRows)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []finalRow{}
	var used int64
	for rows.Next() {
		var r finalRow
		if err = rows.Scan(&r.Key, &r.Ref, &r.Second, &r.Data, &r.Done); err != nil {
			return nil, err
		}
		used += int64(len(r.Key) + len(r.Ref) + len(r.Second) + len(r.Data) + 256)
		if used > e.Config.CacheBytes/4 {
			return nil, &Pending{"finalization page cache quota"}
		}
		if err = e.working(used); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
func pair(a, b string) string { s, _ := canonical.Route(a, b); return s }

type fireMeta struct {
	Fire                       wildfire.Fire
	Position, MembersAt, Count int64
	Sum                        string
}

func (e *Engine) fireMeta(ctx context.Context, q queryer, id string, pos int64) (fireMeta, error) {
	var f fireMeta
	var b []byte
	var headerHash string
	err := q.QueryRow(ctx, `SELECT position,member_position,member_count,member_sha256,header,header_sha256 FROM wildfire_fire_versions WHERE route_key=wildfire_route($1,'fire',$2) AND namespace=$1 AND fire_id=$2 AND position<=$3 ORDER BY position DESC LIMIT 1`, e.Config.Namespace, id, pos).Scan(&f.Position, &f.MembersAt, &f.Count, &f.Sum, &b, &headerHash)
	if err == nil {
		if rawinput.Hash(b) != headerHash {
			return f, fmt.Errorf("fire header integrity")
		}
		if int64(len(b)) > e.Config.CacheBytes/8 {
			return f, &Pending{"fire header quota"}
		}
		err = json.Unmarshal(b, &f.Fire)
	}
	return f, err
}
func (e *Engine) memberPage(ctx context.Context, q queryer, m fireMeta, after string) ([]string, error) {
	rows, err := q.Query(ctx, `SELECT member_key FROM wildfire_memberships WHERE namespace=$1 AND fire_id=$2 AND position=$3 AND member_key COLLATE "C">$4 COLLATE "C" ORDER BY member_key COLLATE "C" LIMIT $5`, e.Config.Namespace, m.Fire.ID, m.MembersAt, after, e.Config.PageRows)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var s string
		if err = rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
func (e *Engine) completeFire(ctx context.Context, q queryer, m fireMeta, pos int64) (wildfire.Fire, error) {
	f := m.Fire
	f.Members = []string{}
	f.Related = []string{}
	after := ""
	var used int64
	for {
		page, err := e.memberPage(ctx, q, m, after)
		if err != nil {
			return f, err
		}
		for _, s := range page {
			used += int64(len(s) + 32)
			if used > e.Config.PreparedBytes {
				return f, &Pending{"single fire membership output quota"}
			}
			f.Members = append(f.Members, s)
			after = s
		}
		if len(page) < e.Config.PageRows {
			break
		}
	}
	if int64(len(f.Members)) != m.Count {
		return f, fmt.Errorf("membership count integrity")
	}
	if memberDigest(f.Members) != m.Sum {
		return f, fmt.Errorf("membership digest integrity")
	}
	if f.Event != nil {
		copy := *f.Event
		copy.Attributes = f.Event.Attributes
		copy.Attributes.DetectionKeys = f.Members
		f.Event = &copy
	}
	after = ""
	for {
		rows, err := q.Query(ctx, `SELECT DISTINCT related_id COLLATE "C" AS related_id FROM wildfire_lineage WHERE namespace=$1 AND fire_id=$2 AND position<=$3 AND related_id COLLATE "C">$4 COLLATE "C" ORDER BY related_id COLLATE "C" LIMIT $5`, e.Config.Namespace, f.ID, pos, after, e.Config.PageRows)
		if err != nil {
			return f, err
		}
		count := 0
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return f, err
			}
			used += int64(len(id) + 32)
			if used > e.Config.PreparedBytes {
				rows.Close()
				return f, &Pending{"single fire lineage inspection quota"}
			}
			f.Related = append(f.Related, id)
			after = id
			count++
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return f, err
		}
		if count < e.Config.PageRows {
			break
		}
	}
	return f, nil
}
func (e *Engine) writeFire(ctx context.Context, tx pgx.Tx, j *job, m fireMeta) error {
	f := m.Fire
	f.Members = nil
	f.Related = nil
	if f.Event != nil {
		v := *f.Event
		v.Attributes = f.Event.Attributes
		v.Attributes.DetectionKeys = nil
		f.Event = &v
	}
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	if err = e.put(ctx, tx, j, "fire", f.ID, f, nil, f.Active, ""); err != nil {
		return err
	}
	var members int64
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM wildfire_memberships WHERE namespace=$1 AND fire_id=$2 AND position=$3`, e.Config.Namespace, f.ID, m.MembersAt).Scan(&members); err != nil {
		return err
	}
	if members != m.Count {
		return fmt.Errorf("normalized membership count integrity")
	}
	_, err = tx.Exec(ctx, `INSERT INTO wildfire_fire_versions(route_key,namespace,fire_id,position,member_position,member_count,member_sha256,header,active,birth,earliest_key,deactivated,header_sha256) VALUES(wildfire_route($1,'fire',$2),$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT DO NOTHING`, e.Config.Namespace, f.ID, j.Position, m.MembersAt, m.Count, m.Sum, b, f.Active, f.Derivation.BirthOffset, f.Derivation.EarliestKey, f.DeactivatedAt, rawinput.Hash(b))
	return err
}
func (e *Engine) writeMember(ctx context.Context, tx pgx.Tx, j *job, id, key string) error {
	value := struct{ Fire, Key string }{id, key}
	if err := e.put(ctx, tx, j, "membership", pair(id, key), value, nil, true, id); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO wildfire_memberships VALUES(wildfire_route($1,'membership',$2),$1,$3,$4,$5) ON CONFLICT DO NOTHING`, e.Config.Namespace, pair(id, key), id, j.Position, key)
	return err
}
func (e *Engine) writeLink(ctx context.Context, tx pgx.Tx, j *job, id, related string) error {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM wildfire_lineage WHERE route_key=wildfire_route($1,'lineage',$2) AND namespace=$1 AND fire_id=$3 AND related_id=$4 AND position<=$5)`, e.Config.Namespace, pair(id, related), id, related, j.Position).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	if err := e.put(ctx, tx, j, "lineage", pair(id, related), struct{ Fire, Related string }{id, related}, nil, true, id); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO wildfire_lineage VALUES(wildfire_route($1,'lineage',$2),$1,$3,$4,$5) ON CONFLICT DO NOTHING`, e.Config.Namespace, pair(id, related), id, related, j.Position)
	return err
}

func memberDigest(keys []string) string {
	h := sha256.New()
	for _, key := range keys {
		_, _ = h.Write(memberBytes(key))
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}
func memberBytes(key string) []byte {
	b := rawinput.Compact(key)
	out := append([]byte(nil), b[1:len(b)-1]...)
	return append(out, '\n')
}

// Compatibility for existing fixture/admin callers writing legacy Fire values.
// Ordinary processing writes normalized members a page at a time instead.
func (e *Engine) indexLegacyFire(ctx context.Context, tx pgx.Tx, pos int64, f wildfire.Fire) error {
	h := sha256.New()
	for _, key := range f.Members {
		_, _ = h.Write(memberBytes(key))
		_, err := tx.Exec(ctx, `INSERT INTO wildfire_memberships VALUES(wildfire_route($1,'membership',$2),$1,$3,$4,$5) ON CONFLICT DO NOTHING`, e.Config.Namespace, pair(f.ID, key), f.ID, pos, key)
		if err != nil {
			return err
		}
	}
	for _, id := range f.Related {
		_, err := tx.Exec(ctx, `INSERT INTO wildfire_lineage VALUES(wildfire_route($1,'lineage',$2),$1,$3,$4,$5) ON CONFLICT DO NOTHING`, e.Config.Namespace, pair(f.ID, id), f.ID, id, pos)
		if err != nil {
			return err
		}
	}
	count := len(f.Members)
	f.Members = nil
	f.Related = nil
	if f.Event != nil {
		v := *f.Event
		v.Attributes = f.Event.Attributes
		v.Attributes.DetectionKeys = nil
		f.Event = &v
	}
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO wildfire_fire_versions VALUES(wildfire_route($1,'fire',$2),$1,$2,$3,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT DO NOTHING`, e.Config.Namespace, f.ID, pos, count, fmt.Sprintf("%x", h.Sum(nil)), b, f.Active, f.Derivation.BirthOffset, f.Derivation.EarliestKey, f.DeactivatedAt, rawinput.Hash(b))
	return err
}
