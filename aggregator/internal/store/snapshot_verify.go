package store

import (
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"context"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Destination verification is separately checkpointed, with the same immutable
// record digest and full timestamp precision as the retained-view copy.
func (s *Store) verifySnapshotPage(ctx context.Context, token CaptureToken, u *snapshotUnit) error {
	pool := s.snapshots.reads[u.Group]
	if pool == nil {
		return canonical.Unavailable()
	}
	h, e := hashState(u.VerifyHash)
	if e != nil {
		return e
	}
	rows, e := pool.Query(ctx, `SELECT route_key,`+selectColumns+` FROM `+physicalTable(u.Target)+` WHERE capture_id=$1 AND ($2::timestamptz IS NULL OR (ingested_at,hazard_id)>($2,$3::uuid)) ORDER BY ingested_at,hazard_id LIMIT 100`, token.ID, u.VerifyTime, nullableID(u.VerifyID))
	if e != nil {
		return e
	}
	next := *u
	n, size := 0, 0
	byteBound := false
	for rows.Next() {
		var r frozenRecord
		var source, kind, severity string
		var occurred, ingested time.Time
		var attrs []byte
		e = rows.Scan(&r.Route, &r.Event.HazardID, &source, &r.Event.SourceRefID, &kind, &severity, &r.Event.AreaName, &r.Event.Latitude, &r.Event.Longitude, &occurred, &ingested, &attrs)
		if e != nil {
			break
		}
		r.Event.Source = domain.Source(source)
		r.Event.HazardType = domain.HazardType(kind)
		r.Event.Severity = domain.Severity(severity)
		r.Event.OccurredAt.Time = occurred
		r.Event.IngestedAt.Time = ingested
		d := json.NewDecoder(strings.NewReader(string(attrs)))
		d.UseNumber()
		if e = d.Decode(&r.Event.Attributes); e != nil {
			break
		}
		b := recordDigest(r)
		if size+len(b) > 1<<20 && n > 0 {
			byteBound = true
			break
		}
		size += len(b)
		if size > 1<<20 {
			e = fmt.Errorf("snapshot verification page quota")
			break
		}
		_, _ = h.Write(b)
		_, _ = h.Write([]byte{'\n'})
		n++
		next.VerifyCount++
		next.VerifyTime = &ingested
		next.VerifyID = r.Event.HazardID
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		return e
	}
	next.VerifyHash, _ = h.(encoding.BinaryMarshaler).MarshalBinary()
	next.Verified = !byteBound && n < 100
	if next.Verified && (next.VerifyCount != u.Count || hex.EncodeToString(h.Sum(nil)) != u.Sum) {
		return fmt.Errorf("snapshot destination count/digest mismatch")
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if e = s.ProtectCaptureCopy(ctx, tx, token); e != nil {
		return e
	}
	_, e = tx.Exec(ctx, `UPDATE audit_snapshot_units SET verified=$3,verify_count=$4,verify_hash=$5,verify_time=$6,verify_id=$7 WHERE capture_id=$1 AND source_shard=$2`, token.ID, u.ID, next.Verified, next.VerifyCount, next.VerifyHash, next.VerifyTime, nullableID(next.VerifyID))
	if e != nil {
		return e
	}
	if e = tx.Commit(ctx); e != nil {
		return canonical.Unavailable()
	}
	*u = next
	return nil
}
