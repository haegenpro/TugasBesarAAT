package wildfireprocessor

import (
	"aggregator/internal/wildfire"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"reflect"
	"sort"
	"time"
)

// A cursor contains no membership/lineage collection. Every collection and
// visited set is independently distributed and checkpointed in SQL.
type finalCursor struct {
	Phase, After, Fire, Member, Root, Region, Node, Sub, Candidate string
	CandidateAt, CandidateBirth                                    int64
	CandidateKey                                                   string
	HasCandidate                                                   bool
	ChildAt                                                        time.Time
	ChildKey, ChildRoot                                            string
	HasChild                                                       bool
}
type componentSummary struct {
	A    wildfire.Aggregate
	Hash []byte
	Sum  string
}
type finalNode struct {
	Key, Component, Satellite, Confidence string
	At                                    time.Time
	Latitude, Longitude, FRP              float64
	Eligible                              bool
}
type overlapAnchor struct {
	At       time.Time `json:"at"`
	Key      string    `json:"key"`
	ChildAt  time.Time `json:"child_at"`
	ChildKey string    `json:"child_key"`
}
type assignment struct {
	ID       string
	Restored bool
}
type regionNode struct{ ID, Kind string }

func nextFinal(j *job, phase string) { j.CP.Final = &finalCursor{Phase: phase} }
func decodeFinal[T any](r finalRow) (T, error) {
	var v T
	err := json.Unmarshal(r.Data, &v)
	return v, err
}

func (e *Engine) summarize(ctx context.Context, tx pgx.Tx, j *job) error {
	f := j.CP.Final
	rows, err := tx.Query(ctx, `SELECT node_key,component,done FROM wildfire_work WHERE namespace=$1 AND position=$2 AND node_key COLLATE "C">$3 COLLATE "C" ORDER BY node_key COLLATE "C" LIMIT $4`, e.Config.Namespace, j.Position, f.After, e.Config.PageRows)
	if err != nil {
		return err
	}
	type item struct {
		key, component string
		done           bool
	}
	page := []item{}
	for rows.Next() {
		var v item
		if err = rows.Scan(&v.key, &v.component, &v.done); err != nil {
			rows.Close()
			return err
		}
		page = append(page, v)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, v := range page {
		if !v.done {
			return fmt.Errorf("unfinished graph node")
		}
		o, err := e.observation(ctx, tx, v.key, j.Position)
		if err != nil && err != pgx.ErrNoRows {
			return err
		}
		n := finalNode{Key: v.key, Component: v.component, Eligible: eligible(o)}
		if n.Eligible {
			n.At = *o.EventTime
			n.Latitude = o.Latitude
			n.Longitude = o.Longitude
			n.FRP = o.Detection.FRPMW
			n.Satellite = o.Detection.Satellite
			n.Confidence = o.Detection.Confidence
		}
		if err = e.stage(ctx, tx, j, "node", v.key, v.component, "", n, true); err != nil {
			return err
		}
		if n.Eligible && v.component != "" {
			a := componentSummary{}
			prior, find := e.staged(ctx, tx, j.Position, "component", v.component)
			if find == nil {
				a, err = decodeFinal[componentSummary](prior)
				if err != nil {
					return err
				}
			} else if find != pgx.ErrNoRows {
				return find
			}
			a.A.Add(n.Key, n.At, n.Latitude, n.Longitude, n.FRP, n.Confidence)
			h, err := restoreHash(a.Hash)
			if err != nil {
				return err
			}
			_, _ = h.Write(memberBytes(n.Key))
			a.Hash = saveHash(h)
			if err = e.stage(ctx, tx, j, "component", v.component, "", "", a, false); err != nil {
				return err
			}
			if err = e.stage(ctx, tx, j, "satellite", pair(v.component, n.Satellite), v.component, n.Satellite, struct{}{}, true); err != nil {
				return err
			}
		}
		f.After = v.key
	}
	if len(page) < e.Config.PageRows {
		nextFinal(j, "summaries")
	}
	return nil
}
func (e *Engine) finishSummaries(ctx context.Context, tx pgx.Tx, j *job) error {
	f := j.CP.Final
	page, err := e.stagePage(ctx, tx, j.Position, "component", f.After, "", "")
	if err != nil {
		return err
	}
	for _, r := range page {
		a, err := decodeFinal[componentSummary](r)
		if err != nil {
			return err
		}
		h, err := restoreHash(a.Hash)
		if err != nil {
			return err
		}
		a.Sum = hex.EncodeToString(h.Sum(nil))
		a.Hash = nil
		if err = e.stage(ctx, tx, j, "component", r.Key, "", "", a, true); err != nil {
			return err
		}
		f.After = r.Key
	}
	if len(page) < e.Config.PageRows {
		nextFinal(j, "discover")
	}
	return nil
}
func (e *Engine) discover(ctx context.Context, tx pgx.Tx, j *job) error {
	f := j.CP.Final
	if f.Fire == "" {
		err := tx.QueryRow(ctx, `SELECT fire_id FROM wildfire_fire_versions f WHERE namespace=$1 AND position<=$2 AND fire_id COLLATE "C">$3 COLLATE "C" AND NOT EXISTS(SELECT 1 FROM wildfire_fire_versions n WHERE n.route_key=f.route_key AND n.namespace=f.namespace AND n.fire_id=f.fire_id AND n.position>f.position AND n.position<=$2) ORDER BY fire_id COLLATE "C" LIMIT 1`, e.Config.Namespace, j.Position-1, f.After).Scan(&f.Fire)
		if err == pgx.ErrNoRows {
			nextFinal(j, "regions-parents")
			return nil
		}
		if err != nil {
			return err
		}
	}
	m, err := e.fireMeta(ctx, tx, f.Fire, j.Position-1)
	if err != nil {
		return err
	}
	keys, err := e.memberPage(ctx, tx, m, f.Member)
	if err != nil {
		return err
	}
	for _, key := range keys {
		current := key
		alias, find := e.one(ctx, tx, "alias", key, j.Position)
		if find == nil && alias.Active {
			current = alias.Ref
		} else if find != nil && find != pgx.ErrNoRows {
			return find
		}
		r, find := e.staged(ctx, tx, j.Position, "node", current)
		if find == pgx.ErrNoRows {
			f.Member = key
			continue
		}
		if find != nil {
			return find
		}
		n, err := decodeFinal[finalNode](r)
		if err != nil {
			return err
		}
		if m.Fire.Active {
			if err = e.stage(ctx, tx, j, "parent", f.Fire, "", "", m, true); err != nil {
				return err
			}
		}
		if n.Eligible && n.Component != "" {
			cr, err := e.staged(ctx, tx, j.Position, "component", n.Component)
			if err != nil {
				return err
			}
			c, err := decodeFinal[componentSummary](cr)
			if err != nil {
				return err
			}
			if c.A.Count >= 2 {
				if err = e.stage(ctx, tx, j, "touch", pair(f.Fire, n.Component), n.Component, f.Fire, struct{}{}, true); err != nil {
					return err
				}
				if m.Fire.Active {
					a := overlapAnchor{At: n.At, Key: n.Key, ChildAt: c.A.EarliestAt, ChildKey: c.A.EarliestKey}
					prior, find := e.staged(ctx, tx, j.Position, "overlap", pair(f.Fire, n.Component))
					if find == nil {
						old, err := decodeFinal[overlapAnchor](prior)
						if err != nil {
							return err
						}
						if old.At.Before(a.At) || old.At.Equal(a.At) && old.Key < a.Key {
							a = old
						}
					} else if find != pgx.ErrNoRows {
						return find
					}
					if err = e.stage(ctx, tx, j, "overlap", pair(f.Fire, n.Component), f.Fire, n.Component, a, true); err != nil {
						return err
					}
				}
			}
		}
		f.Member = key
	}
	if len(keys) < e.Config.PageRows {
		f.After, f.Fire, f.Member = f.Fire, "", ""
	}
	return nil
}
func (e *Engine) regionEnqueue(ctx context.Context, tx pgx.Tx, j *job, region, kind, id string) error {
	key := kind + ":" + id
	r, err := e.staged(ctx, tx, j.Position, "region-node", key)
	if err == nil {
		if r.Ref != region {
			return fmt.Errorf("transition regions overlap")
		}
		return nil
	}
	if err != pgx.ErrNoRows {
		return err
	}
	return e.stage(ctx, tx, j, "region-node", key, region, kind, regionNode{id, kind}, false)
}
func (e *Engine) regions(ctx context.Context, tx pgx.Tx, j *job) error {
	f := j.CP.Final
	seedKind := "parent"
	nodeKind := "p"
	if f.Phase == "regions-children" {
		seedKind, nodeKind = "component", "c"
	}
	if f.Region == "" {
		page, err := e.stagePage(ctx, tx, j.Position, seedKind, f.After, "", "")
		if err != nil {
			return err
		}
		for _, r := range page {
			f.After = r.Key
			if seedKind == "component" {
				c, err := decodeFinal[componentSummary](r)
				if err != nil {
					return err
				}
				if c.A.Count < 2 {
					continue
				}
			}
			if _, find := e.staged(ctx, tx, j.Position, "region-node", nodeKind+":"+r.Key); find == nil {
				continue
			} else if find != pgx.ErrNoRows {
				return find
			}
			f.Region = nodeKind + ":" + r.Key
			if err = e.regionEnqueue(ctx, tx, j, f.Region, nodeKind, r.Key); err != nil {
				return err
			}
			break
		}
		if f.Region == "" {
			if len(page) < e.Config.PageRows {
				if seedKind == "parent" {
					nextFinal(j, "regions-children")
				} else {
					nextFinal(j, "relevant-parents")
				}
			}
			return nil
		}
	}
	if f.Node == "" {
		err := tx.QueryRow(ctx, `SELECT entity_key FROM wildfire_finalize WHERE namespace=$1 AND position=$2 AND kind='region-node' AND reference=$3 AND NOT done ORDER BY entity_key COLLATE "C" LIMIT 1`, e.Config.Namespace, j.Position, f.Region).Scan(&f.Node)
		if err == pgx.ErrNoRows {
			f.Region = ""
			return nil
		}
		if err != nil {
			return err
		}
	}
	r, err := e.staged(ctx, tx, j.Position, "region-node", f.Node)
	if err != nil {
		return err
	}
	n, err := decodeFinal[regionNode](r)
	if err != nil {
		return err
	}
	ref, second := "", ""
	if n.Kind == "p" {
		ref = n.ID
	} else {
		second = n.ID
	}
	page, err := e.stagePage(ctx, tx, j.Position, "overlap", f.Sub, ref, second)
	if err != nil {
		return err
	}
	for _, edge := range page {
		otherKind, otherID := "p", edge.Ref
		if n.Kind == "p" {
			otherKind, otherID = "c", edge.Second
		}
		if err = e.regionEnqueue(ctx, tx, j, f.Region, otherKind, otherID); err != nil {
			return err
		}
		f.Sub = edge.Key
	}
	if len(page) < e.Config.PageRows {
		kind := "parent"
		if n.Kind == "c" {
			kind = "component"
		}
		v, err := e.staged(ctx, tx, j.Position, kind, n.ID)
		if err != nil {
			return err
		}
		if err = e.stage(ctx, tx, j, kind, v.Key, f.Region, v.Second, v.Data, v.Done); err != nil {
			return err
		}
		if err = e.stage(ctx, tx, j, "region-node", r.Key, r.Ref, r.Second, r.Data, true); err != nil {
			return err
		}
		f.Node, f.Sub = "", ""
	}
	return nil
}
func (e *Engine) relevant(ctx context.Context, tx pgx.Tx, j *job) error {
	f := j.CP.Final
	if f.Phase != "relevant-walk" {
		kind := "parent"
		if f.Phase == "relevant-touches" {
			kind = "touch"
		}
		page, err := e.stagePage(ctx, tx, j.Position, kind, f.After, "", "")
		if err != nil {
			return err
		}
		for _, r := range page {
			region, id := r.Ref, r.Key
			if kind == "touch" {
				component, err := e.staged(ctx, tx, j.Position, "component", r.Ref)
				if err != nil {
					return err
				}
				region, id = component.Ref, r.Second
			}
			if region != "" {
				key := pair(region, id)
				if _, find := e.staged(ctx, tx, j.Position, "relevant", key); find == pgx.ErrNoRows {
					if err = e.stage(ctx, tx, j, "relevant", key, region, id, struct{}{}, false); err != nil {
						return err
					}
				} else if find != nil {
					return find
				}
			}
			f.After = r.Key
		}
		if len(page) < e.Config.PageRows {
			if kind == "parent" {
				nextFinal(j, "relevant-touches")
			} else {
				nextFinal(j, "relevant-walk")
			}
		}
		return nil
	}
	if f.Node == "" {
		err := tx.QueryRow(ctx, `SELECT entity_key FROM wildfire_finalize WHERE namespace=$1 AND position=$2 AND kind='relevant' AND NOT done ORDER BY entity_key COLLATE "C" LIMIT 1`, e.Config.Namespace, j.Position).Scan(&f.Node)
		if err == pgx.ErrNoRows {
			nextFinal(j, "restoration")
			return nil
		}
		if err != nil {
			return err
		}
	}
	r, err := e.staged(ctx, tx, j.Position, "relevant", f.Node)
	if err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `SELECT DISTINCT related_id COLLATE "C" AS related_id FROM wildfire_lineage WHERE namespace=$1 AND fire_id=$2 AND position<$3 AND related_id COLLATE "C">$4 COLLATE "C" ORDER BY related_id COLLATE "C" LIMIT $5`, e.Config.Namespace, r.Second, j.Position, f.Sub, e.Config.PageRows)
	if err != nil {
		return err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range ids {
		f.Sub = id
		if _, find := e.fireMeta(ctx, tx, id, j.Position-1); find == pgx.ErrNoRows {
			continue
		} else if find != nil {
			return find
		}
		key := pair(r.Ref, id)
		if _, find := e.staged(ctx, tx, j.Position, "relevant", key); find == pgx.ErrNoRows {
			if err = e.stage(ctx, tx, j, "relevant", key, r.Ref, id, struct{}{}, false); err != nil {
				return err
			}
		} else if find != nil {
			return find
		}
	}
	if len(ids) < e.Config.PageRows {
		if err = e.stage(ctx, tx, j, "relevant", r.Key, r.Ref, r.Second, r.Data, true); err != nil {
			return err
		}
		f.Node, f.Sub = "", ""
	}
	return nil
}
func (e *Engine) currentComponent(ctx context.Context, tx pgx.Tx, j *job) (finalRow, componentSummary, error) {
	f := j.CP.Final
	if f.Root == "" {
		page, err := e.stagePage(ctx, tx, j.Position, "component", f.After, "", "")
		if err != nil {
			return finalRow{}, componentSummary{}, err
		}
		for _, r := range page {
			a, err := decodeFinal[componentSummary](r)
			if err != nil {
				return r, a, err
			}
			if a.A.Count >= 2 {
				f.Root = r.Key
				return r, a, nil
			}
			f.After = r.Key
		}
		if len(page) == e.Config.PageRows {
			return finalRow{}, componentSummary{}, nil
		}
		return finalRow{}, componentSummary{}, pgx.ErrNoRows
	}
	r, err := e.staged(ctx, tx, j.Position, "component", f.Root)
	if err != nil {
		return r, componentSummary{}, err
	}
	a, err := decodeFinal[componentSummary](r)
	return r, a, err
}
func advanceComponent(f *finalCursor) {
	f.After, f.Root, f.Member, f.Candidate = f.Root, "", "", ""
	f.HasCandidate = false
}
func (e *Engine) assign(ctx context.Context, tx pgx.Tx, j *job, component finalRow, id string, restored bool) error {
	prior, find := e.staged(ctx, tx, j.Position, "used", id)
	if find == nil && prior.Second != component.Key {
		return fmt.Errorf("identity assigned to two successors")
	}
	if find != nil && find != pgx.ErrNoRows {
		return find
	}
	if err := e.stage(ctx, tx, j, "assignment", component.Key, component.Ref, id, assignment{id, restored}, true); err != nil {
		return err
	}
	return e.stage(ctx, tx, j, "used", id, component.Ref, component.Key, struct{}{}, true)
}
func (e *Engine) restoration(ctx context.Context, tx pgx.Tx, j *job) error {
	f := j.CP.Final
	component, a, err := e.currentComponent(ctx, tx, j)
	if err == pgx.ErrNoRows {
		nextFinal(j, "claims")
		return nil
	}
	if err != nil || component.Key == "" {
		return err
	}
	if f.Candidate == "" {
		var at, birth int64
		var key, id string
		err = tx.QueryRow(ctx, `SELECT fire_id,-deactivated,birth,earliest_key FROM wildfire_fire_versions f WHERE namespace=$1 AND position<$2 AND NOT active AND deactivated IS NOT NULL AND member_count=$3 AND member_sha256=$4 AND NOT EXISTS(SELECT 1 FROM wildfire_fire_versions n WHERE n.route_key=f.route_key AND n.namespace=f.namespace AND n.fire_id=f.fire_id AND n.position>f.position AND n.position<$2) AND (NOT $5 OR (-deactivated,birth,earliest_key,fire_id)>($6,$7,$8,$9)) ORDER BY -deactivated,birth,earliest_key COLLATE "C",fire_id COLLATE "C" LIMIT 1`, e.Config.Namespace, j.Position, a.A.Count, a.Sum, f.HasCandidate, f.CandidateAt, f.CandidateBirth, f.CandidateKey, f.Sub).Scan(&id, &at, &birth, &key)
		if err == pgx.ErrNoRows {
			advanceComponent(f)
			return nil
		}
		if err != nil {
			return err
		}
		f.CandidateAt, f.CandidateBirth, f.CandidateKey, f.Sub, f.HasCandidate = at, birth, key, id, true
		if _, find := e.staged(ctx, tx, j.Position, "relevant", pair(component.Ref, id)); find == pgx.ErrNoRows {
			return nil
		} else if find != nil {
			return find
		}
		f.Candidate = id
	}
	m, err := e.fireMeta(ctx, tx, f.Candidate, j.Position-1)
	if err != nil {
		return err
	}
	old, err := e.memberPage(ctx, tx, m, f.Member)
	if err != nil {
		return err
	}
	now, err := e.stagePage(ctx, tx, j.Position, "node", f.Member, f.Root, "")
	if err != nil {
		return err
	}
	match := len(old) == len(now)
	if match {
		for i, key := range old {
			if key != now[i].Key {
				match = false
				break
			}
		}
	}
	if !match {
		f.Candidate, f.Member = "", ""
		return nil
	}
	if len(old) < e.Config.PageRows {
		if err = e.assign(ctx, tx, j, component, m.Fire.ID, true); err != nil {
			return err
		}
		advanceComponent(f)
		return nil
	}
	f.Member = old[len(old)-1]
	return nil
}
func (e *Engine) claims(ctx context.Context, tx pgx.Tx, j *job) error {
	f := j.CP.Final
	if f.Fire == "" {
		page, err := e.stagePage(ctx, tx, j.Position, "parent", f.After, "", "")
		if err != nil {
			return err
		}
		if len(page) == 0 {
			nextFinal(j, "assignments")
			return nil
		}
		f.Fire = page[0].Key
	}
	rows, err := tx.Query(ctx, `SELECT entity_key,secondary,payload FROM wildfire_finalize WHERE namespace=$1 AND position=$2 AND kind='overlap' AND reference=$3 AND (NOT $4 OR ((payload->>'child_at')::timestamptz,(payload->>'child_key') COLLATE "C",secondary)>($5,$6,$7)) ORDER BY (payload->>'child_at')::timestamptz,(payload->>'child_key') COLLATE "C",secondary COLLATE "C" LIMIT $8`, e.Config.Namespace, j.Position, f.Fire, f.HasChild, f.ChildAt, f.ChildKey, f.ChildRoot, e.Config.PageRows)
	if err != nil {
		return err
	}
	page := []finalRow{}
	for rows.Next() {
		var r finalRow
		if err = rows.Scan(&r.Key, &r.Second, &r.Data); err != nil {
			rows.Close()
			return err
		}
		page = append(page, r)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, r := range page {
		a, err := decodeFinal[overlapAnchor](r)
		if err != nil {
			return err
		}
		f.ChildAt, f.ChildKey, f.ChildRoot, f.HasChild = a.ChildAt, a.ChildKey, r.Second, true
		if _, find := e.staged(ctx, tx, j.Position, "assignment", r.Second); find == nil {
			continue
		} else if find != pgx.ErrNoRows {
			return find
		}
		if err = e.stage(ctx, tx, j, "claim", pair(r.Second, f.Fire), f.Fire, r.Second, a, true); err != nil {
			return err
		}
		f.After, f.Fire, f.HasChild = f.Fire, "", false
		return nil
	}
	if len(page) < e.Config.PageRows {
		f.After, f.Fire, f.HasChild = f.Fire, "", false
	}
	return nil
}
func (e *Engine) assignments(ctx context.Context, tx pgx.Tx, j *job) error {
	f := j.CP.Final
	component, a, err := e.currentComponent(ctx, tx, j)
	if err == pgx.ErrNoRows {
		nextFinal(j, "retire")
		return nil
	}
	if err != nil || component.Key == "" {
		return err
	}
	if _, find := e.staged(ctx, tx, j.Position, "assignment", component.Key); find == nil {
		advanceComponent(f)
		return nil
	} else if find != pgx.ErrNoRows {
		return find
	}
	var id string
	err = tx.QueryRow(ctx, `SELECT reference FROM wildfire_finalize WHERE namespace=$1 AND position=$2 AND kind='claim' AND secondary=$3 ORDER BY (payload->>'at')::timestamptz,(payload->>'key') COLLATE "C",reference COLLATE "C" LIMIT 1`, e.Config.Namespace, j.Position, component.Key).Scan(&id)
	if err == pgx.ErrNoRows {
		derivation := wildfire.Derivation{Namespace: e.Config.Namespace, EarliestKey: a.A.EarliestKey, BirthOffset: j.Position}
		id, err = wildfire.Identity(derivation)
		if err != nil {
			return err
		}
		old, find := e.fireMeta(ctx, tx, id, j.Position-1)
		if find == nil && old.Fire.Derivation != derivation {
			return fmt.Errorf("fire derivation integrity conflict")
		}
		if find != nil && find != pgx.ErrNoRows {
			return find
		}
	} else if err != nil {
		return err
	}
	if err = e.assign(ctx, tx, j, component, id, false); err != nil {
		return err
	}
	advanceComponent(f)
	return nil
}
func (e *Engine) outputIntent(ctx context.Context, tx pgx.Tx, j *job, f wildfire.Fire) error {
	i := wildfire.LogicalIntent(e.Config.Namespace, j.Position, f)
	b, err := json.Marshal(i)
	if err != nil {
		return err
	}
	if int64(len(b)) > e.Config.PreparedBytes {
		return &Pending{"logical output exceeds prepared quota"}
	}
	if err = e.put(ctx, tx, j, "intent", f.ID, i, nil, true, ""); err != nil {
		return err
	}
	j.OutputCount++
	return nil
}
func (e *Engine) retire(ctx context.Context, tx pgx.Tx, j *job) error {
	f := j.CP.Final
	page, err := e.stagePage(ctx, tx, j.Position, "parent", f.After, "", "")
	if err != nil {
		return err
	}
	for _, r := range page {
		if err = e.stage(ctx, tx, j, "region-id", pair(r.Ref, r.Key), r.Ref, r.Key, struct{}{}, true); err != nil {
			return err
		}
		if _, find := e.staged(ctx, tx, j.Position, "used", r.Key); find == pgx.ErrNoRows {
			m, err := decodeFinal[fireMeta](r)
			if err != nil {
				return err
			}
			m.Fire.Active = false
			m.Fire.LogicalRevision++
			pos := j.Position
			m.Fire.DeactivatedAt = &pos
			if err = e.writeFire(ctx, tx, j, m); err != nil {
				return err
			}
			if err = e.outputIntent(ctx, tx, j, m.Fire); err != nil {
				return err
			}
			if err = e.stage(ctx, tx, j, "affected", r.Key, r.Ref, r.Key, struct{}{}, true); err != nil {
				return err
			}
		} else if find != nil {
			return find
		}
		f.After = r.Key
	}
	if len(page) < e.Config.PageRows {
		nextFinal(j, "construct")
	}
	return nil
}
func (e *Engine) construct(ctx context.Context, tx pgx.Tx, j *job) error {
	f := j.CP.Final
	component, a, err := e.currentComponent(ctx, tx, j)
	if err == pgx.ErrNoRows {
		nextFinal(j, "links")
		return nil
	}
	if err != nil || component.Key == "" {
		return err
	}
	as, err := e.staged(ctx, tx, j.Position, "assignment", component.Key)
	if err != nil {
		return err
	}
	assigned, err := decodeFinal[assignment](as)
	if err != nil {
		return err
	}
	page, err := e.stagePage(ctx, tx, j.Position, "node", f.Member, component.Key, "")
	if err != nil {
		return err
	}
	for _, r := range page {
		if err = e.writeMember(ctx, tx, j, assigned.ID, r.Key); err != nil {
			return err
		}
		f.Member = r.Key
	}
	if len(page) == e.Config.PageRows {
		return nil
	}
	keys := []string{}
	satellites := []string{}
	after := ""
	var used int64
	for {
		members, err := e.stagePage(ctx, tx, j.Position, "node", after, component.Key, "")
		if err != nil {
			return err
		}
		for _, m := range members {
			used += int64(len(m.Key) + 32)
			if used > e.Config.PreparedBytes {
				return &Pending{"single fire member output quota"}
			}
			keys = append(keys, m.Key)
			after = m.Key
		}
		if len(members) < e.Config.PageRows {
			break
		}
	}
	after = ""
	for {
		names, err := e.stagePage(ctx, tx, j.Position, "satellite", after, component.Key, "")
		if err != nil {
			return err
		}
		for _, name := range names {
			used += int64(len(name.Second) + 32)
			if used > e.Config.PreparedBytes {
				return &Pending{"single fire satellite output quota"}
			}
			satellites = append(satellites, name.Second)
			after = name.Key
		}
		if len(names) < e.Config.PageRows {
			break
		}
	}
	sort.Strings(satellites)
	if err = e.working(used*2 + e.Config.CacheBytes/4); err != nil {
		return err
	}
	event, err := a.A.Event(keys, satellites, e.provinces)
	if err != nil {
		return &Pending{err.Error()}
	}
	prior, find := e.fireMeta(ctx, tx, assigned.ID, j.Position-1)
	fire := wildfire.Fire{Source: "SATELIT", ID: assigned.ID, Derivation: wildfire.Derivation{Namespace: e.Config.Namespace, EarliestKey: a.A.EarliestKey, BirthOffset: j.Position}, Related: []string{}}
	changed := true
	if find == nil {
		fire = prior.Fire
		old, err := e.eventForMeta(ctx, tx, prior)
		if err != nil {
			return err
		}
		changed = !fire.Active || !reflect.DeepEqual(old, event)
	} else if find != pgx.ErrNoRows {
		return find
	}
	if changed {
		fire.LogicalRevision++
	}
	fire.Active = true
	fire.DeactivatedAt = nil
	fire.Event = event
	if err = e.writeFire(ctx, tx, j, fireMeta{Fire: fire, Position: j.Position, MembersAt: j.Position, Count: a.A.Count, Sum: a.Sum}); err != nil {
		return err
	}
	if changed {
		if err = e.outputIntent(ctx, tx, j, fire); err != nil {
			return err
		}
	}
	if err = e.stage(ctx, tx, j, "region-id", pair(component.Ref, assigned.ID), component.Ref, assigned.ID, struct{}{}, true); err != nil {
		return err
	}
	if err = e.stage(ctx, tx, j, "affected", assigned.ID, component.Ref, assigned.ID, struct{}{}, true); err != nil {
		return err
	}
	advanceComponent(f)
	return nil
}
func (e *Engine) eventForMeta(ctx context.Context, q queryer, m fireMeta) (*wildfire.Event, error) {
	if m.Fire.Event == nil {
		return nil, nil
	}
	event := *m.Fire.Event
	event.Attributes = m.Fire.Event.Attributes
	keys := []string{}
	after := ""
	var used int64
	for {
		page, err := e.memberPage(ctx, q, m, after)
		if err != nil {
			return nil, err
		}
		for _, key := range page {
			used += int64(len(key) + 32)
			if used > e.Config.PreparedBytes {
				return nil, &Pending{"prior fire comparison quota"}
			}
			keys = append(keys, key)
			after = key
		}
		if len(page) < e.Config.PageRows {
			break
		}
	}
	event.Attributes.DetectionKeys = keys
	if int64(len(keys)) != m.Count {
		return nil, fmt.Errorf("prior membership integrity")
	}
	if memberDigest(keys) != m.Sum {
		return nil, fmt.Errorf("prior membership digest integrity")
	}
	return &event, nil
}
func (e *Engine) links(ctx context.Context, tx pgx.Tx, j *job) error {
	f := j.CP.Final
	if f.Fire == "" {
		page, err := e.stagePage(ctx, tx, j.Position, "affected", f.After, "", "")
		if err != nil {
			return err
		}
		if len(page) == 0 {
			j.Phase = "verify"
			j.CP = checkpoint{}
			return nil
		}
		f.Fire, f.Region = page[0].Key, page[0].Ref
	}
	page, err := e.stagePage(ctx, tx, j.Position, "region-id", f.Sub, f.Region, "")
	if err != nil {
		return err
	}
	for _, r := range page {
		if err = e.writeLink(ctx, tx, j, f.Fire, r.Second); err != nil {
			return err
		}
		f.Sub = r.Key
	}
	if len(page) < e.Config.PageRows {
		f.After, f.Fire, f.Sub = f.Fire, "", ""
	}
	return nil
}

func (e *Engine) finalize(ctx context.Context, tx pgx.Tx, j *job) error {
	if j.CP.Final == nil {
		nextFinal(j, "summarize")
	}
	switch j.CP.Final.Phase {
	case "summarize":
		return e.summarize(ctx, tx, j)
	case "summaries":
		return e.finishSummaries(ctx, tx, j)
	case "discover":
		return e.discover(ctx, tx, j)
	case "regions-parents", "regions-children":
		return e.regions(ctx, tx, j)
	case "relevant-parents", "relevant-touches", "relevant-walk":
		return e.relevant(ctx, tx, j)
	case "restoration":
		return e.restoration(ctx, tx, j)
	case "claims":
		return e.claims(ctx, tx, j)
	case "assignments":
		return e.assignments(ctx, tx, j)
	case "retire":
		return e.retire(ctx, tx, j)
	case "construct":
		return e.construct(ctx, tx, j)
	case "links":
		return e.links(ctx, tx, j)
	default:
		return errors.New("unknown finalization checkpoint")
	}
}
