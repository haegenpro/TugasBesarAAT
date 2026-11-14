package wildfireprocessor

import (
	"aggregator/internal/rawinput"
	"aggregator/internal/wildfire"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"sort"
	"strings"
	"time"
)

func (e *Engine) start(ctx context.Context, tx pgx.Tx, j *job) error {
	_, op, err := e.input(ctx, tx, j.Position)
	if err != nil {
		return err
	}
	if strings.ContainsRune(op.TargetKey, 0) {
		return fmt.Errorf("invalid target identity")
	}
	var changed wildfire.Observation
	if op.Detection != nil {
		changed, err = wildfire.ParseDetection(*op.Detection, op.Offset, op.Payload)
		if err != nil {
			return err
		}
		previous, find := e.observation(ctx, tx, changed.Key, j.Position-1)
		if find != nil && !errors.Is(find, pgx.ErrNoRows) {
			return find
		}
		if find == nil && previous.Detection != nil && previous.Tuple != changed.Tuple {
			return fmt.Errorf("ambiguous lexical key collision")
		}
		j.CP.New = changed.Key
		j.CP.Old = changed.Key
		if op.Kind == "update" {
			old, err := e.observation(ctx, tx, op.TargetKey, j.Position-1)
			if err != nil || !old.Active || old.Detection == nil {
				return fmt.Errorf("unknown update target")
			}
			j.CP.Old = old.Key
			if old.Key != changed.Key {
				if find == nil && previous.Active {
					return fmt.Errorf("replacement key is already active")
				}
				old.Active = false
				old.LastOffset = op.Offset
				old.Payload = append([]byte(nil), op.Payload...)
				if err = e.put(ctx, tx, j, "observation", old.Key, old, old.EventTime, false, ""); err != nil {
					return err
				}
			}
		}
		if err = e.put(ctx, tx, j, "observation", changed.Key, changed, changed.EventTime, true, ""); err != nil {
			return err
		}
	} else if op.Kind == "retract" {
		changed, err = e.observation(ctx, tx, op.TargetKey, j.Position-1)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		changed.Key = op.TargetKey
		changed.Active = false
		changed.LastOffset = op.Offset
		changed.Payload = append([]byte(nil), op.Payload...)
		j.CP.Old = op.TargetKey
		if err = e.put(ctx, tx, j, "observation", changed.Key, changed, changed.EventTime, false, ""); err != nil {
			return err
		}
	}
	j.Late = changed.EventTime != nil && j.Watermark != nil && changed.EventTime.Before(*j.Watermark)
	if op.VirtualNow != nil && (j.Clock == nil || op.VirtualNow.After(*j.Clock)) {
		now := op.VirtualNow.UTC()
		watermark := now.Add(-time.Duration(*e.Config.AllowedLatenessSeconds) * time.Second)
		if watermark.Year() < 1 || watermark.Year() > 9999 {
			return fmt.Errorf("watermark outside supported range")
		}
		j.Clock = &now
		if j.Watermark == nil || watermark.After(*j.Watermark) {
			j.Watermark = &watermark
		}
	}
	for _, k := range []string{j.CP.Old, j.CP.New} {
		if k != "" && (len(j.CP.Targets) == 0 || j.CP.Targets[0] != k) {
			j.CP.Targets = append(j.CP.Targets, k)
			if err = e.enqueue(ctx, tx, j, k, ""); err != nil {
				return err
			}
		}
	}
	if j.CP.Old != "" && j.CP.New != "" && j.CP.Old != j.CP.New {
		j.Phase = "aliases"
	} else {
		j.Phase = "edges"
	}
	return nil
}

func (e *Engine) aliases(ctx context.Context, tx pgx.Tx, j *job) error {
	rows, err := e.page(ctx, tx, "alias", j.Position-1, j.CP.Cursor)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if r.Active && r.Ref == j.CP.Old && r.Key != j.CP.New {
			if err = e.put(ctx, tx, j, "alias", r.Key, j.CP.New, nil, true, j.CP.New); err != nil {
				return err
			}
		}
		j.CP.Cursor = r.Key
	}
	if len(rows) < e.Config.PageRows {
		if err = e.put(ctx, tx, j, "alias", j.CP.Old, j.CP.New, nil, true, j.CP.New); err != nil {
			return err
		}
		if err = e.put(ctx, tx, j, "alias", j.CP.New, "", nil, false, ""); err != nil {
			return err
		}
		j.Phase = "edges"
		j.CP.Cursor = ""
	}
	return nil
}

type edgeRecord struct {
	To string `json:"to"`
}

func (e *Engine) neighbors(ctx context.Context, q queryer, from string, pos int64, after string) ([]record, error) {
	rows, err := q.Query(ctx, `SELECT entity_key,payload,sha256,active FROM (SELECT DISTINCT ON(route_key,namespace,kind,entity_key) route_key,namespace,kind,entity_key,payload,sha256,active FROM wildfire_state WHERE namespace=$1 AND kind='edge' AND reference=$2 AND position<=$3 AND entity_key COLLATE "C">$4 COLLATE "C" ORDER BY route_key,namespace,kind,entity_key,position DESC) v ORDER BY entity_key COLLATE "C" LIMIT $5`, e.Config.Namespace, from, pos, after, e.Config.PageRows)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []record{}
	var total int64
	for rows.Next() {
		var r record
		var hash string
		if err = rows.Scan(&r.Key, &r.Payload, &hash, &r.Active); err != nil {
			return nil, err
		}
		total += int64(len(r.Payload) + len(r.Key) + 256)
		if total > e.Config.CacheBytes/2 {
			return nil, &Pending{"edge page quota"}
		}
		if rawinput.Hash(r.Payload) != hash {
			return nil, fmt.Errorf("edge integrity")
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
func (e *Engine) temporalCandidates(ctx context.Context, q queryer, pos int64, at time.Time, after string) ([]record, error) {
	// Acquisition time is immutable for an exact lexical key. Time filtering can
	// therefore be pushed before latest-version selection without reviving a row.
	rows, err := q.Query(ctx, `SELECT entity_key,payload,sha256 FROM (SELECT DISTINCT ON(route_key,namespace,kind,entity_key) route_key,namespace,kind,entity_key,payload,sha256,active FROM wildfire_state WHERE namespace=$1 AND kind='observation' AND position<=$2 AND event_at BETWEEN $3 AND $4 AND entity_key COLLATE "C">$5 COLLATE "C" ORDER BY route_key,namespace,kind,entity_key,position DESC) v WHERE active ORDER BY entity_key COLLATE "C" LIMIT $6`, e.Config.Namespace, pos, at.Add(-360*time.Minute), at.Add(360*time.Minute), after, e.Config.PageRows)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []record{}
	var total int64
	for rows.Next() {
		var r record
		var hash string
		if err = rows.Scan(&r.Key, &r.Payload, &hash); err != nil {
			return nil, err
		}
		total += int64(len(r.Payload) + len(r.Key) + 256)
		if total > e.Config.CacheBytes/2 {
			return nil, &Pending{"candidate page quota"}
		}
		if rawinput.Hash(r.Payload) != hash {
			return nil, fmt.Errorf("observation integrity")
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
func (e *Engine) edge(ctx context.Context, tx pgx.Tx, j *job, a, b string, active bool) error {
	if a == b {
		return nil
	}
	for _, pair := range [][2]string{{a, b}, {b, a}} {
		key := string(rawinput.Compact(pair[0], pair[1]))
		if err := e.put(ctx, tx, j, "edge", key, edgeRecord{pair[1]}, nil, active, pair[0]); err != nil {
			return err
		}
	}
	return nil
}
func (e *Engine) edges(ctx context.Context, tx pgx.Tx, j *job) error {
	if j.CP.Target >= len(j.CP.Targets) {
		j.Phase = "graph"
		j.CP.Cursor = ""
		j.CP.Kind = ""
		return nil
	}
	key := j.CP.Targets[j.CP.Target]
	o, err := e.observation(ctx, tx, key, j.Position)
	if errors.Is(err, pgx.ErrNoRows) {
		o.Key = key
	} else if err != nil {
		return err
	}
	if j.CP.Kind == "" {
		old, err := e.neighbors(ctx, tx, key, j.Position-1, j.CP.Cursor)
		if err != nil {
			return err
		}
		for _, r := range old {
			var link edgeRecord
			if err = json.Unmarshal(r.Payload, &link); err != nil {
				return err
			}
			other, find := e.observation(ctx, tx, link.To, j.Position)
			if find != nil && !errors.Is(find, pgx.ErrNoRows) {
				return find
			}
			connected := eligible(o) && eligible(other) && wildfire.Connected(o, other)
			if err = e.edge(ctx, tx, j, key, link.To, connected); err != nil {
				return err
			}
			if r.Active {
				if err = e.enqueue(ctx, tx, j, link.To, ""); err != nil {
					return err
				}
			}
			j.CP.Cursor = r.Key
		}
		if len(old) < e.Config.PageRows {
			j.CP.Kind = "candidate"
			j.CP.Cursor = ""
		}
		return nil
	}
	if !eligible(o) {
		j.CP.Target++
		j.CP.Kind = ""
		j.CP.Cursor = ""
		return nil
	}
	rows, err := e.temporalCandidates(ctx, tx, j.Position, *o.EventTime, j.CP.Cursor)
	if err != nil {
		return err
	}
	for _, r := range rows {
		var other wildfire.Observation
		if err = json.Unmarshal(r.Payload, &other); err != nil {
			return err
		}
		if other.Key != key && eligible(other) && wildfire.Connected(o, other) {
			if err = e.edge(ctx, tx, j, key, other.Key, true); err != nil {
				return err
			}
			if err = e.enqueue(ctx, tx, j, other.Key, ""); err != nil {
				return err
			}
		}
		j.CP.Cursor = r.Key
	}
	if len(rows) < e.Config.PageRows {
		j.CP.Target++
		j.CP.Kind = ""
		j.CP.Cursor = ""
	}
	return nil
}
func (e *Engine) graph(ctx context.Context, tx pgx.Tx, j *job) error {
	if j.CP.Root == "" {
		var key string
		err := tx.QueryRow(ctx, `SELECT node_key FROM wildfire_work WHERE namespace=$1 AND position=$2 AND component='' AND NOT done ORDER BY node_key COLLATE "C" LIMIT 1`, e.Config.Namespace, j.Position).Scan(&key)
		if errors.Is(err, pgx.ErrNoRows) {
			j.Phase = "resolve"
			j.CP.Cursor = ""
			return nil
		}
		if err != nil {
			return err
		}
		o, find := e.observation(ctx, tx, key, j.Position)
		if find != nil && !errors.Is(find, pgx.ErrNoRows) {
			return find
		}
		if !eligible(o) {
			_, err = tx.Exec(ctx, `UPDATE wildfire_work SET done=true WHERE route_key=wildfire_route($1,'work',$3) AND namespace=$1 AND position=$2 AND node_key=$3`, e.Config.Namespace, j.Position, key)
			return err
		}
		j.CP.Root = key
		return e.enqueue(ctx, tx, j, key, key)
	}
	if j.CP.Node == "" {
		err := tx.QueryRow(ctx, `SELECT node_key FROM wildfire_work WHERE namespace=$1 AND position=$2 AND component=$3 AND NOT done ORDER BY node_key COLLATE "C" LIMIT 1`, e.Config.Namespace, j.Position, j.CP.Root).Scan(&j.CP.Node)
		if errors.Is(err, pgx.ErrNoRows) {
			j.CP.Root = ""
			j.CP.Cursor = ""
			return nil
		}
		if err != nil {
			return err
		}
	}
	links, err := e.neighbors(ctx, tx, j.CP.Node, j.Position, j.CP.Cursor)
	if err != nil {
		return err
	}
	for _, r := range links {
		if r.Active {
			var link edgeRecord
			if err = json.Unmarshal(r.Payload, &link); err != nil {
				return err
			}
			o, find := e.observation(ctx, tx, link.To, j.Position)
			if find != nil && !errors.Is(find, pgx.ErrNoRows) {
				return find
			}
			if eligible(o) {
				if err = e.enqueue(ctx, tx, j, link.To, j.CP.Root); err != nil {
					return err
				}
			}
		}
		j.CP.Cursor = r.Key
	}
	if len(links) < e.Config.PageRows {
		_, err = tx.Exec(ctx, `UPDATE wildfire_work SET done=true WHERE route_key=wildfire_route($1,'work',$3) AND namespace=$1 AND position=$2 AND node_key=$3 AND component=$4`, e.Config.Namespace, j.Position, j.CP.Node, j.CP.Root)
		j.CP.Node = ""
		j.CP.Cursor = ""
	}
	return err
}
func componentOrder(groups map[string][]string, obs map[string]wildfire.Observation) [][]string {
	out := [][]string{}
	for _, keys := range groups {
		if len(keys) > 1 {
			sort.Strings(keys)
			out = append(out, keys)
		}
	}
	anchor := func(keys []string) wildfire.Observation {
		best := obs[keys[0]]
		for _, k := range keys[1:] {
			o := obs[k]
			if o.EventTime.Before(*best.EventTime) || (o.EventTime.Equal(*best.EventTime) && o.Key < best.Key) {
				best = o
			}
		}
		return best
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := anchor(out[i]), anchor(out[j])
		return a.EventTime.Before(*b.EventTime) || (a.EventTime.Equal(*b.EventTime) && a.Key < b.Key)
	})
	return out
}
