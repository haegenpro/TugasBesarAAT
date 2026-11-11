package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type ReadRow struct {
	ID       string    `json:"hazard_id"`
	Ingested time.Time `json:"ingested_at"`
	Active   bool      `json:"active"`
	Revision int64     `json:"revision"`
}
type PartialPage struct {
	Items       []ReadRow `json:"items"`
	Partial     bool      `json:"partial"`
	Unavailable []string  `json:"unavailable_units"`
	NextCursor  *string   `json:"next_cursor"`
}

func (l *Lab) unitRows(ctx context.Context, service string, shards []Shard) ([]ReadRow, error) {
	c, err := l.connect(ctx, service)
	if err != nil {
		return nil, err
	}
	defer c.Close(context.Background())
	var out []ReadRow
	for _, s := range shards {
		if s.Service != service {
			continue
		}
		rows, e := c.Query(ctx, "SELECT hazard_id::text,ingested_at,active,revision FROM "+shardTable("hazards", s.ID)+" ORDER BY ingested_at,hazard_id LIMIT 100")
		if e != nil {
			return nil, e
		}
		for rows.Next() {
			var r ReadRow
			if e = rows.Scan(&r.ID, &r.Ingested, &r.Active, &r.Revision); e != nil {
				rows.Close()
				return nil, e
			}
			r.Ingested = r.Ingested.UTC()
			out = append(out, r)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return nil, e
		}
	}
	return out, nil
}
func (l *Lab) partialRead(ctx context.Context, shards []Shard) (PartialPage, int) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	page := PartialPage{Items: []ReadRow{}, Unavailable: []string{}}
	var mu sync.Mutex
	var wg sync.WaitGroup
	var rows []ReadRow
	services := map[string]bool{}
	for _, s := range shards {
		services[s.Service] = true
	}
	for service := range services {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			found, err := l.unitRows(ctx, name, shards)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				page.Unavailable = append(page.Unavailable, name)
				return
			}
			rows = append(rows, found...)
		}(service)
	}
	wg.Wait()
	sort.Strings(page.Unavailable)
	if len(page.Unavailable) > 0 {
		page.Partial = true
		cursor := base64.RawURLEncoding.EncodeToString([]byte(`{"v":1,"after":null,"scope":"m1-fixture"}`))
		page.NextCursor = &cursor
		return page, len(rows)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Ingested.Equal(rows[j].Ingested) {
			return rows[i].ID < rows[j].ID
		}
		return rows[i].Ingested.Before(rows[j].Ingested)
	})
	page.Items = rows
	return page, len(rows)
}
func (l *Lab) degraded(ctx context.Context, d map[string]any) error {
	seed, err := l.reset(ctx)
	if err != nil {
		return err
	}
	shards, err := l.shards(ctx)
	if err != nil {
		return err
	}
	missing := seed[1].Service
	if err = l.fault(ctx, "stop", missing); err != nil {
		return err
	}
	defer func() {
		r, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = l.fault(r, "start", missing)
	}()
	c, err := l.connect(ctx, "coord")
	if err != nil {
		return err
	}
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	start := time.Now()
	var count int
	queryErr := c.QueryRow(queryCtx, "SELECT count(*) FROM hazards").Scan(&count)
	d["distributed_query_ms"] = time.Since(start).Milliseconds()
	cancel()
	c.Close(context.Background())
	if queryErr == nil {
		return errors.New("distributed query unexpectedly succeeded without a required worker")
	}
	d["distributed_query_error"] = queryErr.Error()
	start = time.Now()
	page, available := l.partialRead(ctx, shards)
	elapsed := time.Since(start)
	d["fallback_ms"] = elapsed.Milliseconds()
	d["fallback_page"] = page
	d["available_rows_not_emitted_without_lower_bound"] = available
	if !page.Partial || len(page.Items) != 0 || page.NextCursor == nil || len(page.Unavailable) != 1 || page.Unavailable[0] != missing || available != 1 {
		return fmt.Errorf("incorrect conservative partial result: %+v, available %d", page, available)
	}
	if elapsed > 2250*time.Millisecond {
		return fmt.Errorf("two-second read deadline exceeded scheduling allowance: %s", elapsed)
	}
	if err = l.fault(ctx, "start", missing); err != nil {
		return err
	}
	ready, stop := context.WithTimeout(ctx, 45*time.Second)
	defer stop()
	err = until(ready, func() error {
		c, e := l.connect(ready, missing)
		if e != nil {
			return e
		}
		defer c.Close(context.Background())
		if e = c.Ping(ready); e != nil {
			return e
		}
		coordinator, e := l.connect(ready, "coord")
		if e != nil {
			return e
		}
		defer coordinator.Close(context.Background())
		var n int
		e = coordinator.QueryRow(ready, "SELECT count(*) FROM hazards").Scan(&n)
		if e != nil {
			return e
		}
		if n != 2 {
			return errors.New("distributed recovery count mismatch")
		}
		return nil
	})
	if err != nil {
		return err
	}
	complete, n := l.partialRead(ctx, shards)
	d["recovered_page"] = complete
	if complete.Partial || n != 2 || complete.NextCursor != nil || complete.Items[0].ID == complete.Items[1].ID {
		return errors.New("recovered traversal is incomplete or duplicated")
	}
	return nil
}
