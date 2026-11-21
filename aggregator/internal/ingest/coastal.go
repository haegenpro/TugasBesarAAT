package ingest

import (
	"aggregator/internal/coastal"
	"aggregator/internal/ownership"
	"aggregator/internal/platform"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"io"
	"net/http"
	"net/url"
	"time"
)

type Station struct {
	ID        string  `json:"station_id"`
	Name      string  `json:"name"`
	Province  string  `json:"province"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}
type CoastalConfig struct {
	URL, Token, Namespace string
	Rate                  float64
}
type coastalClient struct {
	cfg                    CoastalConfig
	http                   *http.Client
	limiter                coastal.Limiter
	observations, stations coastal.Breaker
	s                      *Store
}

func (c *coastalClient) get(ctx context.Context, path string, b *coastal.Breaker, out any) error {
	if e := b.Allow(); e != nil {
		return e
	}
	if e := c.limiter.Wait(ctx); e != nil {
		b.Release()
		return e
	}
	call, cancel := context.WithTimeout(ctx, 1200*time.Millisecond)
	defer cancel()
	req, e := http.NewRequestWithContext(call, "GET", c.cfg.URL+path, nil)
	if e != nil {
		b.Release()
		return e
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.Token)
	started := time.Now()
	resp, e := c.http.Do(req)
	platform.Observe("coastal_"+b.Name+"_request_seconds", time.Since(started).Seconds())
	if e != nil {
		b.Failure()
		c.health(ctx, b.Name, false, "upstream request failed")
		return e
	}
	defer resp.Body.Close()
	body, e := io.ReadAll(io.LimitReader(resp.Body, (4<<20)+1))
	if e == nil && resp.StatusCode == 200 && len(body) <= 4<<20 {
		e = platform.ValidateJSON(body)
		if e == nil {
			e = json.Unmarshal(body, out)
		}
	} else if e == nil {
		e = fmt.Errorf("sensor HTTP %d or oversized body", resp.StatusCode)
	}
	if e != nil {
		b.Failure()
		c.health(ctx, b.Name, false, e.Error())
		return e
	}
	b.Success()
	c.health(ctx, b.Name, true, "")
	return nil
}
func (c *coastalClient) health(ctx context.Context, endpoint string, success bool, reason string) {
	state := "stale"
	if success {
		state = "fresh"
	}
	tx, e := c.s.Pool.Begin(ctx)
	if e != nil {
		return
	}
	defer tx.Rollback(ctx)
	if e = ownership.Guard(ctx, tx); e != nil {
		return
	}
	if _, e = tx.Exec(ctx, `INSERT INTO source_health(endpoint,state,last_success,reason) VALUES($1,$2,CASE WHEN $3 THEN clock_timestamp() ELSE NULL END,$4) ON CONFLICT(endpoint) DO UPDATE SET state=excluded.state,last_success=coalesce(excluded.last_success,source_health.last_success),last_attempt=clock_timestamp(),reason=excluded.reason`, "coastal:"+endpoint, state, success, reason); e == nil {
		_ = tx.Commit(ctx)
	}
}
func (s *Store) RunCoastal(ctx context.Context, cfg CoastalConfig) {
	if cfg.Rate <= 0 {
		cfg.Rate = 4
	}
	c := &coastalClient{cfg: cfg, http: platform.Client(2 * time.Second), limiter: coastal.Limiter{Rate: cfg.Rate, Burst: 1}, observations: coastal.Breaker{Name: "observations", Threshold: 3, Cooldown: 2 * time.Second, MaxCooldown: 15 * time.Second}, stations: coastal.Breaker{Name: "stations", Threshold: 3, Cooldown: 2 * time.Second, MaxCooldown: 15 * time.Second}, s: s}
	c.http.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	// One owner covers both endpoints, so the shared limiter remains a source-wide
	// bound rather than multiplying upstream traffic by the replica count.
	for ctx.Err() == nil {
		_ = s.Leases.Run(ctx, "acquire:SENSOR_PANTAI:"+cfg.Namespace, func(owned context.Context) error {
			stationDone := make(chan struct{})
			stationCtx, stop := context.WithCancel(owned)
			go func() {
				defer close(stationDone)
				for stationCtx.Err() == nil {
					c.refreshStations(stationCtx)
					Sleep(stationCtx, 30*time.Second)
				}
			}()
			defer func() { stop(); <-stationDone }()
			for owned.Err() == nil {
				if e := c.acquire(owned); e != nil {
					platform.Add("coastal_fetch_failures_total", 1)
				}
				Sleep(owned, 250*time.Millisecond)
			}
			return owned.Err()
		})
		Sleep(ctx, time.Second)
	}
}
func (c *coastalClient) refreshStations(ctx context.Context) {
	var page struct {
		Items []Station `json:"items"`
	}
	if c.get(ctx, "/v1/stations", &c.stations, &page) != nil || page.Items == nil {
		return
	}
	tx, e := c.s.Pool.Begin(ctx)
	if e != nil {
		return
	}
	defer tx.Rollback(ctx)
	if e = ownership.Guard(ctx, tx); e != nil {
		return
	}
	for _, station := range page.Items {
		if station.ID == "" || station.Province == "" || station.Latitude < -90 || station.Latitude > 90 || station.Longitude < -180 || station.Longitude > 180 {
			return
		}
		b, _ := json.Marshal(station)
		if _, e = tx.Exec(ctx, `INSERT INTO coastal_stations(station_id,payload) VALUES($1,$2) ON CONFLICT(station_id) DO UPDATE SET payload=excluded.payload,refreshed_at=clock_timestamp()`, station.ID, b); e != nil {
			return
		}
	}
	_ = tx.Commit(ctx)
}
func (c *coastalClient) acquire(ctx context.Context) error {
	var cursor *string
	e := c.s.Pool.QueryRow(ctx, `SELECT value FROM source_cursors WHERE source='SENSOR_PANTAI' AND namespace=$1`, c.cfg.Namespace).Scan(&cursor)
	if e != nil && !errors.Is(e, pgx.ErrNoRows) {
		return e
	}
	q := url.Values{"limit": {"100"}}
	if cursor != nil {
		q.Set("cursor", *cursor)
	}
	var page struct {
		Generated string            `json:"generated_at"`
		Items     []json.RawMessage `json:"items"`
		Next      *string           `json:"next_cursor"`
	}
	if e = c.get(ctx, "/v1/observations?"+q.Encode(), &c.observations, &page); e != nil {
		return e
	}
	if page.Items == nil {
		return errors.New("observations missing items")
	}
	received, _, _, e := c.s.Progress(ctx, "SENSOR_PANTAI", c.cfg.Namespace, 0)
	if e != nil {
		return e
	}
	var latest *time.Time
	for _, body := range page.Items {
		var timestamp struct {
			Observed string `json:"observed_at"`
		}
		if json.Unmarshal(body, &timestamp) == nil {
			if at, err := time.Parse(time.RFC3339Nano, timestamp.Observed); err == nil && (latest == nil || at.After(*latest)) {
				latest = &at
			}
		}
		id := uuid.NewSHA1(uuid.NameSpaceOID, []byte(c.cfg.Namespace+":"+Hash(body))).String()
		var exists bool
		if e = c.s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM raw_inputs WHERE message_id=$1)`, id).Scan(&exists); e != nil {
			return e
		}
		if exists {
			continue
		}
		var probe struct {
			ID       string `json:"observation_id"`
			Critical bool   `json:"critical"`
		}
		_ = json.Unmarshal(body, &probe)
		entity := probe.ID
		if entity == "" {
			entity = "invalid:" + Hash(body)
		}
		subject := "raw.coastal.normal"
		if probe.Critical {
			subject = "raw.coastal.critical"
		}
		i := Input{ID: id, Source: "SENSOR_PANTAI", Namespace: c.cfg.Namespace, Position: received + 1, Entity: entity, Payload: bytes.Clone(body), Metadata: map[string]any{"critical": probe.Critical, "virtual_now": page.Generated}, CorrelationID: uuid.NewString()}
		if e = c.s.Append(ctx, i, 0, subject); e != nil {
			return e
		}
		received++
	}
	// Keep the last request cursor when next_cursor=null. Re-reading this final
	// page supplies overlap while allowing newly appended observations to appear.
	if page.Next != nil {
		cursor = page.Next
	}
	tx, e := c.s.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if e = ownership.Guard(ctx, tx); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `UPDATE source_health SET last_new=(SELECT received_at FROM raw_inputs WHERE source='SENSOR_PANTAI' AND namespace=$1 ORDER BY position DESC LIMIT 1),latest_observed=GREATEST(latest_observed,$3),source_generated=$2::timestamptz WHERE endpoint='coastal:observations'`, c.cfg.Namespace, nullableTimestamp(page.Generated), latest); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO source_cursors(source,namespace,value) VALUES('SENSOR_PANTAI',$1,$2) ON CONFLICT(source,namespace) DO UPDATE SET value=excluded.value`, c.cfg.Namespace, cursor); e != nil {
		return e
	}
	return tx.Commit(ctx)
}

func nullableTimestamp(value string) any {
	if at, e := time.Parse(time.RFC3339Nano, value); e == nil {
		return at
	}
	return nil
}
