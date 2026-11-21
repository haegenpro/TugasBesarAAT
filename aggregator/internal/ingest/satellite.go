package ingest

import (
	"aggregator/internal/platform"
	"aggregator/internal/satellite"
	"aggregator/internal/wildfire"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type SatelliteConfig struct {
	URL, Token, Namespace string
	Initial               int64
	Exclusive             bool
}

func (s *Store) RunSatellite(ctx context.Context, cfg SatelliteConfig) {
	for ctx.Err() == nil {
		e := s.Leases.Run(ctx, "acquire:SATELIT:"+cfg.Namespace, func(c context.Context) error {
			err := s.satelliteSession(c, cfg)
			if err != nil {
				s.RecordSourceHealth(c, "satellite", false, "feed disconnected; resuming original offsets")
			}
			return err
		})
		if e != nil {
			platform.Add("satellite_reconnects_total", 1)
		}
		Sleep(ctx, time.Second)
	}
}
func (s *Store) satelliteSession(ctx context.Context, cfg SatelliteConfig) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	received, _, _, e := s.Progress(ctx, "SATELIT", cfg.Namespace, cfg.Initial)
	if e != nil {
		return e
	}
	from := received + 1
	if cfg.Exclusive {
		from--
	}
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(cfg.URL, "/")+"/v1/stream?from="+strconv.FormatInt(from, 10), nil)
	if e != nil {
		return e
	}
	req.Header.Set("Authorization", "Bearer "+cfg.Token)
	req.Header.Set("Accept", "text/event-stream")
	client := platform.Client(0)
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, e := client.Do(req)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("satellite stream returned %d", resp.StatusCode)
	}
	idleTimeout := platform.Milliseconds("SATELLITE_IDLE_TIMEOUT_MS", 20000)
	idle := time.AfterFunc(idleTimeout, cancel)
	defer idle.Stop()
	stream := satellite.NewStream(&streamActivity{Reader: resp.Body, timer: idle, timeout: idleTimeout}, platform.Int("MAX_PAYLOAD_BYTES", 4<<20))
	var lastHealth time.Time
	for ctx.Err() == nil {
		event, e := stream.Next()
		if e != nil {
			var rejected *satellite.RejectedFrame
			if errors.As(e, &rejected) {
				if retained := s.RetainResponse(ctx, "SATELIT", cfg.Namespace, "stream:"+strconv.FormatInt(received+1, 10), rejected.Reader, e.Error()); retained != nil {
					return retained
				}
			}
			return e
		}
		if time.Since(lastHealth) > time.Second {
			s.RecordSourceHealth(ctx, "satellite", true, "")
			lastHealth = time.Now()
		}
		n, normErr := satellite.Normalize(cfg.Namespace, event)
		if n.Offset < 0 {
			if event.Type == "heartbeat" {
				continue
			}
			if retained := s.RetainResponse(ctx, "SATELIT", cfg.Namespace, "stream:"+strconv.FormatInt(received+1, 10), bytes.NewReader(event.Data), fmt.Sprint(normErr)); retained != nil {
				return retained
			}
			return normErr
		}
		if n.Offset <= received {
			continue
		}
		if n.Offset != received+1 {
			if retained := s.RetainResponse(ctx, "SATELIT", cfg.Namespace, "stream:"+strconv.FormatInt(received+1, 10), bytes.NewReader(event.Data), ErrGap.Error()); retained != nil {
				return retained
			}
			return ErrGap
		}
		metadata := map[string]any{"event_id": event.ID, "event_type": event.Type}
		if n.VirtualNow != nil {
			metadata["virtual_now"] = n.VirtualNow.UTC().Format(time.RFC3339Nano)
		}
		if normErr != nil {
			metadata["rejection"] = normErr.Error()
		}
		entity := ""
		if normErr == nil {
			var op wildfire.Operation
			if json.Unmarshal(n.Operation, &op) == nil {
				entity = op.TargetKey
				if entity == "" && op.Detection != nil {
					entity = strings.Join([]string{op.Detection.Satellite, op.Detection.AcqDate, op.Detection.AcqTime, op.Detection.Latitude, op.Detection.Longitude}, "|")
				}
			}
		}
		i := Input{Source: "SATELIT", Namespace: cfg.Namespace, Position: n.Offset, Entity: entity, Payload: event.Data, Metadata: metadata, CorrelationID: uuid.NewString()}
		if e = s.Append(ctx, i, cfg.Initial, "raw.satellite"); e != nil {
			return e
		}
		received = n.Offset
		platform.Gauge("satellite_received_position", float64(received))
	}
	return ctx.Err()
}

type streamActivity struct {
	io.Reader
	timer   *time.Timer
	timeout time.Duration
}

func (r *streamActivity) Read(p []byte) (int, error) {
	n, e := r.Reader.Read(p)
	if n > 0 {
		r.timer.Reset(r.timeout)
	}
	return n, e
}
