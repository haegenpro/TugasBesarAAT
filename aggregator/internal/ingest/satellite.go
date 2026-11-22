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
	Batch                 BatchLimits
}

// BatchLimits bound one acquisition transaction by record count, payload bytes
// and the time the oldest buffered record may wait. Records=1 is the
// batch-size-one comparison mode.
type BatchLimits struct {
	Records int
	Bytes   int
	Wait    time.Duration
}

// BatchLimitsFromEnv reads SATELLITE_BATCH_RECORDS, SATELLITE_BATCH_BYTES and
// SATELLITE_BATCH_WAIT_MS.
func BatchLimitsFromEnv() BatchLimits {
	return BatchLimits{Records: platform.Int("SATELLITE_BATCH_RECORDS", 100), Bytes: platform.Int("SATELLITE_BATCH_BYTES", 2<<20),
		Wait: platform.Milliseconds("SATELLITE_BATCH_WAIT_MS", 100)}
}

func (l BatchLimits) normalized() BatchLimits {
	defaults := BatchLimitsFromEnv()
	if l.Records <= 0 {
		l.Records = defaults.Records
	}
	if l.Bytes <= 0 {
		l.Bytes = defaults.Bytes
	}
	if l.Wait <= 0 {
		l.Wait = defaults.Wait
	}
	return l
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

// rejection is an unreadable frame that is retained as evidence, after every
// record that preceded it is durable, and then stops the session.
type rejection struct {
	body   io.Reader
	reason string
	err    error
}

// queued is what the reader hands to the writer: one record, or a terminal rejection or error.
type queued struct {
	input    Input
	enqueued time.Time
	end      *rejection
}

func (s *Store) satelliteSession(ctx context.Context, cfg SatelliteConfig) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	limits := cfg.Batch.normalized()
	// The durable position is the only resume point. It moves only after a commit,
	// so after an interruption or an ambiguous commit it is reloaded from storage
	// and the stable record identities make the retry idempotent.
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

	// The reader keeps draining the connection while the writer commits, through a
	// bounded buffer, so a slow commit cannot stall the feed past its idle limit.
	queue := make(chan queued, 2*limits.Records)
	send := func(item queued) bool {
		select {
		case queue <- item:
			return true
		case <-ctx.Done():
			return false
		}
	}
	go func() {
		defer close(queue)
		next := received + 1
		var lastHealth time.Time
		finish := func(r rejection) { send(queued{end: &r}) }
		for ctx.Err() == nil {
			event, e := stream.Next()
			if e != nil {
				var rejected *satellite.RejectedFrame
				if errors.As(e, &rejected) {
					finish(rejection{body: rejected.Reader, reason: e.Error(), err: e})
				} else {
					finish(rejection{err: e})
				}
				return
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
				finish(rejection{body: bytes.NewReader(event.Data), reason: fmt.Sprint(normErr), err: normErr})
				return
			}
			if n.Offset < next {
				continue
			}
			if n.Offset != next {
				finish(rejection{body: bytes.NewReader(event.Data), reason: ErrGap.Error(), err: ErrGap})
				return
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
			if !send(queued{input: i, enqueued: time.Now()}) {
				return
			}
			next++
		}
	}()

	batch := make([]Input, 0, limits.Records)
	enqueued := make([]time.Time, 0, limits.Records)
	size := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if e := s.AppendBatch(ctx, batch, cfg.Initial, "raw.satellite"); e != nil {
			return e
		}
		received = batch[len(batch)-1].Position
		now := time.Now()
		for _, at := range enqueued {
			platform.Observe("ingest_acquire_to_commit_seconds", now.Sub(at).Seconds())
		}
		platform.Gauge("satellite_received_position", float64(received))
		batch, enqueued, size = batch[:0], enqueued[:0], 0
		return nil
	}
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		var due <-chan time.Time
		if len(batch) > 0 {
			due = timer.C
		}
		select {
		case item, ok := <-queue:
			if !ok {
				if e := flush(); e != nil {
					return e
				}
				return ctx.Err()
			}
			if item.end != nil {
				if e := flush(); e != nil {
					return e
				}
				if item.end.body != nil {
					if retained := s.RetainResponse(ctx, "SATELIT", cfg.Namespace, "stream:"+strconv.FormatInt(received+1, 10), item.end.body, item.end.reason); retained != nil {
						return retained
					}
				}
				return item.end.err
			}
			if len(batch) == 0 {
				timer.Reset(limits.Wait)
			}
			batch = append(batch, item.input)
			enqueued = append(enqueued, item.enqueued)
			size += len(item.input.Payload)
			if len(batch) >= limits.Records || size >= limits.Bytes {
				if e := flush(); e != nil {
					return e
				}
			}
		case <-due:
			if e := flush(); e != nil {
				return e
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
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
