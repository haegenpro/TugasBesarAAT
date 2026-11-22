package ingest

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/coastal"
	"aggregator/internal/domain"
	"aggregator/internal/ownership"
	"aggregator/internal/platform"
	"aggregator/internal/store"
	"aggregator/internal/timeutil"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

//go:embed warning.avsc
var WarningSchema string

type WorkerConfig struct {
	Kind, NATSURL, CanonicalURL, CanonicalToken string
	Lateness                                    time.Duration
}

func (s *Store) RunWorker(ctx context.Context, cfg WorkerConfig, registry *avrowire.Client) {
	type lane struct{ stream, subject, durable string }
	lanes := []lane{}
	switch cfg.Kind {
	case "field":
		lanes = append(lanes, lane{"RAW_FIELD", "raw.field", "field-reports-v1"})
	case "wildfire":
		lanes = append(lanes, lane{"RAW_SATELLITE", "raw.satellite", "wildfire-v1"})
	case "coastal":
		lanes = append(lanes, lane{"RAW_CRITICAL", "raw.coastal.critical", "coastal-critical-v1"}, lane{"RAW_COASTAL", "raw.coastal.normal", "coastal-normal-v1"})
	default:
		panic("unknown worker kind")
	}
	var workers sync.WaitGroup
	for _, l := range lanes {
		l := l
		workers.Add(1)
		go func() {
			defer workers.Done()
			for ctx.Err() == nil {
				err := s.Leases.Run(ctx, "worker:"+l.durable, func(owned context.Context) error {
					nc, e := nats.Connect(cfg.NATSURL, nats.Timeout(time.Second))
					if e != nil {
						return e
					}
					defer nc.Close()
					js, e := jetstream.New(nc)
					if e != nil {
						return e
					}
					// Order is kept by the position checks, not by one-at-a-time delivery; only the
					// wildfire lane fetches several inputs at once.
					pending := 1
					var fire *fireBatcher
					if cfg.Kind == "wildfire" {
						fire = newFireBatcher(s)
						pending = fire.max
					}
					consumer, e := js.CreateOrUpdateConsumer(owned, l.stream, jetstream.ConsumerConfig{Durable: l.durable, FilterSubject: l.subject, AckPolicy: jetstream.AckExplicitPolicy, AckWait: 30 * time.Second, MaxAckPending: pending, Replicas: 3})
					if e != nil {
						return e
					}
					for owned.Err() == nil {
						if fire != nil {
							if e := fire.step(owned, consumer, registry, cfg.Lateness); e != nil {
								return e
							}
							continue
						}
						batch, e := consumer.Fetch(1, jetstream.FetchMaxWait(time.Second))
						if e != nil {
							return e
						}
						for msg := range batch.Messages() {
							processed := time.Now()
							call, cancel := context.WithTimeout(owned, 10*time.Second)
							call, span := platform.Start(platform.Extract(call, http.Header(msg.Headers())), "process raw input")
							i, e := decodeInput(call, registry, msg.Data())
							if e == nil {
								i, e = s.Input(call, i.ID)
							}
							if e == nil {
								e = s.NoteAttempt(call, i.ID)
							}
							if e == nil {
								switch cfg.Kind {
								case "field":
									e = s.ProcessReport(call, i)
								case "wildfire":
									e = s.prepareFire(call, i, cfg.Lateness)
								case "coastal":
									e = s.prepareCoastal(call, i, js, registry)
								}
							}
							if e == nil {
								acked := time.Now()
								e = msg.DoubleAck(call)
								platform.Observe("ingest_ack_seconds", time.Since(acked).Seconds())
								if e != nil {
									platform.Add("ingest_ack_failures_total", 1)
								}
							}
							if e == nil {
								inputLog(call, i, "raw input processed")
							}
							span.End()
							cancel()
							platform.Observe("ingest_stage_seconds_process_"+cfg.Kind, time.Since(processed).Seconds())
							batchStats("process_"+cfg.Kind, 1, len(msg.Data()))
							if e != nil {
								platform.Add(cfg.Kind+"_worker_retries_total", 1)
								_ = msg.NakWithDelay(time.Second)
								slog.Warn("worker retry", "service", cfg.Kind+"-processor", "correlation_id", i.CorrelationID, "error", e.Error())
							}
						}
						if e = batch.Error(); e != nil {
							return e
						}
					}
					return owned.Err()
				})
				if err != nil && !errors.Is(err, ownership.ErrBusy) {
					platform.Add("worker_reconnects_total", 1)
				}
				Sleep(ctx, time.Second)
			}
		}()
	}
	<-ctx.Done()
	workers.Wait()
}

func (s *Store) prepareCoastal(ctx context.Context, i Input, js jetstream.JetStream, registry *avrowire.Client) error {
	defer stage("prepare_coastal")()
	// Endpoint lanes are independent, but entity writes and warning identities
	// serialize through PostgreSQL, so a late ordinary record cannot undo a newer
	// critical correction of the same observation.
	tx, e := s.begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if e = ownership.Guard(ctx, tx); e != nil {
		return e
	}
	locked := time.Now()
	_, e = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,6))`, i.Namespace+":"+i.Entity)
	lockWait("entity_advisory", locked)
	if e != nil {
		return e
	}
	var state string
	if e = tx.QueryRow(ctx, `SELECT state FROM raw_inputs WHERE message_id=$1 FOR UPDATE`, i.ID).Scan(&state); e != nil {
		return e
	}
	if state != "pending" {
		return commit(ctx, tx, "prepare_coastal")
	}
	if waiting, e := blocked(ctx, tx, i); e != nil {
		return e
	} else if waiting {
		return parkInput(ctx, tx, i, "blocked by an earlier unprocessed change of this entity")
	}
	var clock *time.Time
	if text, _ := i.Metadata["virtual_now"].(string); text != "" {
		if at, err := time.Parse(time.RFC3339Nano, text); err == nil {
			clock = &at
		}
	}
	obs, invalid := coastal.Parse(i.Payload, clock, 5*time.Second)
	var station Station
	if invalid == nil {
		var body []byte
		e = tx.QueryRow(ctx, `SELECT payload FROM coastal_stations WHERE station_id=$1`, obs.StationID).Scan(&body)
		if errors.Is(e, pgx.ErrNoRows) {
			return errors.New("waiting for independently refreshed station metadata")
		}
		if e != nil {
			return e
		}
		if e = json.Unmarshal(body, &station); e != nil {
			return e
		}
	}
	if invalid != nil {
		return parkInput(ctx, tx, i, invalid.Error())
	}
	// Warning publication precedes canonical work and requires only the ingestion
	// database and its own R3 stream. The expectation already exists durably.
	if obs.Critical {
		schema, e := registry.Register(ctx, "coastal-warning-value", WarningSchema)
		if e != nil {
			return e
		}
		warning := map[string]any{"warning_version": int32(1), "notification_id": WarningID(i), "recipient": "TIM_LAPANGAN", "source": "SENSOR_PANTAI", "source_namespace": i.Namespace, "observation_id": obs.ID, "operation_id": "coastal-op-v1:" + i.ID, "operation_position": i.Position, "operation": "create", "observed_at": obs.ObservedAt.UTC().Format(time.RFC3339Nano), "first_received_at": i.ReceivedAt.UTC().Format(time.RFC3339Nano), "critical": true, "station_id": station.ID, "province": station.Province, "latitude": station.Latitude, "longitude": station.Longitude, "sea_level_anomaly_m": obs.SeaLevel, "wave_height_m": obs.WaveHeight, "quality": obs.Quality, "payload_sha256": Hash(i.Payload), "correlation_id": i.CorrelationID, "traceparent": nil, "hazard_id": nil}
		frame, e := avrowire.Encode(schema, warning)
		if e != nil {
			return e
		}
		msg := nats.NewMsg("coastal.warning")
		msg.Data = frame
		msg.Header.Set("Nats-Msg-Id", WarningID(i))
		msg.Header.Set("X-Correlation-ID", i.CorrelationID)
		platform.Inject(ctx, http.Header(msg.Header))
		published := time.Now()
		_, e = js.PublishMsg(ctx, msg)
		platform.Observe("ingest_publish_seconds_warning", time.Since(published).Seconds())
		if e != nil {
			return e
		}
	}
	var previous int64
	e = tx.QueryRow(ctx, `SELECT position FROM coastal_state WHERE entity_key=$1`, i.Namespace+":"+i.Entity).Scan(&previous)
	if errors.Is(e, pgx.ErrNoRows) {
		previous = -1
	} else if e != nil {
		return e
	}
	if i.Position <= previous {
		if _, e = tx.Exec(ctx, `UPDATE raw_inputs SET state='applied' WHERE message_id=$1`, i.ID); e != nil {
			return e
		}
		if e = frontiers(ctx, tx, i.Source, i.Namespace); e != nil {
			return e
		}
		if e = releaseEntity(ctx, tx, i); e != nil {
			return e
		}
		return commit(ctx, tx, "prepare_coastal")
	}
	severity := domain.SeverityNormal
	if obs.Critical {
		severity = domain.SeverityAwas
	}
	h := domain.HazardEvent{Source: domain.SourceSensorPantai, SourceRefID: obs.ID, HazardType: domain.HazardCoastal, Severity: severity, AreaName: station.Province, Latitude: station.Latitude, Longitude: station.Longitude, OccurredAt: timeutil.New(obs.ObservedAt), Attributes: map[string]any{"station_id": station.ID, "station_name": station.Name, "sea_level_anomaly_m": obs.SeaLevel, "wave_height_m": obs.WaveHeight, "quality": obs.Quality, "critical": obs.Critical}}
	batch := store.CanonicalBatch{ID: "coastal:" + i.ID, Source: i.Source, Namespace: i.Namespace + ":" + i.Entity, Previous: previous, Position: i.Position, Changes: []store.CanonicalChange{{Ref: obs.ID, Event: &h}}}
	if e = queueCanonical(ctx, tx, i, batch); e != nil {
		return e
	}
	body, _ := json.Marshal(obs)
	if _, e = tx.Exec(ctx, `INSERT INTO coastal_state(entity_key,position,payload) VALUES($1,$2,$3) ON CONFLICT(entity_key) DO UPDATE SET position=excluded.position,payload=excluded.payload`, i.Namespace+":"+i.Entity, i.Position, body); e != nil {
		return e
	}
	if e = releaseEntity(ctx, tx, i); e != nil {
		return e
	}
	if e = frontiers(ctx, tx, i.Source, i.Namespace); e != nil {
		return e
	}
	return commit(ctx, tx, "prepare_coastal")
}
func parkInput(ctx context.Context, tx pgx.Tx, i Input, reason string) error {
	if len(reason) > 1024 {
		reason = string([]rune(reason)[:min(len([]rune(reason)), 256)])
	}
	if i.Entity != "" {
		if _, e := tx.Exec(ctx, `INSERT INTO blocked_entities(source,namespace,entity_key,message_id) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, i.Source, i.Namespace, i.Entity, i.ID); e != nil {
			return e
		}
	}
	if _, e := tx.Exec(ctx, `UPDATE raw_inputs SET state='parked',reason=$2 WHERE message_id=$1`, i.ID, reason); e != nil {
		return e
	}
	if _, e := tx.Exec(ctx, `UPDATE warning_expectations SET rejected=true WHERE message_id=$1`, i.ID); e != nil {
		return e
	}
	if e := frontiers(ctx, tx, i.Source, i.Namespace); e != nil {
		return e
	}
	return commit(ctx, tx, "park")
}
func queueCanonical(ctx context.Context, tx pgx.Tx, i Input, b store.CanonicalBatch) error {
	metadata, e := json.Marshal(traceMetadata(ctx, i.Metadata))
	if e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `UPDATE raw_inputs SET metadata=$2 WHERE message_id=$1`, i.ID, metadata); e != nil {
		return e
	}
	if len(b.Changes) == 0 {
		_, e := tx.Exec(ctx, `UPDATE raw_inputs SET state='applied' WHERE message_id=$1`, i.ID)
		return e
	}
	if i.Source == "SATELIT" {
		var output int64
		if e := tx.QueryRow(ctx, `UPDATE source_progress SET output_position=output_position+1 WHERE source=$1 AND namespace=$2 RETURNING output_position`, i.Source, i.Namespace).Scan(&output); e != nil {
			return e
		}
		b.Previous = output - 1
		b.Position = output
		b.Namespace = i.Namespace + ":wildfire-output"
	}
	raw, e := json.Marshal(b)
	if e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO canonical_outbox(operation_id,message_id,source,namespace,position,payload) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, b.ID, i.ID, i.Source, b.Namespace, b.Position, raw); e != nil {
		return e
	}
	_, e = tx.Exec(ctx, `UPDATE raw_inputs SET state='prepared' WHERE message_id=$1`, i.ID)
	return e
}
func frontiers(ctx context.Context, tx pgx.Tx, source, ns string) error {
	_, e := tx.Exec(ctx, `UPDATE source_progress p SET prepared=GREATEST(prepared,coalesce((SELECT min(position)-1 FROM raw_inputs i WHERE i.source=p.source AND i.namespace=p.namespace AND i.state='pending'),received)),applied=GREATEST(applied,coalesce((SELECT min(position)-1 FROM raw_inputs i WHERE i.source=p.source AND i.namespace=p.namespace AND i.state IN ('pending','prepared')),received)) WHERE source=$1 AND namespace=$2`, source, ns)
	return e
}
