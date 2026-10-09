package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var errUnknownSchema = errors.New("schema ID not found")
var errRegistryUnavailable = errors.New("schema registry unavailable")

type config struct {
	Role, Port, DBPath, NATSURL, Stream, Subject, Durable, RegistryURL, DLQStream, DLQSubject string
	LifecycleStream, LifecycleSubject                                                         string
}

func loadConfig() (config, error) {
	values, err := requiredEnv("CONSUMER_ROLE", "CONSUMER_PORT", "CONSUMER_DB_PATH", "NATS_URL", "NATS_STREAM", "NATS_SUBJECT", "NATS_DURABLE", "SCHEMA_REGISTRY_URL", "NATS_DLQ_STREAM", "NATS_DLQ_SUBJECT", "NATS_LIFECYCLE_STREAM", "NATS_LIFECYCLE_SUBJECT")
	if err != nil {
		return config{}, err
	}
	if _, err = envInt(values, "CONSUMER_PORT", 1, 65535); err != nil {
		return config{}, err
	}
	if err = envURL(values, "NATS_URL", "nats", "tls", "ws", "wss"); err != nil {
		return config{}, err
	}
	if err = envURL(values, "SCHEMA_REGISTRY_URL", "http", "https"); err != nil {
		return config{}, err
	}
	if values["CONSUMER_ROLE"] != "dashboard" && values["CONSUMER_ROLE"] != "notifier" {
		return config{}, errors.New("CONSUMER_ROLE must be dashboard or notifier")
	}
	lifecycleStream, lifecycleSubject := values["NATS_LIFECYCLE_STREAM"], values["NATS_LIFECYCLE_SUBJECT"]
	return config{LifecycleStream: lifecycleStream, LifecycleSubject: lifecycleSubject, Role: values["CONSUMER_ROLE"], Port: values["CONSUMER_PORT"], DBPath: values["CONSUMER_DB_PATH"], NATSURL: values["NATS_URL"], Stream: values["NATS_STREAM"], Subject: values["NATS_SUBJECT"], Durable: values["NATS_DURABLE"], RegistryURL: strings.TrimRight(values["SCHEMA_REGISTRY_URL"], "/"), DLQStream: values["NATS_DLQ_STREAM"], DLQSubject: values["NATS_DLQ_SUBJECT"]}, nil
}

type schemaClient struct {
	base   string
	db     *sql.DB
	http   *http.Client
	mu     sync.RWMutex
	codecs map[int]*goavro.Codec
}

func newSchemaClient(base string, db *sql.DB) *schemaClient {
	return &schemaClient{base: base, db: db, http: &http.Client{Timeout: 2 * time.Second}, codecs: map[int]*goavro.Codec{}}
}

func (s *schemaClient) codec(ctx context.Context, id int) (*goavro.Codec, error) {
	s.mu.RLock()
	cached := s.codecs[id]
	s.mu.RUnlock()
	if cached != nil {
		return cached, nil
	}
	var schema string
	err := s.db.QueryRowContext(ctx, `SELECT schema FROM schema_cache WHERE registry=? AND id=?`, s.base, id).Scan(&schema)
	if err == nil {
		codec, codecErr := goavro.NewCodec(schema)
		if codecErr != nil {
			return nil, fmt.Errorf("%w: cached schema %d: %v", errConsumerIncompatible, id, codecErr)
		}
		s.mu.Lock()
		s.codecs[id] = codec
		s.mu.Unlock()
		return codec, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: read schema cache: %w", errLocalStore, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.base+"/schemas/ids/"+strconv.Itoa(id), nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errRegistryUnavailable, err)
	}
	if cid := correlationFromContext(ctx); cid != "" {
		req.Header.Set(correlationHeader, cid)
	}
	start := time.Now()
	resp, err := s.http.Do(req)
	if err != nil {
		logOutbound(ctx, "registry_lookup", "schema-registry", 0, start, err)
		return nil, fmt.Errorf("%w: %v", errRegistryUnavailable, err)
	}
	defer resp.Body.Close()
	var lookupErr error
	if resp.StatusCode >= 400 {
		lookupErr = fmt.Errorf("registry status %d", resp.StatusCode)
	}
	logOutbound(ctx, "registry_lookup", "schema-registry", resp.StatusCode, start, lookupErr)
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("%w: %d", errUnknownSchema, id)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: status %d", errRegistryUnavailable, resp.StatusCode)
	}
	var result struct {
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result); err != nil {
		return nil, fmt.Errorf("%w: %v", errRegistryUnavailable, err)
	}
	codec, err := goavro.NewCodec(result.Schema)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid registered schema %d: %v", errConsumerIncompatible, id, err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO schema_cache(registry,id,schema) VALUES (?,?,?) ON CONFLICT(registry,id) DO NOTHING`, s.base, id, result.Schema); err != nil {
		return nil, fmt.Errorf("%w: persist schema %d: %w", errLocalStore, id, err)
	}
	var stored string
	if err := s.db.QueryRowContext(ctx, `SELECT schema FROM schema_cache WHERE registry=? AND id=?`, s.base, id).Scan(&stored); err != nil {
		return nil, fmt.Errorf("%w: verify schema %d: %w", errLocalStore, id, err)
	}
	if stored != result.Schema {
		return nil, fmt.Errorf("%w: schema ID %d changed in registry %s", errConsumerIncompatible, id, s.base)
	}
	s.mu.Lock()
	s.codecs[id] = codec
	s.mu.Unlock()
	return codec, nil
}

type envelope struct {
	Lifecycle     *lifecycleEvent
	CorrelationID string
	HazardID      string
	Payload       hazardEvent
}

type hazardEvent struct {
	HazardID    string         `json:"hazard_id"`
	Source      string         `json:"source"`
	SourceRefID string         `json:"source_ref_id"`
	HazardType  string         `json:"hazard_type"`
	Severity    string         `json:"severity"`
	AreaName    string         `json:"area_name"`
	Latitude    float64        `json:"latitude"`
	Longitude   float64        `json:"longitude"`
	OccurredAt  string         `json:"occurred_at"`
	IngestedAt  string         `json:"ingested_at"`
	Attributes  map[string]any `json:"attributes"`
}

func (s *schemaClient) decode(ctx context.Context, data []byte) (envelope, error) {
	if len(data) < 5 || data[0] != 0 {
		return envelope{}, fmt.Errorf("%w: invalid schema ID frame", errCorruptPayload)
	}
	id := int(binary.BigEndian.Uint32(data[1:5]))
	codec, err := s.codec(ctx, id)
	if err != nil {
		return envelope{}, err
	}
	native, rest, err := codec.NativeFromBinary(data[5:])
	if err != nil {
		return envelope{}, fmt.Errorf("%w: invalid Avro payload: %v", errCorruptPayload, err)
	}
	if len(rest) != 0 {
		return envelope{}, fmt.Errorf("%w: trailing Avro bytes", errCorruptPayload)
	}
	outer, ok := native.(map[string]any)
	if !ok || outer["envelope_version"] != int32(1) {
		return envelope{}, fmt.Errorf("%w: unsupported envelope", errConsumerIncompatible)
	}
	cid, ok := outer["correlation_id"].(string)
	if !ok || cid == "" {
		return envelope{}, fmt.Errorf("%w: missing correlation ID", errCorruptPayload)
	}
	if headerCID := correlationFromContext(ctx); headerCID != "" && headerCID != cid {
		return envelope{}, fmt.Errorf("%w: correlation header and envelope differ", errCorruptPayload)
	}
	if _, ok := outer["snapshot"]; ok {
		return decodeLifecycle(outer, cid)
	}
	payload, ok := outer["payload"].(map[string]any)
	if !ok {
		return envelope{}, fmt.Errorf("%w: missing hazard payload", errConsumerIncompatible)
	}
	idValue, ok := payload["hazard_id"].(string)
	if !ok || idValue == "" {
		return envelope{}, fmt.Errorf("%w: missing hazard ID", errCorruptPayload)
	}
	for _, key := range []string{"source", "source_ref_id", "hazard_type", "severity", "area_name", "occurred_at", "ingested_at"} {
		value, ok := payload[key].(string)
		if !ok {
			return envelope{}, fmt.Errorf("%w: invalid %s type", errConsumerIncompatible, key)
		}
		if value == "" {
			return envelope{}, fmt.Errorf("%w: empty %s", errCorruptPayload, key)
		}
	}
	for _, key := range []string{"latitude", "longitude"} {
		if _, ok := payload[key].(float64); !ok {
			return envelope{}, fmt.Errorf("%w: invalid %s type", errConsumerIncompatible, key)
		}
	}
	attrs, ok := payload["attributes"].(map[string]any)
	if !ok {
		return envelope{}, fmt.Errorf("%w: invalid attributes type", errConsumerIncompatible)
	}
	decodedAttrs := make(map[string]any, len(attrs))
	for key, value := range attrs {
		bytes, ok := value.([]byte)
		if !ok {
			return envelope{}, fmt.Errorf("%w: invalid attribute %s type", errConsumerIncompatible, key)
		}
		var decoded any
		if err := json.Unmarshal(bytes, &decoded); err != nil {
			return envelope{}, fmt.Errorf("%w: invalid attribute %s: %v", errCorruptPayload, key, err)
		}
		decodedAttrs[key] = decoded
	}
	payload["attributes"] = decodedAttrs
	encoded, err := json.Marshal(payload)
	if err != nil {
		return envelope{}, fmt.Errorf("%w: encode payload: %v", errConsumerIncompatible, err)
	}
	var hazard hazardEvent
	if err := json.Unmarshal(encoded, &hazard); err != nil {
		return envelope{}, fmt.Errorf("%w: decode consumer model: %v", errConsumerIncompatible, err)
	}
	return envelope{CorrelationID: cid, HazardID: idValue, Payload: hazard}, nil
}

type app struct {
	*haltController
	lifecycle  *app
	cfg        config
	db         *sql.DB
	schemas    *schemaClient
	ready      atomic.Bool
	pauseUntil atomic.Int64
}

type deadLetter struct {
	OriginalStream  string `json:"original_stream"`
	OriginalSubject string `json:"original_subject"`
	StreamSequence  uint64 `json:"stream_sequence"`
	Consumer        string `json:"consumer"`
	CorrelationID   string `json:"correlation_id,omitempty"`
	Attempts        uint64 `json:"attempts"`
	Reason          string `json:"reason"`
	ReasonCode      string `json:"reason_code"`
	FailedAt        string `json:"failed_at"`
	Raw             []byte `json:"raw"`
}

func (a *app) quarantine(ctx context.Context, js jetstream.JetStream, msg jetstream.Msg, meta *jetstream.MsgMetadata, code string, reason error) error {
	entry := deadLetter{OriginalStream: meta.Stream, OriginalSubject: msg.Subject(), StreamSequence: meta.Sequence.Stream, Consumer: a.cfg.Durable, CorrelationID: correlationFromContext(ctx), Attempts: meta.NumDelivered, Reason: reason.Error(), ReasonCode: code, FailedAt: time.Now().UTC().Format(time.RFC3339Nano), Raw: msg.Data()}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	subject := a.dlqSubject(meta)
	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	started := time.Now()
	ack, err := js.Publish(pubCtx, subject, data, jetstream.WithMsgID(fmt.Sprintf("%s:%s:%d", a.cfg.Durable, meta.Stream, meta.Sequence.Stream)))
	logOutbound(ctx, "dlq_publish", "nats", 0, started, err)
	if err != nil || ack == nil || ack.Duplicate {
		if verifyErr := a.verifyStoredDLQ(ctx, js, subject, entry); verifyErr != nil {
			return fmt.Errorf("DLQ publish unconfirmed (%v): %w", err, verifyErr)
		}
	}
	ackCtx, ackCancel := context.WithTimeout(ctx, 5*time.Second)
	defer ackCancel()
	started = time.Now()
	err = msg.DoubleAck(ackCtx)
	logOutbound(ctx, "original_double_ack", "nats", 0, started, err)
	if err != nil {
		return err
	}
	if err := a.clearRetries(ctx, meta); err != nil {
		logEvent(ctx, slog.LevelWarn, "retry cleanup failed", "error", err)
	}
	logEvent(ctx, slog.LevelError, "message quarantined", "consumer", a.cfg.Durable, "dlq_subject", subject, "attempts", meta.NumDelivered, "reason_code", code, "reason", reason.Error())
	return nil
}

func (a *app) delayedRetry(ctx context.Context, msg jetstream.Msg, delay time.Duration, cause error) bool {
	logEvent(ctx, slog.LevelWarn, "message delayed", "delay", delay, "error", cause)
	started := time.Now()
	err := msg.NakWithDelay(delay)
	logOutbound(ctx, "delayed_nak", "nats", 0, started, err)
	if err != nil {
		logEvent(ctx, slog.LevelError, "delayed NAK failed; reconnecting", "error", err)
		return false
	}
	return true
}

func (a *app) confirmedAck(ctx context.Context, msg jetstream.Msg, meta *jetstream.MsgMetadata) bool {
	ackCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	started := time.Now()
	err := msg.DoubleAck(ackCtx)
	logOutbound(ctx, "original_double_ack", "nats", 0, started, err)
	if err != nil {
		logEvent(ctx, slog.LevelError, "confirmed ack failed; redelivery will be deduplicated", "error", err)
		return false
	}
	if err := a.clearRetries(ctx, meta); err != nil {
		logEvent(ctx, slog.LevelWarn, "retry cleanup failed", "error", err)
	}
	return true
}

func (a *app) haltFor(ctx context.Context, reason string, meta *jetstream.MsgMetadata, err error) {
	logEvent(ctx, slog.LevelError, "consumer halted; operator action required", "reason", reason, "error", err)
	a.halt(reason + ": " + err.Error())
}

func (a *app) quarantineOrRetry(ctx context.Context, js jetstream.JetStream, msg jetstream.Msg, meta *jetstream.MsgMetadata, code string, cause error) bool {
	if err := a.quarantine(ctx, js, msg, meta, code, cause); err != nil {
		logEvent(ctx, slog.LevelError, "DLQ publish or ack failed", "error", err)
		if errors.Is(err, errDLQConflict) {
			a.haltFor(ctx, "dlq_identity_conflict", meta, err)
			return false
		}
		return a.delayedRetry(ctx, msg, 10*time.Second, err)
	}
	return true
}

func (a *app) handleStoreFailure(ctx context.Context, js jetstream.JetStream, msg jetstream.Msg, meta *jetstream.MsgMetadata, err error, messageWrite bool) bool {
	if errors.Is(err, errConsumerIncompatible) {
		a.haltFor(ctx, "consumer_incompatible", meta, err)
		return false
	}
	switch classifyStoreFailure(err) {
	case storeTransient:
		return a.delayedRetry(ctx, msg, 5*time.Second, err)
	case storeConstraint:
		if !messageWrite {
			a.haltFor(ctx, "consumer_state_constraint", meta, err)
			return false
		}
		state, countErr := a.recordConstraint(ctx, meta, time.Now())
		if countErr != nil {
			a.haltFor(ctx, "constraint_tracking_failed", meta, countErr)
			return false
		}
		if state.Recent >= constraintStormLimit {
			a.haltFor(ctx, "constraint_failure_storm", meta, err)
			return false
		}
		if state.Attempts < constraintLimit {
			return a.delayedRetry(ctx, msg, retryDelay(state.Attempts, meta.Sequence.Stream), err)
		}
		return a.quarantineOrRetry(ctx, js, msg, meta, "sqlite_constraint", err)
	default:
		a.haltFor(ctx, "sqlite_environmental_failure", meta, err)
		return false
	}
}

// process returns false when the connection should be closed or fetching must stop.
func (a *app) process(parent context.Context, js jetstream.JetStream, msg jetstream.Msg) bool {
	ctx, cancel := context.WithTimeout(parent, 25*time.Second)
	defer cancel()
	meta, err := msg.Metadata()
	if err != nil {
		return a.delayedRetry(ctx, msg, 5*time.Second, err)
	}
	ctx = withMessageTrace(ctx, a.cfg.Role, msg, meta)
	processed, err := a.alreadyProcessed(ctx, meta, msg.Data())
	if err != nil {
		return a.handleStoreFailure(ctx, js, msg, meta, err, false)
	}
	if processed {
		return a.confirmedAck(ctx, msg, meta)
	}
	e, err := a.schemas.decode(ctx, msg.Data())
	if err != nil {
		if errors.Is(err, errRegistryUnavailable) {
			a.pauseUntil.Store(time.Now().Add(30 * time.Second).UnixNano())
			return a.delayedRetry(ctx, msg, 30*time.Second, err)
		}
		if errors.Is(err, errUnknownSchema) {
			id := int(binary.BigEndian.Uint32(msg.Data()[1:5]))
			state, countErr := a.recordUnknown(ctx, meta, id, time.Now())
			if countErr != nil {
				return a.handleStoreFailure(ctx, js, msg, meta, countErr, false)
			}
			if state.Recent >= unknownStormLimit {
				a.haltFor(ctx, "unknown_schema_storm", meta, err)
				return false
			}
			if time.Since(state.FirstSeen) < unknownSchemaWindow {
				return a.delayedRetry(ctx, msg, retryDelay(state.Attempts, meta.Sequence.Stream), err)
			}
			return a.quarantineOrRetry(ctx, js, msg, meta, "schema_not_found", err)
		}
		if errors.Is(err, errLocalStore) {
			return a.handleStoreFailure(ctx, js, msg, meta, err, false)
		}
		if errors.Is(err, errCorruptPayload) {
			recent, countErr := a.recordCorrupt(ctx, meta, time.Now())
			if countErr != nil {
				return a.handleStoreFailure(ctx, js, msg, meta, countErr, false)
			}
			if recent >= corruptStormLimit {
				a.haltFor(ctx, "corrupt_payload_storm", meta, err)
				return false
			}
			return a.quarantineOrRetry(ctx, js, msg, meta, "corrupt_payload", err)
		}
		a.haltFor(ctx, "consumer_incompatible", meta, err)
		return false
	}
	ctx = withDecodedCorrelation(ctx, e.CorrelationID)
	if err := a.save(ctx, meta, msg.Data(), e); err != nil {
		return a.handleStoreFailure(ctx, js, msg, meta, err, true)
	}
	if !a.confirmedAck(ctx, msg, meta) {
		return false
	}
	logEvent(ctx, slog.LevelInfo, "event processed", "hazard_id", e.HazardID)
	return true
}

func (a *app) run(ctx context.Context) {
	ctx = withServiceTrace(ctx, a.cfg.Role)
	for ctx.Err() == nil {
		started := time.Now()
		nc, err := nats.Connect(a.cfg.NATSURL, nats.Name(a.cfg.Durable), nats.Timeout(3*time.Second))
		logOutbound(ctx, "broker_connect", "nats", 0, started, err)
		if err != nil {
			slog.Warn("broker unavailable", "service", a.cfg.Role, "error", err)
			sleep(ctx, 2*time.Second)
			continue
		}
		js, err := jetstream.New(nc)
		if err != nil {
			nc.Close()
			sleep(ctx, 2*time.Second)
			continue
		}
		setupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		started = time.Now()
		stream, err := js.Stream(setupCtx, a.cfg.Stream)
		logOutbound(ctx, "source_stream_lookup", "nats", 0, started, err)
		var dlq jetstream.Stream
		if err == nil {
			started = time.Now()
			dlq, err = js.Stream(setupCtx, a.cfg.DLQStream)
			logOutbound(ctx, "dlq_stream_lookup", "nats", 0, started, err)
		}
		if err == nil {
			started = time.Now()
			var info *jetstream.StreamInfo
			info, err = dlq.Info(setupCtx)
			logOutbound(ctx, "dlq_stream_info", "nats", 0, started, err)
			if err == nil && (info.Config.Discard != jetstream.DiscardNew || !info.Config.DiscardNewPerSubject || info.Config.MaxMsgsPerSubject != 1 ||
				len(info.Config.Subjects) != 1 || !strings.HasPrefix(a.cfg.DLQSubject+".", strings.TrimSuffix(info.Config.Subjects[0], ">"))) {
				err = fmt.Errorf("DLQ stream %q does not enforce unique subjects for %q", a.cfg.DLQStream, a.cfg.DLQSubject)
			}
		}
		var consumer jetstream.Consumer
		if err == nil {
			started = time.Now()
			consumer, err = stream.CreateOrUpdateConsumer(setupCtx, jetstream.ConsumerConfig{Durable: a.cfg.Durable, FilterSubject: a.cfg.Subject, DeliverPolicy: jetstream.DeliverAllPolicy, AckPolicy: jetstream.AckExplicitPolicy, AckWait: 60 * time.Second, MaxDeliver: -1, MaxAckPending: 64})
			logOutbound(ctx, "consumer_setup", "nats", 0, started, err)
		}
		cancel()
		if err != nil {
			slog.Warn("consumer setup failed", "service", a.cfg.Role, "error", err)
			nc.Close()
			sleep(ctx, 2*time.Second)
			continue
		}
		slog.Info("consumer ready", "service", a.cfg.Role, "durable", a.cfg.Durable, "stream", a.cfg.Stream)
		for ctx.Err() == nil && !nc.IsClosed() {
			if err := a.refreshHalt(ctx); err != nil {
				slog.Error("cannot read consumer halt state", "service", a.cfg.Role, "error", err)
				a.ready.Store(false)
				sleep(ctx, 5*time.Second)
				continue
			}
			if a.halted.Load() {
				a.ready.Store(false)
				sleep(ctx, 5*time.Second)
				continue
			}
			if until := time.Unix(0, a.pauseUntil.Load()); time.Now().Before(until) {
				a.ready.Store(false)
				sleep(ctx, 5*time.Second)
				continue
			}
			a.markReady()
			started = time.Now()
			batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
			logOutbound(ctx, "consumer_fetch", "nats", 0, started, err)
			if err != nil {
				slog.Warn("fetch failed", "service", a.cfg.Role, "error", err)
				break
			}
			continueFetch := true
			for msg := range batch.Messages() {
				if !a.process(ctx, js, msg) {
					continueFetch = false
					break
				}
			}
			if !continueFetch {
				break
			}
			if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, jetstream.ErrNoMessages) {
				slog.Warn("fetch batch failed", "service", a.cfg.Role, "error", err)
				break
			}
		}
		a.ready.Store(false)
		nc.Close()
		sleep(ctx, 2*time.Second)
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func (a *app) router() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"status": "ok", "service": "consumer-" + a.cfg.Role})
	})
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		halted, reason := a.snapshot()
		ready := a.ready.Load() && !halted && time.Now().UnixNano() >= a.pauseUntil.Load()
		if a.lifecycle != nil {
			ready = ready && a.lifecycle.ready.Load() && !a.lifecycle.halted.Load() && time.Now().UnixNano() >= a.lifecycle.pauseUntil.Load()
		}
		body := map[string]any{"ready": ready, "halted": halted}
		if halted {
			body["reason"] = reason
		}
		if until := a.pauseUntil.Load(); time.Now().UnixNano() < until {
			body["registry_retry_after"] = time.Unix(0, until).UTC().Format(time.RFC3339)
		}
		if !ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		writeJSON(w, body)
	})
	mux.HandleFunc("GET /records", func(w http.ResponseWriter, r *http.Request) {
		if view := r.URL.Query().Get("view"); view != "" {
			a.lifecycleRecords(w, r, view)
			return
		}
		rows, err := a.db.QueryContext(r.Context(), `SELECT hazard_id,payload,correlation_id,processed_at FROM records ORDER BY processed_at DESC LIMIT 100`)
		if err != nil {
			http.Error(w, "store unavailable", http.StatusServiceUnavailable)
			return
		}
		defer rows.Close()
		items := []map[string]any{}
		for rows.Next() {
			var id, payload, cid, at string
			if err := rows.Scan(&id, &payload, &cid, &at); err != nil {
				http.Error(w, "store unavailable", http.StatusServiceUnavailable)
				return
			}
			items = append(items, map[string]any{"hazard_id": id, "payload": json.RawMessage(payload), "correlation_id": cid, "processed_at": at})
		}
		if err := rows.Err(); err != nil {
			http.Error(w, "store unavailable", http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, map[string]any{"data": items, "count": len(items)})
	})
	return mux
}

func (a *app) serve(ctx context.Context) error {
	srv := &http.Server{Addr: ":" + a.cfg.Port, Handler: a.router()}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	return srv.ListenAndServe()
}

func writeJSON(w http.ResponseWriter, body any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

func main() {
	if len(os.Args) > 1 {
		if err := runAdminCommand(context.Background(), os.Args[1:], os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	cfg, err := loadConfig()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	db, err := openDB(cfg.DBPath)
	if err != nil {
		slog.Error("store unavailable", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	a := &app{haltController: &haltController{}, cfg: cfg, db: db, schemas: newSchemaClient(cfg.RegistryURL, db)}
	lifecycleCfg := cfg
	lifecycleCfg.Stream = cfg.LifecycleStream
	lifecycleCfg.Subject = cfg.LifecycleSubject
	lifecycleCfg.Durable = cfg.Durable + "-lifecycle"
	lifecycleCfg.DLQSubject = cfg.DLQSubject + ".lifecycle"
	a.lifecycle = &app{haltController: a.haltController, cfg: lifecycleCfg, db: db, schemas: a.schemas}
	closeAdmin, err := a.startLocalAdmin(ctx)
	if err != nil {
		slog.Error("local administration unavailable", "error", err)
		os.Exit(1)
	}
	defer closeAdmin()
	go a.lifecycle.run(ctx)
	go a.run(ctx)
	if err := a.serve(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("http server failed", "error", err)
		os.Exit(1)
	}
}
