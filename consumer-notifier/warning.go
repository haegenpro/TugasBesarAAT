package main

import (
	"consumernotifier/internal/platform"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const onTime = 5 * time.Second

type warningConfig struct {
	NATSURL, Stream, Subject, Durable string
	BacklogAfter                      time.Duration
	FreshQuota                        int
}

func loadWarningConfig(base config) (warningConfig, bool, error) {
	durable := os.Getenv("WARNING_DURABLE")
	if durable == "" {
		return warningConfig{}, false, nil
	}
	if base.Role != "notifier" {
		return warningConfig{}, false, errors.New("coastal warnings are delivered only by the notifier role")
	}
	values, err := requiredEnv("WARNING_STREAM", "WARNING_SUBJECT")
	if err != nil {
		return warningConfig{}, false, err
	}
	return warningConfig{NATSURL: base.NATSURL, Stream: values["WARNING_STREAM"], Subject: values["WARNING_SUBJECT"], Durable: durable, BacklogAfter: platform.Duration("WARNING_BACKLOG_AFTER", time.Minute), FreshQuota: platform.Int("WARNING_FRESH_QUOTA", 3)}, true, nil
}

type warning struct {
	NotificationID   string  `json:"notification_id"`
	Recipient        string  `json:"recipient"`
	Source           string  `json:"source"`
	Namespace        string  `json:"source_namespace"`
	ObservationID    string  `json:"observation_id"`
	OperationID      string  `json:"operation_id"`
	Position         int64   `json:"operation_position"`
	Operation        string  `json:"operation"`
	ObservedAt       string  `json:"observed_at"`
	FirstReceivedAt  string  `json:"first_received_at"`
	Critical         bool    `json:"critical"`
	StationID        string  `json:"station_id"`
	Province         string  `json:"province"`
	Latitude         float64 `json:"latitude"`
	Longitude        float64 `json:"longitude"`
	SeaLevelAnomalyM float64 `json:"sea_level_anomaly_m"`
	WaveHeightM      float64 `json:"wave_height_m"`
	Quality          string  `json:"quality"`
	PayloadSHA256    string  `json:"payload_sha256"`
	CorrelationID    string  `json:"correlation_id"`
	Traceparent      *string `json:"traceparent"`
	HazardID         *string `json:"hazard_id"`
	observed         time.Time
	received         time.Time
}

type delivery struct {
	msg     jetstream.Msg
	warning warning
	digest  string
	err     error
}

type warningRecipient struct {
	cfg     warningConfig
	db      *sql.DB
	schemas *schemaClient
	ready   atomic.Bool
}

func (s *schemaClient) decodeWarning(ctx context.Context, data []byte) (warning, error) {
	var w warning
	if len(data) < 5 || data[0] != 0 {
		return w, fmt.Errorf("%w: invalid schema ID frame", errCorruptPayload)
	}
	codec, err := s.codec(ctx, int(binary.BigEndian.Uint32(data[1:5])))
	if err != nil {
		return w, err
	}
	native, rest, err := codec.NativeFromBinary(data[5:])
	if err != nil || len(rest) != 0 {
		return w, fmt.Errorf("%w: invalid warning datum", errCorruptPayload)
	}
	record, ok := native.(map[string]any)
	if !ok || record["warning_version"] != int32(1) {
		return w, fmt.Errorf("%w: unsupported warning version", errConsumerIncompatible)
	}
	for _, key := range []string{"traceparent", "hazard_id"} {
		if union, ok := record[key].(map[string]any); ok {
			record[key] = union["string"]
		}
	}
	b, err := json.Marshal(record)
	if err != nil {
		return w, err
	}
	if err = json.Unmarshal(b, &w); err != nil {
		return w, fmt.Errorf("%w: %v", errConsumerIncompatible, err)
	}
	if w.Recipient != "TIM_LAPANGAN" || !strings.HasPrefix(w.NotificationID, "warning-v1:") || w.ObservationID == "" || !w.Critical {
		return w, fmt.Errorf("%w: not a Tim Lapangan critical warning", errCorruptPayload)
	}
	if w.observed, err = time.Parse(time.RFC3339Nano, w.ObservedAt); err != nil {
		return w, fmt.Errorf("%w: observed_at", errCorruptPayload)
	}
	if w.received, err = time.Parse(time.RFC3339Nano, w.FirstReceivedAt); err != nil {
		return w, fmt.Errorf("%w: first_received_at", errCorruptPayload)
	}
	return w, nil
}

func schedule(batch []delivery, now time.Time, backlogAfter time.Duration, freshQuota int) []delivery {
	type group struct {
		items []delivery
		fresh bool
	}
	var order []string
	groups := map[string]*group{}
	for i, d := range batch {
		key := d.warning.ObservationID
		if d.err != nil {
			key = fmt.Sprintf("\x00%d", i)
		}
		g, ok := groups[key]
		if !ok {
			g = &group{fresh: d.err != nil || now.Sub(d.warning.received) <= backlogAfter}
			groups[key] = g
			order = append(order, key)
		}
		g.items = append(g.items, d)
	}
	var fresh, backlog []*group
	for _, key := range order {
		if groups[key].fresh {
			fresh = append(fresh, groups[key])
		} else {
			backlog = append(backlog, groups[key])
		}
	}
	out := make([]delivery, 0, len(batch))
	for len(fresh) > 0 || len(backlog) > 0 {
		for i := 0; i < max(freshQuota, 1) && len(fresh) > 0; i++ {
			out = append(out, fresh[0].items...)
			fresh = fresh[1:]
		}
		if len(backlog) > 0 {
			out = append(out, backlog[0].items...)
			backlog = backlog[1:]
		}
	}
	return out
}

func (r *warningRecipient) commit(ctx context.Context, d delivery) (string, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	if err = lockApplication(ctx, tx, "warning:"+d.warning.NotificationID); err != nil {
		return "", err
	}
	var known string
	err = tx.QueryRowContext(ctx, `SELECT digest FROM coastal_warning_receipts WHERE notification_id=$1`, d.warning.NotificationID).Scan(&known)
	switch {
	case err == nil && known == d.digest:
		return "duplicate", tx.Commit()
	case err == nil:
		_, err = tx.ExecContext(ctx, `INSERT INTO coastal_warning_conflicts(notification_id,digest,payload,detected_at) VALUES($1,$2,$3,now()) ON CONFLICT DO NOTHING`, d.warning.NotificationID, d.digest, d.msg.Data())
		if err != nil {
			return "", err
		}
		return "conflict", tx.Commit()
	case !errors.Is(err, sql.ErrNoRows):
		return "", err
	}
	payload, err := json.Marshal(d.warning)
	if err != nil {
		return "", err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO coastal_warning_receipts(notification_id,digest,observation_id,observed_at,first_received_at,committed_at) VALUES($1,$2,$3,$4,$5,clock_timestamp())`, d.warning.NotificationID, d.digest, d.warning.ObservationID, d.warning.observed, d.warning.received); err != nil {
		return "", err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO notifications(event_id,kind,payload,processed_at) VALUES($1,'coastal_warning',$2,$3)`, d.warning.NotificationID, string(payload), time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return "", err
	}
	return "delivered", tx.Commit()
}

func (r *warningRecipient) deliver(ctx context.Context, d delivery) error {
	if d.err != nil {
		if errors.Is(d.err, errRegistryUnavailable) || errors.Is(d.err, errLocalStore) {
			return d.err
		}
		slog.Error("coastal warning rejected", "service", "notifier", "reason", d.err.Error())
		if _, err := r.db.ExecContext(ctx, `INSERT INTO coastal_warning_conflicts(notification_id,digest,payload,detected_at) VALUES($1,$2,$3,now()) ON CONFLICT DO NOTHING`, "unreadable:"+d.digest, d.digest, d.msg.Data()); err != nil {
			return err
		}
		platform.Add("coastal_warnings_rejected_total", 1)
		return d.msg.Term()
	}
	outcome, err := r.commit(ctx, d)
	if err != nil {
		return err
	}
	if err = d.msg.DoubleAck(ctx); err != nil {
		return err
	}
	platform.Add("coastal_warnings_"+outcome+"_total", 1)
	if outcome == "delivered" {
		latency := time.Since(d.warning.received)
		platform.Gauge("coastal_warning_last_delivery_seconds", latency.Seconds())
		if latency <= onTime {
			platform.Add("coastal_warnings_on_time_total", 1)
		}
	}
	return nil
}

func (r *warningRecipient) consumer(ctx context.Context, js jetstream.JetStream) (jetstream.Consumer, error) {
	stream, err := js.Stream(ctx, r.cfg.Stream)
	if err != nil {
		return nil, err
	}
	consumer, err := stream.Consumer(ctx, r.cfg.Durable)
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		return stream.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: r.cfg.Durable, AckPolicy: jetstream.AckExplicitPolicy, AckWait: 30 * time.Second, MaxDeliver: -1, MaxAckPending: 128, FilterSubject: r.cfg.Subject, MaxRequestBatch: 32, MaxRequestExpires: time.Second})
	}
	return consumer, err
}

func (r *warningRecipient) run(ctx context.Context) {
	for ctx.Err() == nil {
		nc, err := nats.Connect(r.cfg.NATSURL, nats.Name(r.cfg.Durable), nats.Timeout(3*time.Second))
		if err != nil {
			slog.Warn("warning broker unavailable", "service", "notifier", "error", err)
			sleep(ctx, 2*time.Second)
			continue
		}
		js, err := jetstream.New(nc)
		var consumer jetstream.Consumer
		if err == nil {
			setup, cancel := context.WithTimeout(ctx, 5*time.Second)
			consumer, err = r.consumer(setup, js)
			cancel()
		}
		if err != nil {
			slog.Warn("warning consumer setup failed", "service", "notifier", "error", err)
			nc.Close()
			sleep(ctx, 2*time.Second)
			continue
		}
		r.ready.Store(true)
		for ctx.Err() == nil && !nc.IsClosed() {
			if err = r.cycle(ctx, consumer); err != nil {
				slog.Warn("warning delivery retrying", "service", "notifier", "error", err)
				sleep(ctx, time.Second)
			}
		}
		r.ready.Store(false)
		nc.Close()
	}
}

func (r *warningRecipient) cycle(ctx context.Context, consumer jetstream.Consumer) error {
	batch, err := consumer.Fetch(32, jetstream.FetchMaxWait(250*time.Millisecond))
	if err != nil {
		return err
	}
	var pending []delivery
	for msg := range batch.Messages() {
		d := delivery{msg: msg, digest: digestOf(msg.Data())}
		decodeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		d.warning, d.err = r.schemas.decodeWarning(decodeCtx, msg.Data())
		cancel()
		pending = append(pending, d)
	}
	if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, jetstream.ErrNoMessages) {
		return err
	}
	platform.Gauge("coastal_warning_batch_size", float64(len(pending)))
	ordered := schedule(pending, time.Now(), r.cfg.BacklogAfter, r.cfg.FreshQuota)
	for i, d := range ordered {
		call, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := r.deliver(call, d)
		cancel()
		if err != nil {
			for _, rest := range ordered[i:] {
				_ = rest.msg.NakWithDelay(time.Second)
			}
			return err
		}
	}
	return nil
}

type warningSLO struct {
	Window       string   `json:"window"`
	Delivered    int64    `json:"delivered"`
	OnTime       int64    `json:"on_time"`
	OnTimeRatio  *float64 `json:"on_time_ratio"`
	P95Seconds   *float64 `json:"p95_seconds"`
	MaxSeconds   *float64 `json:"max_seconds"`
	Conflicts    int64    `json:"conflicts"`
	BudgetUsed   *float64 `json:"error_budget_used"`
	TargetRatio  float64  `json:"target_ratio"`
	TargetP95Sec float64  `json:"target_p95_seconds"`
}

func (r *warningRecipient) slo(ctx context.Context, since time.Time) (warningSLO, error) {
	out := warningSLO{Window: since.UTC().Format(time.RFC3339) + "/now", TargetRatio: 0.95, TargetP95Sec: onTime.Seconds()}
	err := r.db.QueryRowContext(ctx, `SELECT count(*),count(*) FILTER (WHERE committed_at-first_received_at<=interval '5 seconds'),percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM committed_at-first_received_at)),max(extract(epoch FROM committed_at-first_received_at)),(SELECT count(*) FROM coastal_warning_conflicts WHERE detected_at>=$1) FROM coastal_warning_receipts WHERE first_received_at>=$1`, since).Scan(&out.Delivered, &out.OnTime, &out.P95Seconds, &out.MaxSeconds, &out.Conflicts)
	if err != nil {
		return out, err
	}
	if out.Delivered > 0 {
		ratio := float64(out.OnTime) / float64(out.Delivered)
		used := (1 - ratio) / (1 - out.TargetRatio)
		out.OnTimeRatio, out.BudgetUsed = &ratio, &used
	}
	return out, nil
}

func (r *warningRecipient) routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /warnings/slo", func(w http.ResponseWriter, req *http.Request) {
		since := time.Now().Add(-time.Hour)
		if raw := req.URL.Query().Get("since"); raw != "" {
			t, err := time.Parse(time.RFC3339Nano, raw)
			if err != nil {
				platform.WriteError(w, 400, "since must be an RFC 3339 timestamp")
				return
			}
			since = t
		}
		report, err := r.slo(req.Context(), since)
		if err != nil {
			platform.WriteError(w, 503, "store unavailable")
			return
		}
		writeJSON(w, report)
	})
}

func digestOf(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}