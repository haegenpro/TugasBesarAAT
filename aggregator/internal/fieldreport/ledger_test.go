package fieldreport

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/canonical"
	"aggregator/internal/ingestion"
	"aggregator/internal/rawinput"
	"aggregator/internal/store"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

type labDurability struct{ pool *pgxpool.Pool }

func (labDurability) Prepare(context.Context, pgx.Tx) error { return nil }
func (d labDurability) ConfirmRecord(ctx context.Context, q string, args ...any) error {
	var ok bool
	if err := d.pool.QueryRow(ctx, q, args...).Scan(&ok); err != nil {
		return err
	}
	if !ok {
		return canonical.Unavailable()
	}
	return nil
}

type fakeBroker struct {
	mu   sync.Mutex
	fail error
	sent []*nats.Msg
}

func (b *fakeBroker) PublishMsg(_ context.Context, msg *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fail != nil {
		return nil, b.fail
	}
	b.sent = append(b.sent, msg)
	return &jetstream.PubAck{}, nil
}

func rawSchema(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "fixtures", "raw-input-v1.avsc"))
	require.NoError(t, err)
	return string(b)
}

func registry(t *testing.T, schema string) *avrowire.Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/schemas/ids/"):
			_ = json.NewEncoder(w).Encode(map[string]string{"schema": schema})
		case strings.HasSuffix(r.URL.Path, "/versions"):
			_, _ = w.Write([]byte(`{"id":1}`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	t.Cleanup(server.Close)
	return avrowire.NewClient(server.URL)
}

func ledgerLab(t *testing.T) (*Ledger, *fakeBroker) {
	t.Helper()
	raw := os.Getenv("M2_DATABASE_URL")
	if raw == "" {
		t.Skip("isolated database unavailable")
	}
	u, err := url.Parse(raw)
	require.NoError(t, err)
	require.Contains(t, []string{"127.0.0.1", "localhost"}, u.Hostname())
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, raw)
	require.NoError(t, err)
	name := "reports_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize())
	require.NoError(t, err)
	u.Path = "/" + name
	c, err := pgx.Connect(ctx, u.String())
	require.NoError(t, err)
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	require.NoError(t, err)
	require.NoError(t, ingestion.Migrate(ctx, c, root, ""))
	c.Close(ctx)
	db, err := store.New(ctx, u.String(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
		_, _ = admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
		admin.Close(ctx)
	})
	broker := &fakeBroker{}
	schema := rawSchema(t)
	return &Ledger{DB: db, Durability: labDurability{pool: db.Pool}, Sender: rawinput.Sender{JS: broker, Registry: registry(t, schema), SchemaText: schema}, Namespace: "field-reports-fixture", Holder: "test"}, broker
}

func submission(key, body string) Submission {
	return Submission{Submitter: "field-demo", Key: key, ContentType: ContentType, Body: []byte(body), CorrelationID: "fixture"}
}

func TestAcceptIsIdempotentPerSubmitterAndKey(t *testing.T) {
	l, broker := ledgerLab(t)
	ctx := context.Background()
	first, err := l.Accept(ctx, submission("report-fixture-1", validReport))
	require.NoError(t, err)
	require.Equal(t, "accepted", first.Status)
	require.Equal(t, "report-fixture-1", first.ReportID)
	again, err := l.Accept(ctx, submission("report-fixture-1", validReport))
	require.NoError(t, err)
	require.Equal(t, first, again)
	require.Len(t, broker.sent, 1)
	require.Equal(t, rawinput.Reports.Subject, broker.sent[0].Subject)
	_, err = l.Accept(ctx, submission("report-fixture-1", strings.Replace(validReport, "lutut", "pinggang", 1)))
	require.ErrorContains(t, err, "identity_conflict")
	other := submission("report-fixture-1", validReport)
	other.Submitter = "field-other"
	third, err := l.Accept(ctx, other)
	require.NoError(t, err)
	require.NotEqual(t, first.ReceiptID, third.ReceiptID)
}

func TestBrokerFailureNeverReturnsAcceptedAndOutboxCompletesIt(t *testing.T) {
	l, broker := ledgerLab(t)
	ctx := context.Background()
	broker.fail = errors.New("stream unavailable")
	_, err := l.Accept(ctx, submission("k1", validReport))
	require.ErrorIs(t, err, errPending)
	var state string
	require.NoError(t, l.DB.Pool.QueryRow(ctx, `SELECT state FROM field_report_ledger WHERE report_id='k1'`).Scan(&state))
	require.Equal(t, "pending", state)
	broker.fail = nil
	n, err := l.Republish(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	receipt, err := l.Accept(ctx, submission("k1", validReport))
	require.NoError(t, err)
	require.Len(t, broker.sent, 1)
	require.Equal(t, broker.sent[0].Header.Get("Nats-Msg-Id"), func() string {
		var id string
		require.NoError(t, l.DB.Pool.QueryRow(ctx, `SELECT message_id FROM field_report_ledger WHERE receipt_id=$1`, receipt.ReceiptID).Scan(&id))
		return id
	}())
}

func TestProjectionIsExactlyOnceAndStatusIsScoped(t *testing.T) {
	l, broker := ledgerLab(t)
	ctx := context.Background()
	receipt, err := l.Accept(ctx, submission("k2", validReport))
	require.NoError(t, err)
	e, err := rawinput.Decode(ctx, l.Sender.Registry, mustReader(t), broker.sent[0].Data)
	require.NoError(t, err)
	require.Equal(t, Source, e.Source)
	require.Equal(t, receipt.ReceiptID, *e.Ref)
	require.NoError(t, l.project(ctx, *e.Ref, e.Payload))
	require.NoError(t, l.project(ctx, *e.Ref, e.Payload))
	var projected int
	require.NoError(t, l.DB.Pool.QueryRow(ctx, `SELECT count(*) FROM field_reports`).Scan(&projected))
	require.Equal(t, 1, projected)
	status, err := l.Status(ctx, receipt.ReceiptID, "field-demo")
	require.NoError(t, err)
	require.Equal(t, "processed", status.Status)
	_, err = l.Status(ctx, receipt.ReceiptID, "field-other")
	require.ErrorContains(t, err, "not_found")
	status, err = l.Status(ctx, receipt.ReceiptID, "")
	require.NoError(t, err)
	require.Equal(t, "processed", status.Status)
	require.ErrorContains(t, l.project(ctx, uuid.NewString(), []byte(validReport)), "unknown_receipt")
}

func mustReader(t *testing.T) *avrowire.Reader {
	t.Helper()
	r, err := avrowire.NewReader(rawSchema(t))
	require.NoError(t, err)
	return r
}

func TestAcceptedHTTPReportSurvivesReceiverReplacementWithRealBroker(t *testing.T) {
	url := os.Getenv("TEST_NATS_URL")
	if url == "" {
		t.Skip("TEST_NATS_URL required for report transport integration")
	}
	l, _ := ledgerLab(t)
	nc, err := nats.Connect(url)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	stream, err := rawinput.Ensure(ctx, js, rawinput.Reports, 1)
	require.NoError(t, err)
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), rawinput.Reports.Name) })
	l.Sender.JS = js
	router := gin.New()
	HTTP{Ledger: l, Token: "test-service"}.Register(router)
	server := httptest.NewServer(router)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/internal/field-reports", strings.NewReader(validReport))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer test-service")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Submitter-ID", "field-demo")
	req.Header.Set("Idempotency-Key", "receiver-replacement")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	var accepted Receipt
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&accepted))
	resp.Body.Close()
	server.Close()
	// Processing was stopped throughout acceptance. Replace the HTTP receiver
	// and ledger object, then consume the work already persisted in JetStream.
	replacement := &Ledger{DB: l.DB, Durability: l.Durability, Sender: l.Sender, Namespace: l.Namespace, Holder: "replacement"}
	consumer := &Consumer{Ledger: replacement, Registry: l.Sender.Registry, Reader: mustReader(t), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	batch, err := stream.Fetch(1, jetstream.FetchMaxWait(time.Second))
	require.NoError(t, err)
	for msg := range batch.Messages() {
		require.NoError(t, consumer.handle(ctx, msg))
	}
	require.NoError(t, batch.Error())
	status, err := replacement.Status(ctx, accepted.ReceiptID, "field-demo")
	require.NoError(t, err)
	require.Equal(t, "processed", status.Status)
	repeated, err := replacement.Accept(ctx, submission("receiver-replacement", validReport))
	require.NoError(t, err)
	require.Equal(t, accepted.ReceiptID, repeated.ReceiptID)
	var count int
	require.NoError(t, l.DB.Pool.QueryRow(ctx, `SELECT count(*) FROM field_reports`).Scan(&count))
	require.Equal(t, 1, count)
	info, err := stream.Info(ctx)
	require.NoError(t, err)
	require.Zero(t, info.NumAckPending)
}
