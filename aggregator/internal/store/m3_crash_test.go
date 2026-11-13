package store

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/canonical"
	"aggregator/internal/ownership"
	"aggregator/internal/rawinput"
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestM3HistoryCrashHelper(t *testing.T) {
	dsn := os.Getenv("M3_HISTORY_HELPER_DSN")
	if dsn == "" {
		t.Skip("subprocess-only crash helper")
	}
	ctx := context.Background()
	s, err := New(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	defer s.Close()
	s.Canonical.Durability = m3LabDurability{pool: s.Pool}
	a, err := ownership.NewProcessor(s.Pool, 15*time.Second)
	require.NoError(t, err)
	s.Processor = a
	lease, err := a.Acquire(ctx, canonical.AcquireOwnership{Scope: "processor:SATELIT", Holder: "crashing-receiver"})
	require.NoError(t, err)
	text, err := os.ReadFile(filepath.Join("..", "..", "..", "docs/fixtures/raw-input-v1.avsc"))
	require.NoError(t, err)
	reader, err := avrowire.NewReader(string(text))
	require.NoError(t, err)
	registry := avrowire.NewClient(os.Getenv("M3_HISTORY_HELPER_REGISTRY"))
	nc, err := nats.Connect(os.Getenv("M3_NATS_URL"))
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	consumer, err := js.Consumer(ctx, rawinput.Satellite.Name, rawinput.Satellite.Consumer)
	require.NoError(t, err)
	batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(time.Second))
	require.NoError(t, err)
	var msg jetstream.Msg
	for m := range batch.Messages() {
		msg = m
	}
	require.NotNil(t, msg)
	e, err := rawinput.Decode(ctx, registry, reader, msg.Data())
	require.NoError(t, err)
	s.Inputs.Starts = map[string]int64{InputSession("SATELIT", e.Namespace): 1}
	_, err = s.AppendHistory(ctx, rawinput.AppendRequest{Version: 1, Ownership: lease.Ownership, Messages: []rawinput.Envelope{e}})
	require.NoError(t, err)
	replay, err := s.ReadHistory(ctx, e.Namespace, nil, 1)
	require.NoError(t, err)
	require.Len(t, replay.Items, 1)
	ready, _ := json.Marshal(map[string]any{"ready": true, "reply": msg.Reply()})
	fmt.Println(string(ready))
	// The parent kills this owned process at the persisted-receipt/no-ACK boundary.
	select {}
}
func TestM3KilledReceiverRetainsHistoryBeforeACK(t *testing.T) {
	if os.Getenv("M3_NATS_URL") == "" {
		t.Skip("run-owned three-replica broker required")
	}
	dsn, c := m2Database(t, false)
	m2Schema(t, c, "postgres", false)
	s := m2Store(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	s.Canonical.Durability = m3LabDurability{pool: s.Pool}
	text, err := os.ReadFile(filepath.Join("..", "..", "..", "docs/fixtures/raw-input-v1.avsc"))
	require.NoError(t, err)
	registryServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"schema": string(text)})
	}))
	defer registryServer.Close()
	registry := avrowire.NewClient(registryServer.URL)
	schema, err := registry.Lookup(ctx, 1)
	require.NoError(t, err)
	nc, err := nats.Connect(os.Getenv("M3_NATS_URL"))
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	consumer, err := rawinput.Ensure(ctx, js, rawinput.Satellite, 3)
	require.NoError(t, err)
	input := testInput(1)
	input.Namespace = "process-crash-v1"
	input.MessageID = rawinput.ID(input)
	frame, err := rawinput.Encode(schema, input)
	require.NoError(t, err)
	_, err = js.Publish(ctx, rawinput.Satellite.Subject, frame, jetstream.WithMsgID(input.MessageID))
	require.NoError(t, err)
	executable, err := os.Executable()
	require.NoError(t, err)
	child := exec.CommandContext(ctx, executable, "-test.run=^TestM3HistoryCrashHelper$", "-test.timeout=40s")
	child.Env = append(os.Environ(), "M3_HISTORY_HELPER_DSN="+dsn, "M3_HISTORY_HELPER_REGISTRY="+registryServer.URL)
	stdout, err := child.StdoutPipe()
	require.NoError(t, err)
	child.Stderr = io.Discard
	require.NoError(t, child.Start())
	childDone := make(chan error, 1)
	go func() { childDone <- child.Wait() }()
	reaped := false
	defer func() {
		if !reaped {
			_ = child.Process.Kill()
			<-childDone
		}
	}()
	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			var v struct {
				Ready bool   `json:"ready"`
				Reply string `json:"reply"`
			}
			if json.Unmarshal(scanner.Bytes(), &v) == nil && v.Ready {
				ready <- v.Reply
				return
			}
		}
	}()
	var reply string
	select {
	case reply = <-ready:
	case <-ctx.Done():
		t.Fatal("crash helper never confirmed durable receipt")
	}
	require.NoError(t, child.Process.Kill())
	select {
	case err = <-childDone:
		reaped = true
		require.Error(t, err)
	case <-ctx.Done():
		t.Fatal("owned receiver did not stop")
	}
	require.Contains(t, reply, rawinput.Satellite.Name)
	require.NoError(t, nc.Publish(reply, []byte("-NAK")))
	require.NoError(t, nc.Flush())
	a, err := ownership.NewProcessor(s.Pool, 30*time.Second)
	require.NoError(t, err)
	s.Processor = a
	s.Inputs.Starts = map[string]int64{InputSession("SATELIT", input.Namespace): 1}
	var successor canonical.Lease
	for {
		successor, err = a.Acquire(ctx, canonical.AcquireOwnership{Scope: "processor:SATELIT", Holder: "replacement"})
		if err == nil {
			break
		}
		var p *canonical.Error
		require.ErrorAs(t, err, &p)
		require.Equal(t, "ownership_conflict", p.Code)
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			t.Fatal(ctx.Err())
		case <-timer.C:
		}
	}
	fetch, err := consumer.Fetch(1, jetstream.FetchMaxWait(time.Second))
	require.NoError(t, err)
	var redelivery jetstream.Msg
	for m := range fetch.Messages() {
		redelivery = m
	}
	require.NotNil(t, redelivery)
	reader, err := avrowire.NewReader(string(text))
	require.NoError(t, err)
	decoded, err := rawinput.Decode(ctx, registry, reader, redelivery.Data())
	require.NoError(t, err)
	require.Equal(t, input, decoded)
	receipt, err := s.AppendHistory(ctx, rawinput.AppendRequest{Version: 1, Ownership: successor.Ownership, Messages: []rawinput.Envelope{decoded}})
	require.NoError(t, err)
	require.Equal(t, "duplicate", receipt.Items[0].Outcome)
	require.NoError(t, redelivery.DoubleAck(ctx))
	var count int
	require.NoError(t, c.QueryRow(ctx, `SELECT count(*) FROM input_history`).Scan(&count))
	require.Equal(t, 1, count)
	if dir := os.Getenv("M2_EVIDENCE_DIR"); dir != "" {
		b, _ := json.Marshal(map[string]any{"fault": "owned receiver killed after receipt before ACK", "receipt": receipt, "replacement_epoch": successor.Ownership.Epoch, "transport_redelivery": "synchronized NAK after process termination; not an AckWait timing claim"})
		require.NoError(t, os.WriteFile(filepath.Join(dir, "receipt-process-crash.json"), append(b, '\n'), 0600))
	}
}
