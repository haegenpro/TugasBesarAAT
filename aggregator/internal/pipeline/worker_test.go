package pipeline

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/canonical"
	"aggregator/internal/rawinput"
	"context"
	"encoding/json"
	"errors"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type handoffRepository struct {
	stored      *rawinput.Envelope
	logical     int
	replayError error
}

func (r *handoffRepository) AppendHistory(_ context.Context, req rawinput.AppendRequest) (rawinput.Receipt, error) {
	if r.stored == nil {
		r.logical++
		e := req.Messages[0]
		r.stored = &e
	}
	return rawinput.Receipt{Through: *r.stored.Position}, nil
}
func (r *handoffRepository) ReadHistory(context.Context, string, *int64, int) (rawinput.HistoryPage, error) {
	if r.replayError != nil {
		return rawinput.HistoryPage{}, r.replayError
	}
	return rawinput.HistoryPage{Items: []rawinput.Envelope{*r.stored}}, nil
}
func (r *handoffRepository) PrepareBMKG(context.Context, canonical.Ownership, string, *avrowire.Client, map[string]*avrowire.SourceReader) (bool, error) {
	return false, nil
}
func (r *handoffRepository) ApplyInputOutput(context.Context, canonical.Ownership, canonical.Ownership, string) (bool, error) {
	return false, nil
}

type message struct {
	body []byte
	acks int
}

func (m *message) Metadata() (*jetstream.MsgMetadata, error) { return &jetstream.MsgMetadata{}, nil }
func (m *message) Data() []byte                              { return m.body }
func (m *message) Headers() nats.Header                      { return nats.Header{} }
func (m *message) Subject() string                           { return rawinput.Satellite.Subject }
func (m *message) Reply() string                             { return "reply" }
func (m *message) Ack() error                                { panic("unconfirmed ACK must not be used") }
func (m *message) DoubleAck(context.Context) error           { m.acks++; return nil }
func (m *message) Nak() error                                { return nil }
func (m *message) NakWithDelay(time.Duration) error          { return nil }
func (m *message) InProgress() error                         { return nil }
func (m *message) Term() error                               { panic("raw input must not be discarded") }
func (m *message) TermWithReason(string) error               { panic("raw input must not be discarded") }
func TestM3CommitBeforeACKReplayAndRetry(t *testing.T) {
	schema, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "fixtures", "raw-input-v1.avsc"))
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"schema": string(schema)})
	}))
	defer server.Close()
	registry := avrowire.NewClient(server.URL)
	registered, err := registry.Lookup(context.Background(), 1)
	require.NoError(t, err)
	reader, err := avrowire.NewReader(string(schema))
	require.NoError(t, err)
	pos := int64(1)
	e := rawinput.Envelope{Version: 1, Source: "SATELIT", Namespace: "fixture", Position: &pos, Operation: "heartbeat", ReceivedAt: "2026-11-13T00:00:00Z", Payload: []byte("{}"), Size: 2, Digest: rawinput.Hash([]byte("{}")), ContentType: "application/json", CorrelationID: "fixture", OwnershipScope: "acquisition:SATELIT", OwnershipEpoch: 1, OwnershipHolder: "expired-producer"}
	e.MessageID = rawinput.ID(e)
	frame, err := rawinput.Encode(registered, e)
	require.NoError(t, err)
	repo := &handoffRepository{}
	w := Worker{Store: repo, Registry: registry, Reader: reader, BeforeACK: func(context.Context) error { return errors.New("synchronized process death before ACK") }}
	msg := &message{body: frame}
	owner := canonical.Ownership{Scope: "processor:SATELIT", Epoch: 2, Holder: "successor"}
	require.Error(t, w.Handoff(context.Background(), msg, "SATELIT", "fixture", owner))
	require.Equal(t, 1, repo.logical)
	require.Zero(t, msg.acks)
	w.BeforeACK = nil
	repo.replayError = errors.New("replay unavailable")
	require.Error(t, w.Handoff(context.Background(), msg, "SATELIT", "fixture", owner))
	require.Zero(t, msg.acks)
	repo.replayError = nil
	require.NoError(t, w.Handoff(context.Background(), msg, "SATELIT", "fixture", owner))
	require.Equal(t, 1, repo.logical)
	require.Equal(t, 1, msg.acks)
	require.Equal(t, "expired-producer", repo.stored.OwnershipHolder)
}
