package poller_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"aggregator/internal/clients"
	"aggregator/internal/domain"
	"aggregator/internal/poller"
	"github.com/stretchr/testify/require"
)

type protectedSaver struct{}

func (protectedSaver) AcquisitionRequired() bool { return true }
func (protectedSaver) SaveDedup(context.Context, domain.HazardEvent, string) (bool, error) {
	return false, nil
}

type acquisitionTransport struct{ calls atomic.Int64 }

func (t *acquisitionTransport) RoundTrip(*http.Request) (*http.Response, error) {
	t.calls.Add(1)
	return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("[]"))}, nil
}

func TestAcquisitionOneShotCannotBypassProtectedStore(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	transport := &acquisitionTransport{}
	bmkg := clients.NewBMKGClient("http://source.invalid", "key", logger)
	pvmbg := clients.NewPVMBGClient("http://source.invalid", "token", logger)
	bmkg.HTTP.Transport, pvmbg.HTTP.Transport = transport, transport
	// Even an accidentally reconstructed Poller without its coordinator must
	// reject a diagnostic fetch when the Store requires acquisition protection.
	p := &poller.Poller{Store: protectedSaver{}, BMKG: bmkg, PVMBG: pvmbg}
	p.PollBMKGOnce(context.Background())
	p.PollPVMBGOnce(context.Background())
	require.Zero(t, transport.calls.Load())
}
