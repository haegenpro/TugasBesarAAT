package clients

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/platform"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

type Failure struct {
	Source  string
	Entity  string
	Payload []byte
	// PayloadStream contains ALL bytes, including the bounded prefix, when a
	// response exceeded the decoding limit. Recorders must consume it before
	// returning; the upstream response is closed immediately afterwards.
	PayloadStream io.Reader
	ContentType   string
	Truncated     bool
	Retryable     bool
	Reason        string
	CorrelationID string
}

func readFailureBody(r io.Reader) ([]byte, bool, io.Reader, error) {
	limit := max(platform.Int("MAX_PAYLOAD_BYTES", 4<<20), 1)
	b, err := io.ReadAll(io.LimitReader(r, int64(limit)+1))
	if len(b) > limit {
		return b[:limit], true, io.MultiReader(bytes.NewReader(b), r), fmt.Errorf("payload_too_large")
	}
	return b, err != nil, nil, err
}

type FailureRecorder func(context.Context, Failure) error

// capture records decoding/validation failures before a fetch can be considered successful.
// It deliberately does not advance source progress or implement an automatic parked-message replay.
func capture(ctx context.Context, record FailureRecorder, f Failure, err *error) {
	if *err == nil || record == nil {
		return
	}
	f.Reason = (*err).Error()
	f.Retryable = !errors.Is(*err, ErrContractViolation) && !errors.Is(*err, avrowire.ErrIncompatible)
	if f.Truncated {
		f.Retryable = false
	}
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	if e := record(persistCtx, f); e != nil {
		*err = errors.Join(*err, fmt.Errorf("persist ingestion failure: %w", e))
	}
	platform.Add("ingestion_failures_total", 1)
}
