package clients

import (
	"aggregator/internal/acquisition"
	"aggregator/internal/avrowire"
	"aggregator/internal/platform"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
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
func capture(ctx context.Context, record FailureRecorder, f Failure, err *error, responses ...*http.Response) {
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

	var stream *retainedResponseReader
	if f.PayloadStream != nil {
		stream = &retainedResponseReader{Reader: f.PayloadStream}
		f.PayloadStream = stream
	}
	retain := func() error {
		if e := record(persistCtx, f); e != nil {
			return e
		}
		if len(responses) > 0 && acquisition.ResponseID(responses[0]) != "" && f.Truncated && (stream == nil || !stream.eof) {
			return errors.New("failure recorder did not retain a complete response")
		}
		return nil
	}
	var e error
	if !f.Retryable && len(responses) > 0 {
		e = acquisition.QuarantineResponse(persistCtx, responses[0], retain)
	} else {
		e = retain()
	}
	if e != nil {
		*err = errors.Join(*err, fmt.Errorf("persist ingestion failure: %w", e))
	}

	platform.Add("ingestion_failures_total", 1)
}

// Cache overflow still returns a response whose complete body can be consumed
// by the existing streaming failure recorder. Never decode it as accepted data.
func readResponseFailureBody(resp *http.Response) ([]byte, bool, io.Reader, error) {
	body, truncated, stream, err := readFailureBody(resp.Body)
	if acquisition.ResponseOverflow(resp) && err == nil {
		err = fmt.Errorf("%w: response exceeds dispatch cache bound", ErrContractViolation)
	}
	return body, truncated, stream, err
}

type retainedResponseReader struct {
	io.Reader
	eof bool
}

func (r *retainedResponseReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		r.eof = true
	}
	return n, err
}
