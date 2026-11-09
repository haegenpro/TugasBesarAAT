package clients

import (
	"aggregator/internal/platform"
	"context"
	"errors"
	"fmt"
	"time"
)

type Failure struct {
	Source        string
	Entity        string
	Payload       []byte
	Truncated     bool
	Retryable     bool
	Reason        string
	CorrelationID string
}
type FailureRecorder func(context.Context, Failure) error

// capture records decoding/validation failures before a fetch can be considered successful.
// It deliberately does not advance source progress or implement an automatic parked-message replay.
func capture(ctx context.Context, record FailureRecorder, f Failure, err *error) {
	if *err == nil || record == nil {
		return
	}
	f.Reason = (*err).Error()
	f.Retryable = !errors.Is(*err, ErrContractViolation)
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
