package coastal

import (
	"aggregator/internal/acquisition"
	"aggregator/internal/ingestion"
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"net/http"
	"time"
)

type responseQuarantineKey struct{}

func withResponseQuarantine(ctx context.Context, source *ingestion.SourceStore) context.Context {
	return context.WithValue(ctx, responseQuarantineKey{}, source)
}

func rejectResponse(ctx context.Context, resp *http.Response, body io.Reader, reason string) error {
	original := &SourceError{Status: resp.StatusCode, Reason: reason}
	source, _ := ctx.Value(responseQuarantineKey{}).(*ingestion.SourceStore)
	if source == nil {
		if acquisition.ResponseID(resp) != "" {
			return errors.Join(original, errors.New("durable HTTP response quarantine unavailable"))
		}
		return original // uncoordinated standalone client; no cached receipt to retire
	}
	id := acquisition.ResponseID(resp)
	if id == "" {
		id = uuid.NewString()
	}
	persist, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	err := acquisition.QuarantineResponse(persist, resp, func() error {
		return source.QuarantineResponse(persist, id, resp.Header.Get("Content-Type"), fmt.Sprintf("HTTP %s: %s", resp.Request.URL.Path, reason), body)
	})
	if err != nil {
		return errors.Join(original, fmt.Errorf("retain rejected response: %w", err))
	}
	return original
}
