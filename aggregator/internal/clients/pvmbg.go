package clients

import (
	"aggregator/internal/platform"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"aggregator/internal/avrowire"
	"aggregator/internal/middleware"
)

// VolcanicReport contains mapped PVMBG fields and source-specific attributes.
type VolcanicReport struct {
	SourceSequence   int64
	ReportID         string
	VolcanoID        string
	AlertLevel       string
	EruptionCount24h int
	AshColumnHeightM *float64
	ReportedAt       time.Time
	Attributes       map[string]any
}

const (
	VolcanicAdapterV1 = 1
	VolcanicAdapterV2 = 2
)

var volcanicAdapter atomic.Int32

func init() { volcanicAdapter.Store(VolcanicAdapterV1) }

func SetVolcanicAdapter(version int) error {
	if version != VolcanicAdapterV1 && version != VolcanicAdapterV2 {
		return fmt.Errorf("unsupported PVMBG adapter version %d", version)
	}
	volcanicAdapter.Store(int32(version))
	return nil
}

func VolcanicAdapter() int { return int(volcanicAdapter.Load()) }

func VolcanicAdapterName() string { return fmt.Sprintf("pvmbg-adapter-v%d", VolcanicAdapter()) }

func decodeVolcanicReport(raw map[string]interface{}) (VolcanicReport, error) {
	var r VolcanicReport
	var err error
	adapted := VolcanicAdapter() == VolcanicAdapterV2
	if r.ReportID, err = requiredString(raw, "report_id"); err != nil {
		return r, err
	}
	if r.VolcanoID, err = requiredString(raw, "volcano_id"); err != nil {
		return r, err
	}
	levelField := "alert_level"
	if _, ok := raw[levelField]; !ok && adapted {
		levelField = "level"
	}
	if r.AlertLevel, err = requiredString(raw, levelField); err != nil {
		return r, err
	}
	switch r.AlertLevel {
	case "Normal", "Waspada", "Siaga", "Awas":
	default:
		return r, fmt.Errorf("%w: invalid alert_level %q", ErrContractViolation, r.AlertLevel)
	}
	if r.EruptionCount24h, err = eruptionCount(raw, adapted); err != nil {
		return r, err
	}
	if value, present := raw["ash_column_height_m"]; present && value != nil || !adapted {
		ash, err := requiredNumber(raw, "ash_column_height_m")
		if err != nil {
			return r, err
		}
		r.AshColumnHeightM = &ash
	}
	reportedAt, err := requiredString(raw, "reported_at")
	if err != nil {
		return r, err
	}
	r.ReportedAt, err = time.Parse(time.RFC3339Nano, reportedAt)
	if err != nil {
		return r, fmt.Errorf("%w: invalid required field %q: %v", ErrContractViolation, "reported_at", err)
	}
	r.Attributes = unmappedFields(raw, "report_id", "volcano_id", levelField, "reported_at", "eruption_count_24h", "ash_column_height_m")
	return r, nil
}

func eruptionCount(raw map[string]any, adapted bool) (int, error) {
	text, isText := raw["eruption_count_24h"].(string)
	if !isText || !adapted {
		return requiredInt(raw, "eruption_count_24h")
	}
	n, err := strconv.ParseUint(text, 10, 31)
	if err != nil {
		return 0, fmt.Errorf("%w: eruption_count_24h %q is not a nonnegative integer", ErrContractViolation, text)
	}
	return int(n), nil
}

type PVMBGClient struct {
	BeforeFetch    func(context.Context, string) error
	CaptureFailure FailureRecorder
	BaseURL        string
	Token          string
	HTTP           *http.Client
	Logger         *slog.Logger
	Registry       *avrowire.Client
	Reader         *avrowire.SourceReader
	ReaderSwitch   *avrowire.ReaderSwitch
}

func NewPVMBGClient(baseURL, token string, logger *slog.Logger) *PVMBGClient {
	return &PVMBGClient{
		BaseURL: baseURL,
		Token:   token,
		HTTP:    platform.Client(5 * time.Second),
		Logger:  logger,
	}
}

type ParkedPayload struct {
	ID        string
	Entity    string
	Payload   []byte
	Truncated bool
	Rejection string
}

func (c *PVMBGClient) Reprocess(ctx context.Context, p ParkedPayload) ([]VolcanicReport, error) {
	if c.Registry == nil {
		if err := platform.ValidateJSON(p.Payload); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrContractViolation, err)
		}
	}
	if p.Rejection != "" {
		return nil, fmt.Errorf("%w: %s", ErrContractViolation, p.Rejection)
	}
	if p.Truncated || len(p.Payload) == 0 {
		return nil, fmt.Errorf("%w: original response bytes were not retained completely", ErrContractViolation)
	}
	reader := c.Reader
	if c.ReaderSwitch != nil {
		var release func()
		var err error
		ctx, reader, release, err = c.ReaderSwitch.Begin(ctx)
		if err != nil {
			return nil, err
		}
		defer release()
	}
	framed := c.Registry != nil && len(p.Payload) > 5 && p.Payload[0] == 0
	if framed && reader == nil {
		return nil, fmt.Errorf("explicit reader schema required for volcanic-reports")
	}
	var raws []map[string]any
	if strings.HasPrefix(p.Entity, "history:") {
		var page HistoryPage
		if framed {
			native, err := c.Registry.DecodeWithReader(ctx, p.Payload, reader.History)
			if err != nil {
				return nil, err
			}
			b, err := json.Marshal(native)
			if err != nil {
				return nil, err
			}
			if err = json.Unmarshal(b, &page); err != nil {
				return nil, err
			}
		} else if err := json.Unmarshal(p.Payload, &page); err != nil {
			return nil, fmt.Errorf("%w: history JSON: %v", ErrContractViolation, err)
		}
		for _, item := range page.Data {
			var fields map[string]any
			if err := decodeHistoryFields(item.Payload, &fields); err != nil {
				return nil, fmt.Errorf("%w: history item: %v", ErrContractViolation, err)
			}
			raws = append(raws, fields)
		}
	} else if framed {
		native, err := c.Registry.DecodeWithReader(ctx, p.Payload, reader.Live)
		if err != nil {
			return nil, err
		}
		if raws, err = avrowire.Records(native); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrContractViolation, err)
		}
	} else if err := json.Unmarshal(p.Payload, &raws); err != nil {
		return nil, fmt.Errorf("%w: JSON: %v", ErrContractViolation, err)
	}
	reports := make([]VolcanicReport, 0, len(raws))
	for i, raw := range raws {
		r, err := decodeVolcanicReport(raw)
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", i, err)
		}
		reports = append(reports, r)
	}
	return reports, nil
}

func (c *PVMBGClient) FetchVolcanicReports(ctx context.Context, correlationID string, since time.Time) (result []VolcanicReport, retErr error) {
	reader := c.Reader
	if c.ReaderSwitch != nil {
		var release func()
		var err error
		ctx, reader, release, err = c.ReaderSwitch.Begin(ctx)
		if err != nil {
			return nil, err
		}
		defer release()
	}
	path := "/volcanic-reports"
	if !since.IsZero() {
		path += "?since=" + since.UTC().Format(time.RFC3339Nano)
	}

	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if c.BeforeFetch != nil {
		if err := c.BeforeFetch(callCtx, "PVMBG"); err != nil {
			return nil, err
		}
	}

	req, err := http.NewRequestWithContext(callCtx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	if c.Registry != nil {
		req.Header.Set("Accept", avrowire.MediaType)
	}
	req.Header.Set(middleware.CorrelationIDHeader, correlationID)

	start := time.Now()
	resp, err := c.HTTP.Do(req)
	latency := time.Since(start).Milliseconds()
	if err != nil {
		middleware.LogOutbound(c.Logger, "aggregator", correlationID, "pvmbg", path, 0, false, latency, err.Error())
		return nil, err
	}
	defer resp.Body.Close()
	rawBody, truncated, payloadStream, readErr := readResponseFailureBody(resp)
	defer func() {
		capture(callCtx, c.CaptureFailure, Failure{Source: "PVMBG", Entity: path, Payload: rawBody, PayloadStream: payloadStream, ContentType: resp.Header.Get("Content-Type"), Truncated: truncated, CorrelationID: correlationID}, &retErr, resp)
	}()
	if readErr != nil {
		return nil, fmt.Errorf("%w: %v", ErrContractViolation, readErr)
	}

	success := resp.StatusCode < 400
	middleware.LogOutbound(c.Logger, "aggregator", correlationID, "pvmbg", path, resp.StatusCode, success, latency, "")
	if !success {
		return nil, fmt.Errorf("pvmbg %s returned status %d", path, resp.StatusCode)
	}

	if c.Registry == nil {
		if err := platform.ValidateJSON(rawBody); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrContractViolation, err)
		}
	}
	resp.Body = io.NopCloser(bytes.NewReader(rawBody))
	var raws []map[string]interface{}
	var writer *avrowire.Schema
	if c.Registry != nil {
		if resp.Header.Get("Content-Type") != avrowire.MediaType {
			return nil, fmt.Errorf("%w: pvmbg %s did not return Avro", ErrContractViolation, path)
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		if err != nil {
			return nil, err
		}
		if reader == nil {
			return nil, fmt.Errorf("explicit reader schema required for volcanic-reports")
		}
		native, err := c.Registry.DecodeWithReader(avrowire.WithCorrelationID(callCtx, correlationID), body, reader.Live)
		if err != nil {
			return nil, fmt.Errorf("pvmbg %s: %w", path, err)
		}
		raws, err = avrowire.Records(native)
		if err != nil {
			return nil, fmt.Errorf("%w: pvmbg %s: %v", ErrContractViolation, path, err)
		}
		writer, err = c.Registry.SchemaForFrame(callCtx, body)
		if err != nil {
			return nil, err
		}
	} else if err := json.NewDecoder(resp.Body).Decode(&raws); err != nil {
		return nil, fmt.Errorf("%w: pvmbg %s JSON: %v", ErrContractViolation, path, err)
	}
	if raws == nil {
		return nil, fmt.Errorf("%w: pvmbg %s returned null instead of an array", ErrContractViolation, path)
	}
	sequence := int64(0)
	if header := resp.Header.Get("X-Source-Sequence"); header != "" {
		var err error
		sequence, err = strconv.ParseInt(header, 10, 64)
		if err != nil || sequence < 0 {
			return nil, fmt.Errorf("%w: invalid source sequence", ErrContractViolation)
		}
	}
	reports := make([]VolcanicReport, 0, len(raws))
	for _, raw := range raws {
		r, err := decodeVolcanicReport(raw)
		if err != nil {
			return nil, fmt.Errorf("pvmbg %s record %d: %w", path, len(reports), err)
		}
		r.SourceSequence = sequence
		reports = append(reports, r)
	}
	// History envelope IDs are deliberately not used for reader discovery.
	if c.ReaderSwitch != nil && writer != nil {
		if err := c.ReaderSwitch.Observe(writer); err != nil && c.Logger != nil {
			c.Logger.Warn("writer schema cannot be selected as a reader", "schema_id", writer.ID, "error", err)
		}
	}
	return reports, nil
}
