// Package avrowire provides Avro framing and schema-registry access.
// It contains transport code only; source and hazard mapping stay in each service.
package avrowire

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/linkedin/goavro/v2"
)

const MediaType = "application/avro"

type Schema struct {
	ID    int
	Text  string
	Codec *goavro.Codec
	plan  *schemaPlan
}

type Client struct {
	base      string
	http      *http.Client
	logger    *slog.Logger
	service   string
	mu        sync.RWMutex
	byID      map[int]*Schema
	bySubject map[string]*Schema
}

type correlationIDKey struct{}

func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationIDKey{}, id)
}

func (c *Client) ConfigureLogging(logger *slog.Logger, service string) {
	c.logger = logger
	c.service = service
}

func NewClient(base string) *Client {
	return &Client{base: strings.TrimRight(base, "/"), http: &http.Client{Timeout: 2 * time.Second}, byID: map[int]*Schema{}, bySubject: map[string]*Schema{}}
}

func (c *Client) request(ctx context.Context, method, path string, body any, output any) (requestErr error) {
	start := time.Now()
	status := 0
	correlationID, _ := ctx.Value(correlationIDKey{}).(string)
	defer func() {
		if c.logger == nil {
			return
		}
		level := slog.LevelInfo
		if requestErr != nil {
			level = slog.LevelWarn
		}
		attrs := []slog.Attr{
			slog.String("service", c.service),
			slog.String("correlation_id", correlationID),
			slog.String("target_service", "schema-registry"),
			slog.String("target_endpoint", method+" "+path),
			slog.Int("status_code", status),
			slog.Bool("success", requestErr == nil),
			slog.Int64("latency_ms", time.Since(start).Milliseconds()),
		}
		if requestErr != nil {
			attrs = append(attrs, slog.String("error", requestErr.Error()))
		}
		c.logger.LogAttrs(ctx, level, "outbound call", attrs...)
	}()
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	}
	if correlationID != "" {
		req.Header.Set("X-Correlation-ID", correlationID)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	status = resp.StatusCode
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("schema registry %s: status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(message)))
	}
	if output != nil {
		return json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(output)
	}
	return nil
}

// Register applies the compatibility rule before submitting a version.
// Successful IDs are cached, so an outage does not affect known schemas.
func (c *Client) Register(ctx context.Context, subject, schemaText string) (*Schema, error) {
	key := subject + "\x00" + schemaText
	c.mu.RLock()
	cached := c.bySubject[key]
	c.mu.RUnlock()
	if cached != nil {
		return cached, nil
	}
	codec, err := goavro.NewCodec(schemaText)
	if err != nil {
		return nil, err
	}
	plan, err := compilePlan(schemaText)
	if err != nil {
		return nil, err
	}
	part := url.PathEscape(subject)
	if err := c.request(ctx, http.MethodPut, "/config/"+part, map[string]string{"compatibility": "FULL_TRANSITIVE"}, nil); err != nil {
		return nil, err
	}
	var result struct {
		ID int `json:"id"`
	}
	if err := c.request(ctx, http.MethodPost, "/subjects/"+part+"/versions", map[string]string{"schemaType": "AVRO", "schema": schemaText}, &result); err != nil {
		return nil, err
	}
	if result.ID <= 0 {
		return nil, errors.New("schema registry returned invalid ID")
	}
	schema := &Schema{ID: result.ID, Text: schemaText, Codec: codec, plan: plan}
	c.mu.Lock()
	c.bySubject[key] = schema
	c.byID[result.ID] = schema
	c.mu.Unlock()
	return schema, nil
}

func (c *Client) Lookup(ctx context.Context, id int) (*Schema, error) {
	c.mu.RLock()
	cached := c.byID[id]
	c.mu.RUnlock()
	if cached != nil {
		return cached, nil
	}
	var result struct {
		Schema string `json:"schema"`
	}
	if err := c.request(ctx, http.MethodGet, "/schemas/ids/"+strconv.Itoa(id), nil, &result); err != nil {
		return nil, err
	}
	codec, err := goavro.NewCodec(result.Schema)
	if err != nil {
		return nil, err
	}
	plan, err := compilePlan(result.Schema)
	if err != nil {
		return nil, err
	}
	schema := &Schema{ID: id, Text: result.Schema, Codec: codec, plan: plan}
	c.mu.Lock()
	c.byID[id] = schema
	c.mu.Unlock()
	return schema, nil
}

func Encode(schema *Schema, native any) ([]byte, error) {
	data, err := schema.Codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 5, 5+len(data))
	binary.BigEndian.PutUint32(out[1:], uint32(schema.ID))
	return append(out, data...), nil
}

func (c *Client) Decode(ctx context.Context, framed []byte) (any, error) {
	if len(framed) < 5 || framed[0] != 0 {
		return nil, errors.New("invalid Avro schema ID frame")
	}
	id := int(binary.BigEndian.Uint32(framed[1:5]))
	schema, err := c.Lookup(ctx, id)
	if err != nil {
		return nil, err
	}
	native, rest, err := schema.Codec.NativeFromBinary(framed[5:])
	if err != nil {
		return nil, err
	}
	if len(rest) != 0 {
		return nil, errors.New("trailing Avro bytes")
	}
	return schema.plan.walk(native, "$", 0, nil)
}

func Records(value any) ([]map[string]any, error) {
	items, ok := value.([]any)
	if !ok {
		return nil, errors.New("Avro response is not an array")
	}
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		record, ok := item.(map[string]any)
		if !ok {
			return nil, errors.New("Avro array item is not a record")
		}
		out = append(out, record)
	}
	return out, nil
}
