package canonical

import (
	"aggregator/internal/domain"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

func (c Client) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.BaseURL, "/")+path, body)
	if err != nil {
		return Unavailable()
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return Unavailable()
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var problem struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
				Details any    `json:"details"`
			} `json:"error"`
		}
		if json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&problem) != nil || problem.Error.Code == "" {
			return Unavailable()
		}
		return &Error{Status: resp.StatusCode, Code: problem.Error.Code, Message: problem.Error.Message, Details: problem.Error.Details}
	}
	if out == nil {
		return nil
	}
	if json.NewDecoder(io.LimitReader(resp.Body, InlineLimit)).Decode(out) != nil {
		return Unavailable()
	}
	return nil
}

func (c Client) ApplyBatch(ctx context.Context, b Batch) (Result, error) {
	var r Result
	err := c.do(ctx, http.MethodPost, "/internal/canonical/batches", b, &r)
	return r, err
}

func (c Client) Reconcile(ctx context.Context, source domain.Source, ref string) (Entity, error) {
	var e Entity
	q := url.Values{"source": {string(source)}, "source_ref_id": {ref}}
	err := c.do(ctx, http.MethodGet, "/internal/canonical/entities?"+q.Encode(), nil, &e)
	return e, err
}

func (c Client) Acquire(ctx context.Context, r AcquireOwnership) (Lease, error) {
	var l Lease
	err := c.do(ctx, http.MethodPost, "/internal/ownership/acquire", r, &l)
	return l, err
}

func (c Client) Renew(ctx context.Context, o Ownership) (Lease, error) {
	var l Lease
	err := c.do(ctx, http.MethodPost, "/internal/ownership/renew", o, &l)
	return l, err
}

func (c Client) Release(ctx context.Context, o Ownership) error {
	return c.do(ctx, http.MethodPost, "/internal/ownership/release", o, nil)
}
