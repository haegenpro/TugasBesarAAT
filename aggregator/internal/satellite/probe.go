package satellite

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ProbeConfig describes a read-only bounded source contract check. Token is
// deliberately excluded from every evidence type below.
type ProbeConfig struct {
	BaseURL, Token, Namespace string
	From                      int64
	Records                   int
	Duration, Pause           time.Duration
	AllowLoopbackHTTP         bool
	HTTP                      *http.Client
}

type ProbeSession struct {
	Group        string  `json:"group"`
	Profile      string  `json:"profile"`
	Speed        float64 `json:"speed"`
	LatestOffset int64   `json:"latest_offset"`
	VirtualNow   string  `json:"virtual_now"`
}

type ProbeRecord struct {
	Offset          int64  `json:"offset"`
	Kind            string `json:"kind"`
	PayloadSHA256   string `json:"payload_sha256"`
	OperationSHA256 string `json:"operation_sha256"`
}

type ProbePhase struct {
	Name      string        `json:"name"`
	From      int64         `json:"from"`
	ElapsedMS int64         `json:"elapsed_ms"`
	Records   []ProbeRecord `json:"records"`
}

type ProbeReport struct {
	Version     int            `json:"version"`
	Passed      bool           `json:"passed"`
	Started     time.Time      `json:"started"`
	Finished    time.Time      `json:"finished"`
	BaseURL     string         `json:"base_url"`
	Namespace   string         `json:"namespace"`
	Before      *ProbeSession  `json:"before,omitempty"`
	After       *ProbeSession  `json:"after,omitempty"`
	Phases      []ProbePhase   `json:"phases"`
	Kinds       map[string]int `json:"kinds"`
	Error       string         `json:"error,omitempty"`
	Limitations []string       `json:"limitations"`
}

func probeHash(raw []byte) string {
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

// Probe uses the production SSE reader and normalizer. It does not publish,
// advance ingestion progress, change a source session, or claim end-to-end SLA.
func Probe(ctx context.Context, cfg ProbeConfig) (report ProbeReport, err error) {
	report = ProbeReport{Version: 1, Started: time.Now().UTC(), Namespace: cfg.Namespace, Kinds: map[string]int{},
		Limitations: []string{"Bounded HTTP replay/normalization only; not full source completeness or burst acceptance.", "No ingestion, canonical state, broker durability or node failure was tested.", "Only observed event kinds were exercised; use course reference data for correctness."}}
	defer func() {
		report.Finished = time.Now().UTC()
		report.Passed = err == nil
		if err != nil {
			report.Error = err.Error()
		}
	}()
	u, parseErr := url.Parse(cfg.BaseURL)
	if parseErr != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return report, errors.New("source URL must have only scheme and host")
	}
	ip := net.ParseIP(u.Hostname())
	if u.Scheme != "https" && !(cfg.AllowLoopbackHTTP && u.Scheme == "http" && ip != nil && ip.IsLoopback()) {
		return report, errors.New("source requires HTTPS; HTTP is allowed only for explicit loopback checks")
	}
	if cfg.Token == "" || strings.ContainsAny(cfg.Token, "\r\n\t ") || cfg.Namespace == "" || cfg.From < 0 || cfg.From > (1<<63)-1-4096 || cfg.Records < 2 || cfg.Records > 1024 || cfg.Duration <= 0 || cfg.Duration > 5*time.Minute || cfg.Pause < 0 || cfg.Pause > time.Minute || cfg.Pause >= cfg.Duration {
		return report, errors.New("invalid probe bounds, token or namespace")
	}
	report.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	ctx, cancel := context.WithTimeout(ctx, cfg.Duration)
	defer cancel()
	client := &http.Client{}
	if cfg.HTTP != nil {
		*client = *cfg.HTTP
	}
	// Never forward a bearer credential to a redirect, including another origin.
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	request := func(path, accept string) (*http.Response, error) {
		req, requestErr := http.NewRequestWithContext(ctx, http.MethodGet, report.BaseURL+path, nil)
		if requestErr != nil {
			return nil, errors.New("cannot construct source request")
		}
		req.Header.Set("Authorization", "Bearer "+cfg.Token)
		req.Header.Set("User-Agent", UserAgent)
		req.Header.Set("Accept", accept)
		resp, requestErr := client.Do(req)
		if requestErr != nil {
			return nil, errors.New("source request failed or exceeded probe deadline")
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("source returned HTTP %d", resp.StatusCode)
		}
		return resp, nil
	}
	session := func() (*ProbeSession, error) {
		resp, e := request("/v1/session", "application/json")
		if e != nil {
			return nil, e
		}
		defer resp.Body.Close()
		raw, e := io.ReadAll(io.LimitReader(resp.Body, (64<<10)+1))
		if e != nil || len(raw) > 64<<10 {
			return nil, errors.New("source session response is unreadable or too large")
		}
		var s ProbeSession
		if json.Unmarshal(raw, &s) != nil || s.Group == "" || s.Profile == "" || s.Speed <= 0 || s.LatestOffset < 0 {
			return nil, errors.New("source session identity or playback metadata is missing")
		}
		return &s, nil
	}
	report.Before, err = session()
	if err != nil {
		return report, err
	}
	seen := map[int64]ProbeRecord{}
	phase := func(name string, from int64, count int) error {
		report.Phases = append(report.Phases, ProbePhase{Name: name, From: from})
		p := &report.Phases[len(report.Phases)-1]
		started := time.Now()
		defer func() { p.ElapsedMS = time.Since(started).Milliseconds() }()
		resp, e := request("/v1/stream?from="+strconv.FormatInt(from, 10), "text/event-stream")
		if e != nil {
			return e
		}
		defer resp.Body.Close()
		if !strings.HasPrefix(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream") {
			return errors.New("source did not return an SSE content type")
		}
		stream := NewStream(io.LimitReader(resp.Body, 64<<20), 1<<20)
		expected := from
		for len(p.Records) < count {
			event, e := stream.Next()
			if e != nil {
				return errors.New("source stream ended, exceeded a bound, or timed out before requested records")
			}
			n, e := Normalize(cfg.Namespace, event)
			if n.Offset < 0 && event.Type == "heartbeat" {
				continue
			}
			if e != nil {
				return errors.New("production normalizer rejected a source record")
			}
			if n.Offset != expected {
				return fmt.Errorf("noncontiguous inclusive replay: expected %d, got %d", expected, n.Offset)
			}
			record := ProbeRecord{Offset: n.Offset, Kind: n.Kind, PayloadSHA256: probeHash(event.Data), OperationSHA256: probeHash(n.Operation)}
			if old, ok := seen[n.Offset]; ok && old != record {
				return fmt.Errorf("replayed offset %d changed type or payload", n.Offset)
			}
			seen[n.Offset] = record
			p.Records = append(p.Records, record)
			report.Kinds[n.Kind]++
			expected++
		}
		return nil
	}
	if err = phase("initial", cfg.From, cfg.Records); err != nil {
		return report, err
	}
	if cfg.Pause > 0 {
		timer := time.NewTimer(cfg.Pause)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return report, errors.New("probe deadline elapsed during deliberate disconnect")
		case <-timer.C:
		}
	}
	last := cfg.From + int64(cfg.Records) - 1
	if err = phase("overlap-resume", last, cfg.Records); err != nil {
		return report, err
	}
	if err = phase("next-offset-resume", last+1, cfg.Records); err != nil {
		return report, err
	}
	report.After, err = session()
	if err != nil {
		return report, err
	}
	if report.Before.Group != report.After.Group || report.Before.Profile != report.After.Profile || report.Before.Speed != report.After.Speed || report.After.LatestOffset < report.Before.LatestOffset {
		return report, errors.New("source session identity, playback rate or history changed during probe")
	}
	if report.Kinds["detection"]+report.Kinds["update"]+report.Kinds["retract"] == 0 {
		return report, errors.New("probe observed no data records")
	}
	return report, nil
}
