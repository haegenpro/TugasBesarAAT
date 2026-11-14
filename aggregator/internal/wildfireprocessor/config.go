// Package wildfireprocessor implements resumable source-ordered processing.
// It never instantiates the fixture-sized wildfire.Model with session history.
package wildfireprocessor

import (
	"aggregator/internal/rawinput"
	"aggregator/internal/wildfire"
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"time"
)

const ContentType = "application/vnd.bnpb.satellite-fixture+json;version=1"

type Config struct {
	Version                int             `json:"version"`
	Namespace              string          `json:"namespace"`
	InitialOffset          *int64          `json:"initial_offset"`
	AlgorithmVersion       int             `json:"algorithm_version"`
	AllowedLatenessSeconds *int64          `json:"allowed_lateness_seconds"`
	ProvinceFile           string          `json:"province_file"`
	ProvinceSHA256         string          `json:"province_sha256"`
	ProvinceJSON           json.RawMessage `json:"-"`
	PageRows               int             `json:"page_rows"`
	CacheBytes             int64           `json:"cache_bytes"`
	StagingBytes           int64           `json:"staging_bytes"`
	PreparedBytes          int64           `json:"prepared_bytes"`
}

func (c Config) validate() ([]wildfire.Province, string, error) {
	if c.AlgorithmVersion != 2 || c.PageRows < 1 || c.PageRows > 1000 || c.CacheBytes < 4096 || c.StagingBytes < 4096 || c.PreparedBytes < 1024 || c.PreparedBytes > c.CacheBytes/4 || int64(c.PageRows) > c.CacheBytes/256 {
		return nil, "", fmt.Errorf("explicit bounded execution quotas and algorithm version 2 required")
	}
	// New validates only the small immutable configuration; no history is loaded.
	_, err := wildfire.New(wildfire.Config{Version: c.Version, AlgorithmVersion: 2, Namespace: c.Namespace, InitialOffset: c.InitialOffset, AllowedLatenessSeconds: c.AllowedLatenessSeconds, ProvinceSHA256: c.ProvinceSHA256, ProvinceJSON: c.ProvinceJSON})
	if err != nil {
		return nil, "", err
	}
	var provinces []wildfire.Province
	if int64(len(c.ProvinceJSON)) > c.CacheBytes/4 {
		return nil, "", fmt.Errorf("province table exceeds cache budget")
	}
	if err = wildfire.Decode(c.ProvinceJSON, &provinces); err != nil {
		return nil, "", err
	}
	semantic := struct {
		Namespace       string
		Start, Lateness int64
		Algorithm       int
		Provinces       string
	}{c.Namespace, *c.InitialOffset, *c.AllowedLatenessSeconds, 2, c.ProvinceSHA256}
	b, _ := json.Marshal(semantic)
	return provinces, rawinput.Hash(b), nil
}

// DecodeOperation preserves the complete normalized wrapper and nested original
// bytes separately; this adapter deliberately does not guess live-feed payloads.
func DecodeOperation(e rawinput.Envelope, payload []byte, limit int64) (wildfire.Operation, error) {
	var op wildfire.Operation
	if err := rawinput.Validate(e); err != nil {
		return op, err
	}
	if e.Source != "SATELIT" || e.ContentType != ContentType || int64(len(payload)) > limit || int64(len(payload)) != e.Size || rawinput.Hash(payload) != e.Digest {
		return op, fmt.Errorf("unsupported adapter, quota or byte integrity")
	}
	if err := wildfire.Decode(payload, &op); err != nil {
		return op, err
	}
	if op.Version != 1 || op.Namespace != e.Namespace || e.Position == nil || op.Offset != *e.Position || op.Offset < 0 || op.Offset >= math.MaxInt64 || op.Kind != e.Operation {
		return op, fmt.Errorf("envelope/operation disagreement")
	}
	if (op.VirtualNow == nil) != (e.VirtualNow == nil) {
		return op, fmt.Errorf("clock disagreement")
	}
	if op.VirtualNow != nil {
		if op.VirtualNow.IsZero() || op.VirtualNow.Year() < 1 || op.VirtualNow.Year() > 9999 {
			return op, fmt.Errorf("invalid virtual clock")
		}
		at, err := time.Parse(time.RFC3339Nano, *e.VirtualNow)
		if err != nil || !op.VirtualNow.Equal(at) {
			return op, fmt.Errorf("clock disagreement")
		}
	}
	if (op.Kind == "detection" || op.Kind == "update") != (op.Detection != nil) || (op.Kind == "update" || op.Kind == "retract") != (op.TargetKey != "") {
		return op, fmt.Errorf("invalid operation members")
	}
	if op.Kind != "detection" && op.Kind != "update" && op.Kind != "retract" && op.Kind != "heartbeat" {
		return op, fmt.Errorf("unknown operation")
	}
	if op.Detection != nil {
		if _, err := wildfire.ParseDetection(*op.Detection, op.Offset, op.Payload); err != nil {
			return op, err
		}
	}
	return op, nil
}

// FixtureEnvelope is a development publisher adapter, never a live connector.
func FixtureEnvelope(line []byte, received string, scope string, epoch int64, holder string) (rawinput.Envelope, error) {
	var op wildfire.Operation
	if err := wildfire.Decode(bytes.TrimSuffix(line, []byte{'\r'}), &op); err != nil {
		return rawinput.Envelope{}, err
	}
	e := rawinput.Envelope{Version: 1, Source: "SATELIT", Namespace: op.Namespace, Position: &op.Offset, Operation: op.Kind, ReceivedAt: received, Payload: append([]byte(nil), line...), Size: int64(len(line)), Digest: rawinput.Hash(line), ContentType: ContentType, CorrelationID: rawinput.Identity("fixture-correlation-v1", op.Namespace, op.Offset), OwnershipScope: scope, OwnershipEpoch: epoch, OwnershipHolder: holder}
	if op.VirtualNow != nil {
		v := op.VirtualNow.UTC().Format(time.RFC3339Nano)
		e.VirtualNow = &v
	}
	e.MessageID = rawinput.ID(e)
	_, err := DecodeOperation(e, line, int64(len(line)))
	return e, err
}
