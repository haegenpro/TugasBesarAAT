package main

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

// BenchmarkP2JSON measures the actual response processing used by listHazards.
// Supply P2_PAYLOAD_PATH with a saved Aggregator /internal/hazards response.
func BenchmarkP2JSON(b *testing.B) {
	path := os.Getenv("P2_PAYLOAD_PATH")
	if path == "" {
		b.Skip("set P2_PAYLOAD_PATH to a saved hazard-list response")
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		b.Fatal(err)
	}
	var list aggregatorList
	if err := json.Unmarshal(payload, &list); err != nil {
		b.Fatal(err)
	}
	b.Run("ValidatedDecode", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(payload)))
		for b.Loop() {
			var decoded aggregatorList
			if err := json.NewDecoder(bytes.NewReader(payload)).Decode(&decoded); err != nil {
				b.Fatal(err)
			}
		}
	})
	// This is a cost baseline, not a proposed replacement: it intentionally
	// omits required-field validation and therefore has different guarantees.
	b.Run("DecodeWithoutValidationBaseline", func(b *testing.B) {
		type plainHazard hazard
		type plainStatus sourceStatus
		type plainList struct {
			Data         []plainHazard             `json:"data"`
			Count        int                       `json:"count"`
			AsOf         string                    `json:"as_of"`
			SourceStatus map[string]plainStatus    `json:"source_status"`
			Metadata     map[string]recordMetadata `json:"metadata"`
		}
		b.ReportAllocs()
		b.SetBytes(int64(len(payload)))
		for b.Loop() {
			var decoded plainList
			if err := json.NewDecoder(bytes.NewReader(payload)).Decode(&decoded); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("EncodeRawResponse", func(b *testing.B) {
		response := map[string]any{"data": list.Data, "count": len(list.Data), "as_of": list.AsOf,
			"source_status": list.SourceStatus, "metadata": list.Metadata}
		b.ReportAllocs()
		for b.Loop() {
			if _, err := json.Marshal(response); err != nil {
				b.Fatal(err)
			}
		}
	})
}
