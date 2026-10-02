package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRenamedAggregatorFieldFailsToDecode(t *testing.T) {
	var item hazard
	err := json.Unmarshal([]byte(`{"hazard_code":"id","source":"PVMBG","source_ref_id":"rep-1","hazard_type":"VOLCANIC","severity":"SIAGA","area_name":"Merapi","latitude":-7.5,"longitude":110.4,"occurred_at":"2026-09-30T00:00:00Z","ingested_at":"2026-09-30T00:00:01Z","attributes":{}}`), &item)
	if err == nil || !strings.Contains(err.Error(), `"hazard_id"`) {
		t.Fatalf("renamed hazard_id returned %v", err)
	}

	var list aggregatorList
	err = json.Unmarshal([]byte(`{"records":[],"count":0,"as_of":"2026-09-30T00:00:00Z","source_status":{}}`), &list)
	if err == nil || !strings.Contains(err.Error(), `"data"`) {
		t.Fatalf("renamed data returned %v", err)
	}
}
