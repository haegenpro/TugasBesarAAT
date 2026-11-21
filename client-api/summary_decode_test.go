package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestSummaryWireRequiresSummaryFieldsAndRejectsAmbiguity(t *testing.T) {
	valid := func() map[string]any {
		return map[string]any{"hazard_id": testHazardID, "source": "BMKG", "hazard_type": "SEISMIC", "severity": "AWAS", "area_name": "", "occurred_at": "now", "ingested_at": "now"}
	}
	decode := func(record any) (aggregatorSummaryList, error) {
		var l aggregatorSummaryList
		b := jsonBytes(t, map[string]any{"data": []any{record}, "count": 1, "as_of": "now", "source_status": map[string]any{}})
		err := json.Unmarshal(b, &l)
		return l, err
	}
	for key := range valid() {
		for _, mutation := range []string{"missing", "null", "wrong-case", "wrong-type"} {
			t.Run(key+"/"+mutation, func(t *testing.T) {
				r := valid()
				switch mutation {
				case "missing":
					delete(r, key)
				case "null":
					r[key] = nil
				case "wrong-case":
					r[strings.ToUpper(key)] = r[key]
					delete(r, key)
				case "wrong-type":
					r[key] = true
				}
				if _, err := decode(r); err == nil {
					t.Fatal("invalid summary accepted")
				}
			})
		}
	}
	for _, key := range []string{"source", "hazard_type", "severity"} {
		r := valid()
		r[key] = "unknown"
		if _, err := decode(r); err == nil {
			t.Fatal("invalid enum accepted")
		}
	}
	l, err := decode(valid())
	if err != nil || len(l.Data) != 1 {
		t.Fatalf("valid summary rejected: %v", err)
	}
	before := l
	if err := json.Unmarshal([]byte(`{"data":[],"data":[],"count":0,"as_of":"now","source_status":{}}`), &l); err == nil || !reflect.DeepEqual(l, before) {
		t.Fatal("ambiguous input accepted or failed decode changed value")
	}
	if err := json.Unmarshal([]byte(`{"data":[],"count":1,"as_of":"now","source_status":{}}`), &l); err == nil {
		t.Fatal("wrong count accepted")
	}
}

func TestSummaryDecoderSafelyAcceptsOlderOwnerRawResponse(t *testing.T) {
	r := validHazardJSON()
	r["attributes"] = map[string]any{"private": "secret"}
	var l aggregatorSummaryList
	err := json.Unmarshal(jsonBytes(t, map[string]any{"data": []any{r}, "count": 1, "as_of": "now", "source_status": map[string]any{}, "partial": true, "unavailable_units": []string{"worker-2"}, "next_cursor": nil}), &l)
	if err != nil {
		t.Fatal(err)
	}
	if !l.Partial || len(l.UnavailableUnits) != 1 || l.NextCursor != nil {
		t.Fatal("partial response changed")
	}
	b := jsonBytes(t, l.Data)
	for _, key := range []string{"attributes", "latitude", "longitude", "source_ref_id", "secret"} {
		if strings.Contains(string(b), key) {
			t.Fatalf("legacy raw field leaked: %s", key)
		}
	}
}
