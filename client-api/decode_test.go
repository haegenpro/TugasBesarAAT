package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func validHazardJSON() map[string]any {
	return map[string]any{"hazard_id": testHazardID, "source": "BMKG", "source_ref_id": "event-1",
		"hazard_type": "SEISMIC", "severity": "NORMAL", "area_name": "", "latitude": 0, "longitude": 0,
		"occurred_at": "2026-10-09T00:00:00Z", "ingested_at": "2026-10-09T00:00:01Z", "attributes": map[string]any{}}
}

func jsonBytes(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestHazardWireRequiredFieldsInStandaloneAndList(t *testing.T) {
	for field := range validHazardJSON() {
		for _, mutation := range []string{"missing", "null", "wrong-type", "wrong-case"} {
			t.Run(field+"/"+mutation, func(t *testing.T) {
				item := validHazardJSON()
				switch mutation {
				case "missing":
					delete(item, field)
				case "null":
					item[field] = nil
				case "wrong-type":
					item[field] = false
				case "wrong-case":
					item[strings.ToUpper(field)] = item[field]
					delete(item, field)
				}
				var standalone hazard
				if err := json.Unmarshal(jsonBytes(t, item), &standalone); err == nil {
					t.Fatal("invalid standalone record accepted")
				}
				var list aggregatorList
				envelope := map[string]any{"data": []any{item}, "count": 1, "as_of": "now", "source_status": map[string]any{}}
				if err := json.Unmarshal(jsonBytes(t, envelope), &list); err == nil {
					t.Fatal("invalid nested record accepted")
				}
			})
		}
	}
}

func TestHazardWireNonemptyFieldsAndAdditiveAttributes(t *testing.T) {
	for _, field := range []string{"hazard_id", "source", "source_ref_id", "hazard_type", "severity", "occurred_at", "ingested_at"} {
		t.Run(field, func(t *testing.T) {
			item := validHazardJSON()
			item[field] = ""
			var decoded hazard
			if err := json.Unmarshal(jsonBytes(t, item), &decoded); err == nil {
				t.Fatal("empty required identity/time accepted")
			}
		})
	}
	item := validHazardJSON()
	item["new_top_level"] = "not a canonical field"
	item["attributes"] = map[string]any{"confidence_level": .85, "potential_tsunami": false,
		"additional": map[string]any{"array": []any{0., false, nil, "value"}}}
	var decoded hazard
	if err := json.Unmarshal(jsonBytes(t, item), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Latitude != 0 || decoded.Longitude != 0 || decoded.AreaName != "" || !reflect.DeepEqual(decoded.Attributes, item["attributes"]) {
		t.Fatalf("valid zeroes/empty area/additive attributes changed: %#v", decoded)
	}
	var output map[string]any
	if err := json.Unmarshal(jsonBytes(t, decoded), &output); err != nil {
		t.Fatal(err)
	}
	if _, leaked := output["new_top_level"]; leaked {
		t.Fatal("unknown top-level field forwarded")
	}
}

func TestListWirePresenceAndShapes(t *testing.T) {
	valid := func() map[string]any {
		return map[string]any{"data": []any{}, "count": 0, "as_of": "now", "source_status": map[string]any{}}
	}
	for field := range valid() {
		for _, mutation := range []string{"missing", "null", "wrong-type", "wrong-case"} {
			t.Run(field+"/"+mutation, func(t *testing.T) {
				value := valid()
				switch mutation {
				case "missing":
					delete(value, field)
				case "null":
					value[field] = nil
				case "wrong-type":
					value[field] = false
				case "wrong-case":
					value[strings.ToUpper(field)] = value[field]
					delete(value, field)
				}
				var decoded aggregatorList
				if err := json.Unmarshal(jsonBytes(t, value), &decoded); err == nil {
					t.Fatal("invalid envelope accepted")
				}
			})
		}
	}
	for _, data := range []string{
		`null`, `[]`, `{"data":[null],"count":1,"as_of":"now","source_status":{}}`,
		`{"data":[],"count":0.5,"as_of":"now","source_status":{}}`,
		`{"data":[],"count":0,"as_of":"","source_status":{}}`,
		`{"data":[],"count":0,"as_of":"now","source_status":{"BMKG":null}}`,
	} {
		var decoded aggregatorList
		if err := json.Unmarshal([]byte(data), &decoded); err == nil {
			t.Fatalf("invalid list accepted: %s", data)
		}
	}
	for _, metadata := range []any{nil, map[string]any{}} {
		value := valid()
		value["metadata"] = metadata
		var decoded aggregatorList
		if err := json.Unmarshal(jsonBytes(t, value), &decoded); err != nil || decoded.Data == nil || decoded.SourceStatus == nil || decoded.Count != 0 {
			t.Fatalf("valid empty response rejected: %#v, %v", decoded, err)
		}
	}
}

func TestStatusWireValidation(t *testing.T) {
	for _, data := range []string{`null`, `{}`, `{"STATE":"fresh"}`, `{"state":null}`, `{"state":false}`,
		`{"state":"unknown"}`, `{"state":"stale"}`, `{"state":"stale","stale_since":null}`,
		`{"state":"stale","stale_since":""}`} {
		var decoded sourceStatus
		if err := json.Unmarshal([]byte(data), &decoded); err == nil {
			t.Fatalf("invalid status accepted: %s", data)
		}
	}
	for _, data := range []string{`{"state":"fresh"}`, `{"state":"unavailable"}`,
		`{"state":"stale","stale_since":"2026-10-09T00:00:00Z","new_field":true}`} {
		var decoded sourceStatus
		if err := json.Unmarshal([]byte(data), &decoded); err != nil {
			t.Fatalf("valid status rejected: %s: %v", data, err)
		}
	}
}

func TestWireDecodeDoesNotModifyReceiverOnFailure(t *testing.T) {
	var item hazard
	if err := json.Unmarshal(jsonBytes(t, validHazardJSON()), &item); err != nil {
		t.Fatal(err)
	}
	before := item
	if err := json.Unmarshal([]byte(`{"hazard_id":"changed"}`), &item); err == nil || !reflect.DeepEqual(item, before) {
		t.Fatal("failed decode modified the previous value")
	}
}

func TestListWireRejectsInvalidRepeatedData(t *testing.T) {
	first := jsonBytes(t, map[string]any{"data": []any{validHazardJSON()}, "count": 1,
		"as_of": "now", "source_status": map[string]any{}})
	for name, last := range map[string]string{
		"missing-fields": `[{}]`, "null-record": `[null]`,
		"partial-record": `[{"hazard_id":"changed"}]`,
	} {
		t.Run(name, func(t *testing.T) {
			payload := append(append([]byte(nil), first[:len(first)-1]...), []byte(`,"data":`+last+`}`)...)
			var decoded aggregatorList
			if err := json.Unmarshal(payload, &decoded); err == nil {
				t.Fatal("invalid final data array inherited required fields from the earlier array")
			}
		})
	}
}

func TestWireRejectsDuplicateKeysAtEveryLevel(t *testing.T) {
	valid := string(jsonBytes(t, validHazardJSON()))
	for name, payload := range map[string]string{
		"hazard":     valid[:len(valid)-1] + `,"hazard_id":"second"}`,
		"attributes": strings.Replace(valid, `"attributes":{}`, `"attributes":{"x":1,"x":2}`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			var decoded hazard
			if err := json.Unmarshal([]byte(payload), &decoded); err == nil {
				t.Fatal("ambiguous duplicate key accepted")
			}
		})
	}
	var status sourceStatus
	if err := json.Unmarshal([]byte(`{"state":"fresh","state":"unavailable"}`), &status); err == nil {
		t.Fatal("duplicate source state accepted")
	}
	var list aggregatorList
	if err := json.Unmarshal([]byte(`{"data":[],"count":0,"as_of":"now","source_status":{"BMKG":{"state":"fresh","state":"unavailable"}}}`), &list); err == nil {
		t.Fatal("duplicate nested source state accepted")
	}
}

func TestDetailWrappedAndLegacyFormatsRemainSupported(t *testing.T) {
	item := validHazardJSON()
	item["attributes"] = map[string]any{"stale": true, "stale_since": "now", "confidence_level": .9}
	for _, input := range []any{item, map[string]any{"data": item, "as_of": "now", "metadata": map[string]any{},
		"source_status": map[string]any{"BMKG": map[string]any{"state": "stale", "stale_since": "now"}}}} {
		var decoded aggregatorDetail
		if err := json.Unmarshal(jsonBytes(t, input), &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.Data.HazardID != testHazardID || decoded.Data.Attributes["confidence_level"] != .9 || decoded.SourceStatus["BMKG"].State != "stale" {
			t.Fatalf("detail compatibility/freshness lost: %#v", decoded)
		}
	}
}
