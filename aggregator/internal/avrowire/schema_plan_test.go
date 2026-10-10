package avrowire

import (
	"context"
	"encoding/json"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/require"
)

func decodeLocal(t *testing.T, text string, value any) (any, error) {
	t.Helper()
	codec, e := goavro.NewCodec(text)
	require.NoError(t, e)
	plan, e := compilePlan(text)
	require.NoError(t, e)
	schema := &Schema{ID: 42, Text: text, Codec: codec, plan: plan}
	c := NewClient("http://unused")
	c.byID[42] = schema
	b, e := Encode(schema, value)
	if e != nil {
		return nil, e
	}
	return c.Decode(context.Background(), b)
}
func TestBoundsNullableAndStructuralTyping(t *testing.T) {
	text := `{"type":"record","name":"Report","fields":[{"name":"score","type":["null","double"],"default":null,"x-validation":{"minimum":0,"maximum":1}}]}`
	for _, v := range []any{nil, goavro.Union("double", 0.0), goavro.Union("double", 1.0), goavro.Union("double", 0.5)} {
		_, e := decodeLocal(t, text, map[string]any{"score": v})
		require.NoError(t, e)
	}
	for _, v := range []any{goavro.Union("double", -0.1), goavro.Union("double", 1.5), goavro.Union("double", math.NaN()), goavro.Union("double", math.Inf(1)), goavro.Union("double", "ABC")} {
		_, e := decodeLocal(t, text, map[string]any{"score": v})
		require.Error(t, e)
	}
	_, e := decodeLocal(t, text, map[string]any{"score": goavro.Union("double", 1.5)})
	require.ErrorContains(t, e, "$.score: maximum")
}
func TestScalarAllowlistsAndBoundsAreCombined(t *testing.T) {
	for _, tc := range []struct {
		typ, allowed string
		good, bad    any
	}{
		{"double", `[0.1,0.5,1]`, 0.5, 0.6}, {"float", `[0.1,0.5]`, float32(.1), float32(.2)},
		{"int", `[1,2]`, int32(1), int32(3)}, {"long", `[9007199254740993]`, int64(9007199254740993), int64(9007199254740992)},
		{"string", `["normal","alert"]`, "normal", "other"}, {"boolean", `[true]`, true, false},
	} {
		t.Run(tc.typ, func(t *testing.T) {
			text := `{"type":"record","name":"Example","fields":[{"name":"value","type":"` + tc.typ + `","x-validation":{"allowedValues":` + tc.allowed + `}}]}`
			_, e := decodeLocal(t, text, map[string]any{"value": tc.good})
			require.NoError(t, e)
			_, e = decodeLocal(t, text, map[string]any{"value": tc.bad})
			require.ErrorContains(t, e, "allowedValues")
		})
	}
	text := `{"type":"record","name":"Example","fields":[{"name":"value","type":"double","x-validation":{"allowedValues":[0.5,1.5],"minimum":0,"maximum":1}}]}`
	_, e := decodeLocal(t, text, map[string]any{"value": 1.5})
	require.ErrorContains(t, e, "maximum")
}
func TestMalformedRulesAreRejectedAtCompile(t *testing.T) {
	for _, rule := range []string{`{"min":0}`, `{"minimum":"0"}`, `{"minimum":2,"maximum":1}`, `{"allowedValues":[]}`, `{"allowedValues":["0"]}`, `{"allowedValues":[{}]}`, `[]`} {
		_, e := compilePlan(`{"type":"record","name":"X","fields":[{"name":"v","type":"double","x-validation":` + rule + `}]}`)
		require.Error(t, e, rule)
	}
	for _, typ := range []string{`"null"`, `["null"]`, `"string"`, `{"type":"array","items":"double"}`, `{"type":"long","logicalType":"timestamp-millis"}`} {
		_, e := compilePlan(`{"type":"record","name":"X","fields":[{"name":"v","type":` + typ + `,"x-validation":{"minimum":0}}]}`)
		require.Error(t, e, typ)
	}
}
func TestSchemaAwareMapsNamedUnionsAndLargeIntegers(t *testing.T) {
	text := `{"type":"record","name":"Root","namespace":"test","fields":[{"name":"map","type":{"type":"map","values":["null","long"]}},{"name":"child","type":["null",{"type":"record","name":"Child","fields":[{"name":"value","type":"long"}]}]},{"name":"again","type":["null","Child"]}]}`
	child := goavro.Union("test.Child", map[string]any{"value": int64(9007199254740993)})
	v, e := decodeLocal(t, text, map[string]any{"map": map[string]any{"long": goavro.Union("long", int64(9007199254740993)), "empty": nil}, "child": child, "again": child})
	require.NoError(t, e)
	m := v.(map[string]any)
	require.Equal(t, int64(9007199254740993), m["map"].(map[string]any)["long"])
	require.Contains(t, m["map"], "empty")
	require.Equal(t, int64(9007199254740993), m["again"].(map[string]any)["value"])
	b, e := json.Marshal(v)
	require.NoError(t, e)
	require.Contains(t, string(b), "9007199254740993")
}
func TestNestedConstraintsAndRecursiveReferences(t *testing.T) {
	text := `{"type":"record","name":"Node","fields":[{"name":"values","type":{"type":"array","items":{"type":"double","x-validation":{"maximum":1}}}},{"name":"next","type":["null","Node"],"default":null}]}`
	_, e := decodeLocal(t, text, map[string]any{"values": []any{0.5}, "next": goavro.Union("Node", map[string]any{"values": []any{1.5}, "next": nil})})
	require.ErrorContains(t, e, "$.next.values[0]: maximum")
	// A real map with a primitive type name as its only key is not a union.
	v, e := decodeLocal(t, `{"type":"map","values":"string"}`, map[string]any{"double": "ABC"})
	require.NoError(t, e)
	require.Equal(t, map[string]any{"double": "ABC"}, v)
}
func TestLogicalTypesNormalizeExplicitly(t *testing.T) {
	instant := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, tc := range []struct {
		schema       string
		native, want any
	}{
		{`["null",{"type":"long","logicalType":"timestamp-millis"}]`, goavro.Union("long.timestamp-millis", instant), instant.Format(time.RFC3339Nano)},
		{`{"type":"int","logicalType":"date"}`, time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), "2026-01-02"},
		{`{"type":"int","logicalType":"time-millis"}`, time.Second, int64(1000)},
		{`{"type":"bytes","logicalType":"decimal","precision":6,"scale":2}`, big.NewRat(1234, 100), "12.34"},
		{`{"type":"string","logicalType":"uuid"}`, "abc", "abc"},
	} {
		v, e := decodeLocal(t, tc.schema, tc.native)
		require.NoError(t, e)
		require.Equal(t, tc.want, v)
	}
}
func TestEnumFixedAndUnionAreStillAvroTyped(t *testing.T) {
	_, e := decodeLocal(t, `{"type":"enum","name":"Status","symbols":["OK","ALERT"]}`, "BAD")
	require.Error(t, e)
	_, e = decodeLocal(t, `{"type":"fixed","name":"ID","size":2}`, []byte{1})
	require.Error(t, e)
}

func TestLogicalFixedUnionAndMixedScalarUnion(t *testing.T) {
	text := `["null",{"type":"fixed","name":"Money","namespace":"test","size":4,"logicalType":"decimal","precision":8,"scale":2}]`
	v, e := decodeLocal(t, text, goavro.Union("test.Money", big.NewRat(1234, 100)))
	require.NoError(t, e)
	require.Equal(t, "12.34", v)
	text = `{"type":"record","name":"Mixed","fields":[{"name":"v","type":["null","double","string","boolean"],"x-validation":{"allowedValues":[0.5,"ready",true]}}]}`
	for _, v := range []any{nil, goavro.Union("double", .5), goavro.Union("string", "ready"), goavro.Union("boolean", true)} {
		_, e := decodeLocal(t, text, map[string]any{"v": v})
		require.NoError(t, e)
	}
	_, e = decodeLocal(t, text, map[string]any{"v": goavro.Union("string", "0.5")})
	require.ErrorContains(t, e, "allowedValues")
}
func TestMapValuesAndFloatBounds(t *testing.T) {
	text := `{"type":"map","values":{"type":"float","x-validation":{"minimum":0.1,"maximum":0.3}}}`
	_, e := decodeLocal(t, text, map[string]any{"float": float32(.1), "upper": float32(.3)})
	require.NoError(t, e)
	_, e = decodeLocal(t, text, map[string]any{"float": float32(.4)})
	require.ErrorContains(t, e, `$["float"]: maximum`)
	for _, rule := range []string{`null`, `{"maximum":1e400}`} {
		_, e := compilePlan(`{"type":"record","name":"X","fields":[{"name":"v","type":"double","x-validation":` + rule + `}]}`)
		require.Error(t, e)
	}
}

func TestObjectReferencesKeepNamedRulesAndLogicalTypes(t *testing.T) {
	text := `{"type":"record","name":"Root","fields":[{"name":"status","type":{"type":"enum","name":"Status","symbols":["OK","ALERT"],"x-validation":{"allowedValues":["OK"]}}},{"name":"again","type":{"type":"Status"}},{"name":"money","type":{"type":"fixed","name":"Money","size":4,"logicalType":"decimal","precision":8,"scale":2}},{"name":"amount","type":{"type":"Money"}}]}`
	record := map[string]any{"status": "OK", "again": "OK", "money": big.NewRat(1234, 100), "amount": big.NewRat(1234, 100)}
	v, e := decodeLocal(t, text, record)
	require.NoError(t, e)
	require.Equal(t, "12.34", v.(map[string]any)["amount"])
	record["again"] = "ALERT"
	_, e = decodeLocal(t, text, record)
	require.ErrorContains(t, e, "$.again: allowedValues")
	text = `{"type":"record","name":"Node","fields":[{"name":"value","type":"long"},{"name":"next","type":["null",{"type":"Node"}],"default":null}]}`
	v, e = decodeLocal(t, text, map[string]any{"value": int64(1), "next": goavro.Union("Node", map[string]any{"value": int64(2), "next": nil})})
	require.NoError(t, e)
	require.Equal(t, int64(2), v.(map[string]any)["next"].(map[string]any)["value"])
}
