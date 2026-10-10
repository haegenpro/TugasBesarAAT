package avrowire

import (
	"context"
	"fmt"
	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/require"
	"testing"
)

func writerForTest(t *testing.T, c *Client, id int, text string) *Schema {
	t.Helper()
	codec, err := goavro.NewCodec(text)
	require.NoError(t, err)
	plan, err := compilePlan(text)
	require.NoError(t, err)
	s := &Schema{ID: id, Text: text, Codec: codec, plan: plan}
	c.byID[id] = s
	return s
}
func TestReaderMatrixIsIndependentOfVersionLabels(t *testing.T) {
	for _, field := range []string{"confidence_level", "quality_score"} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reverse=%t", field, reverse), func(t *testing.T) {
				base := `{"type":"array","items":{"type":"record","name":"Report","fields":[{"name":"id","type":"string"}%s]}}`
				// A non-null default proves presence tracking, rather than a null filter.
				extra := `,{"name":"` + field + `","type":["double","null"],"default":0.6,"x-validation":{"minimum":0,"maximum":1}}`
				texts := []string{fmt.Sprintf(base, ""), fmt.Sprintf(base, extra)}
				if reverse {
					texts[0], texts[1] = texts[1], texts[0]
				}
				c := NewClient("http://unused")
				writers := []*Schema{writerForTest(t, c, 1, texts[0]), writerForTest(t, c, 2, texts[1])}
				readers := make([]*Reader, 2)
				for i, text := range texts {
					var err error
					readers[i], err = NewReader(text)
					require.NoError(t, err)
				}
				for wi, w := range writers {
					for ri, r := range readers {
						t.Run(fmt.Sprintf("writer-v%d/reader-v%d", wi+1, ri+1), func(t *testing.T) {
							writerHas := wi == 1
							readerHas := ri == 1
							if reverse {
								writerHas = !writerHas
								readerHas = !readerHas
							}
							record := map[string]any{"id": "one"}
							if writerHas {
								record[field] = goavro.Union("double", 0.9)
							}
							frame, err := Encode(w, []any{record})
							require.NoError(t, err)
							value, err := c.DecodeWithReader(context.Background(), frame, r)
							require.NoError(t, err)
							rows, err := Records(value)
							require.NoError(t, err)
							require.Equal(t, "one", rows[0]["id"])
							if writerHas && readerHas {
								require.Equal(t, 0.9, rows[0][field])
							} else {
								require.NotContains(t, rows[0], field)
							}
						})
					}
				}
			})
		}
	}
}
func TestReaderDefaultsAreNotSourceFieldsRecursively(t *testing.T) {
	wtext := `{"type":"record","name":"Page","fields":[{"name":"data","type":{"type":"array","items":{"type":"record","name":"Report","fields":[{"name":"id","type":"string"}]}}}]}`
	rtext := `{"type":"record","name":"Page","fields":[{"name":"data","type":{"type":"array","items":{"type":"record","name":"Report","fields":[{"name":"id","type":"string"},{"name":"score","type":"double","default":0.75}]}}},{"name":"synthetic","type":"string","default":"not sent"}]}`
	c := NewClient("http://unused")
	w := writerForTest(t, c, 1, wtext)
	r, err := NewReader(rtext)
	require.NoError(t, err)
	frame, err := Encode(w, map[string]any{"data": []any{map[string]any{"id": "one"}}})
	require.NoError(t, err)
	value, err := c.DecodeWithReader(context.Background(), frame, r)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"data": []any{map[string]any{"id": "one"}}}, value)
}
func TestReaderValidatesDroppedWriterFieldsAndDefaults(t *testing.T) {
	base := `{"type":"record","name":"Report","fields":[{"name":"id","type":"string"}%s]}`
	extra := `,{"name":"score","type":"double","default":0.75,"x-validation":{"maximum":1}}`
	c := NewClient("http://unused")
	w := writerForTest(t, c, 1, fmt.Sprintf(base, extra))
	r, err := NewReader(fmt.Sprintf(base, ""))
	require.NoError(t, err)
	frame, err := Encode(w, map[string]any{"id": "one", "score": 1.5})
	require.NoError(t, err)
	_, err = c.DecodeWithReader(context.Background(), frame, r)
	require.ErrorContains(t, err, "score: maximum")
	w = writerForTest(t, c, 2, fmt.Sprintf(base, ""))
	r, err = NewReader(fmt.Sprintf(base, `,{"name":"score","type":"double","default":1.5,"x-validation":{"maximum":1}}`))
	require.NoError(t, err)
	frame, err = Encode(w, map[string]any{"id": "one"})
	require.NoError(t, err)
	_, err = c.DecodeWithReader(context.Background(), frame, r)
	require.ErrorContains(t, err, "score: maximum")
	r, err = NewReader(fmt.Sprintf(base, `,{"name":"score","type":"double"}`))
	require.NoError(t, err)
	_, err = c.DecodeWithReader(context.Background(), frame, r)
	require.ErrorContains(t, err, "without default")
}
func TestReaderResolvesUnionsAliasesAndPromotions(t *testing.T) {
	c := NewClient("http://unused")
	w := writerForTest(t, c, 1, `{"type":"record","name":"Old","fields":[{"name":"old","type":["null","int"]}]}`)
	r, err := NewReader(`{"type":"record","name":"New","aliases":["Old"],"fields":[{"name":"new","aliases":["old"],"type":["null","long"]}]}`)
	require.NoError(t, err)
	frame, err := Encode(w, map[string]any{"old": goavro.Union("int", int32(42))})
	require.NoError(t, err)
	value, err := c.DecodeWithReader(context.Background(), frame, r)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"new": int64(42)}, value)
}

func TestEmptyResponseCannotHideIncompatibleReader(t *testing.T) {
	c := NewClient("http://unused")
	w := writerForTest(t, c, 1, `{"type":"array","items":{"type":"record","name":"Report","fields":[{"name":"id","type":"string"}]}}`)
	r, err := NewReader(`{"type":"array","items":{"type":"record","name":"Report","fields":[{"name":"id","type":"string"},{"name":"missing","type":"double"}]}}`)
	require.NoError(t, err)
	frame, err := Encode(w, []any{})
	require.NoError(t, err)
	_, err = c.DecodeWithReader(context.Background(), frame, r)
	require.ErrorContains(t, err, "without default")
}
func TestReaderPresenceInsideMapOfUnionRecords(t *testing.T) {
	base := `{"type":"map","values":["null",{"type":"record","name":"Report","fields":[{"name":"score","type":"double"}%s]}]}`
	c := NewClient("http://unused")
	w := writerForTest(t, c, 1, fmt.Sprintf(base, ""))
	r, err := NewReader(fmt.Sprintf(base, `,{"name":"reader_only","type":"double","default":0.7}`))
	require.NoError(t, err)
	frame, err := Encode(w, map[string]any{"empty": nil, "one": goavro.Union("Report", map[string]any{"score": 0.9})})
	require.NoError(t, err)
	value, err := c.DecodeWithReader(context.Background(), frame, r)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"empty": nil, "one": map[string]any{"score": 0.9}}, value)
}
