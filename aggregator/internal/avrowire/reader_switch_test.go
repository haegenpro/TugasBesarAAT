package avrowire

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/require"
)

func TestDiscoveredReaderMatrixAndRepeatedSwitching(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reverse=%t", reverse), func(t *testing.T) {
			base := `{"type":"array","items":{"type":"record","name":"Report","fields":[{"name":"id","type":"string"}%s]}}`
			extra := `,{"name":"quality_score","type":["double","null"],"default":0.6,"x-validation":{"maximum":1}}`
			texts := []string{fmt.Sprintf(base, ""), fmt.Sprintf(base, extra)}
			if reverse {
				texts[0], texts[1] = texts[1], texts[0]
			}
			baseline, err := NewSourceReader(texts[0], "Page")
			require.NoError(t, err)
			s, err := NewReaderSwitch(baseline, "Page")
			require.NoError(t, err)
			c := NewClient("http://unused")
			// IDs need not be ordered by schema version.
			writers := []*Schema{writerForTest(t, c, 99, texts[0]), writerForTest(t, c, 3, texts[1])}
			require.NoError(t, s.Observe(writers[0]))
			_, err = s.Toggle(context.Background())
			require.ErrorIs(t, err, ErrReaderCandidateUnavailable)
			require.NoError(t, s.Observe(writers[1]))
			for step := 0; step < 5; step++ {
				ri := step % 2
				_, reader, release, err := s.Begin(context.Background())
				require.NoError(t, err)
				for wi, writer := range writers {
					writerHas, readerHas := wi == 1, ri == 1
					if reverse {
						writerHas, readerHas = !writerHas, !readerHas
					}
					record := map[string]any{"id": "one"}
					if writerHas {
						record["quality_score"] = goavro.Union("double", 0.9)
					}
					frame, err := Encode(writer, []any{record})
					require.NoError(t, err)
					value, err := c.DecodeWithReader(context.Background(), frame, reader.Live)
					require.NoError(t, err)
					rows, err := Records(value)
					require.NoError(t, err)
					if writerHas && readerHas {
						require.Equal(t, 0.9, rows[0]["quality_score"])
					} else {
						require.NotContains(t, rows[0], "quality_score")
					}
				}
				release()
				version, err := s.Toggle(context.Background())
				require.NoError(t, err)
				require.Equal(t, fmt.Sprintf("v%d", 2-ri), version)
			}
		})
	}
}

func TestReaderSwitchRejectsInvalidCandidatesAndRetainsSnapshots(t *testing.T) {
	baseline := `{"type":"array","items":{"type":"record","name":"R","fields":[{"name":"id","type":"string"}]}}`
	reader, err := NewSourceReader(baseline, "Page")
	require.NoError(t, err)
	s, err := NewReaderSwitch(reader, "Page")
	require.NoError(t, err)
	for _, invalid := range []string{`{`, `"string"`, `{"type":"array","items":{"type":"record","name":"R","fields":[]}}`, `{"type":"array","items":{"type":"record","name":"R","fields":[{"name":"id","type":"string"},{"name":"required_extension","type":"double"}]}}`} {
		require.Error(t, s.Observe(&Schema{Text: invalid}))
		_, err := s.Toggle(context.Background())
		require.ErrorIs(t, err, ErrReaderCandidateUnavailable)
	}
	second := `{"type":"array","items":{"type":"record","name":"R","fields":[{"name":"id","type":"string"},{"name":"extra","type":"double","default":0}]}}`
	require.NoError(t, s.Observe(&Schema{Text: second}))
	// Further variants do not replace the cached second demo schema.
	require.NoError(t, s.Observe(&Schema{Text: `{"type":"array","items":{"type":"record","name":"Other","fields":[]}}`}))
	_, err = s.Toggle(context.Background())
	require.NoError(t, err)
	_, selected, release, err := s.Begin(context.Background())
	require.NoError(t, err)
	defer release()
	key, err := schemaKey(selected.Live.codec.Schema())
	require.NoError(t, err)
	want, err := schemaKey(second)
	require.NoError(t, err)
	require.Equal(t, want, key)
}

func TestReaderSwitchDrainsLeasesAndCancellationKeepsReader(t *testing.T) {
	baseline := `{"type":"array","items":{"type":"record","name":"R","fields":[{"name":"id","type":"string"}]}}`
	reader, err := NewSourceReader(baseline, "Page")
	require.NoError(t, err)
	s, err := NewReaderSwitch(reader, "Page")
	require.NoError(t, err)
	ctx, selected, release, err := s.Begin(context.Background())
	require.NoError(t, err)
	defer func() {
		if release != nil {
			release()
		}
	}()
	require.NoError(t, s.Observe(&Schema{Text: `{"type":"array","items":{"type":"record","name":"R","fields":[{"name":"id","type":"string"},{"name":"extra","type":"double","default":0}]}}`}))
	cancelCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err = s.Toggle(cancelCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	_, stillBaseline, nestedRelease, err := s.Begin(ctx)
	require.NoError(t, err)
	require.Same(t, selected, stillBaseline)
	nestedRelease()
	done := make(chan error, 1)
	go func() { _, err := s.Toggle(context.Background()); done <- err }()
	select {
	case err := <-done:
		t.Fatalf("switched before commit release: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	// Reusing the pinned context must not reacquire the gate behind the toggle.
	_, nested, nestedRelease, err := s.Begin(ctx)
	require.NoError(t, err)
	require.Same(t, selected, nested)
	nestedRelease()
	release()
	release = nil
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("toggle did not finish after drain")
	}
	_, upgraded, upgradedRelease, err := s.Begin(context.Background())
	require.NoError(t, err)
	defer upgradedRelease()
	require.NotSame(t, selected, upgraded)
}
