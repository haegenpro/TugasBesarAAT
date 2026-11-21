package satellite

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOversizedFrameEvidenceIsCompleteAndStopsBeforeNextFrame(t *testing.T) {
	for _, ending := range []string{"\n", "\r\n"} {
		first := "id: 42" + ending + "event: detection" + ending + "data: " + strings.Repeat("original-data", 20000) + ending + ending
		second := "id: 43" + ending + "data: next" + ending + ending
		stream := NewStream(strings.NewReader(first+second), 128)
		_, e := stream.Next()
		require.ErrorIs(t, e, ErrLineTooLong)
		var rejected *RejectedFrame
		require.True(t, errors.As(e, &rejected))
		body, e := io.ReadAll(rejected.Reader)
		require.NoError(t, e)
		require.Equal(t, first, string(body))
		next, e := stream.Next()
		require.NoError(t, e)
		require.Equal(t, "43", next.ID)
		require.Equal(t, "next", string(next.Data))
	}
}
func TestOversizedMultilineFrameRetainsHeadersAndEveryDataLine(t *testing.T) {
	first := "id: 9\nevent: detection\n" + strings.Repeat("data: abcdefghijklmnopqrstuvwxyz\n", 100) + "\n"
	stream := NewStream(strings.NewReader(first+"id: 10\ndata: ok\n\n"), 128)
	_, e := stream.Next()
	require.ErrorIs(t, e, ErrEventTooLong)
	var rejected *RejectedFrame
	require.ErrorAs(t, e, &rejected)
	body, e := io.ReadAll(rejected.Reader)
	require.NoError(t, e)
	require.Equal(t, first, string(body))
	next, e := stream.Next()
	require.NoError(t, e)
	require.Equal(t, "10", next.ID)
}
