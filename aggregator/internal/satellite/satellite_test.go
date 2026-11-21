package satellite

import (
	"aggregator/internal/wildfire"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStreamParsesServerSentEvents(t *testing.T) {
	body := ": keepalive\n\nid: 7\nevent: detection\ndata: {\"a\":\ndata: 1}\n\nevent: heartbeat\ndata: {}\r\n\r\nid: 8\ndata: x\n\n"
	s := NewStream(strings.NewReader(body), 1<<10)
	e, err := s.Next()
	require.NoError(t, err)
	require.Equal(t, Event{ID: "7", Type: "detection", Data: []byte("{\"a\":\n1}")}, e)
	e, err = s.Next()
	require.NoError(t, err)
	require.Equal(t, "heartbeat", e.Type)
	e, err = s.Next()
	require.NoError(t, err)
	require.Equal(t, "message", e.Type)
	_, err = s.Next()
	require.ErrorIs(t, err, io.EOF)
	_, err = NewStream(strings.NewReader("id: 9\ndata: partial"), 1<<10).Next()
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	_, err = NewStream(strings.NewReader("data: "+strings.Repeat("x", 2048)+"\n\n"), 1<<10).Next()
	require.ErrorIs(t, err, ErrLineTooLong)
}

// Bounded read-only observation of the practice feed: offset zero is valid,
// detection fields are nested, acquisition time is numeric, and FRP uses "frp".
func TestObservedPracticeDetectionAndClientIdentification(t *testing.T) {
	raw := []byte(`{"offset":0,"detection":{"version":"2.0NRT","scan":0.55,"acq_date":"2026-11-18","acq_time":2309,"bright_ti4":308.43,"bright_ti5":283.67,"confidence":"n","daynight":"N","frp":1.41,"instrument":"VIIRS","latitude":1.38076,"longitude":97.58036,"satellite":"N21","track":0.61}}`)
	n, err := Normalize("session-a", Event{ID: "0", Type: "detection", Data: raw})
	require.NoError(t, err)
	require.Zero(t, n.Offset)
	op, err := decodeOperation(n.Operation)
	require.NoError(t, err)
	require.Equal(t, "2309", op.Detection.AcqTime)
	require.Equal(t, 1.41, op.Detection.FRPMW)
	require.Equal(t, raw, op.Payload)

}

func decodeOperation(raw []byte) (wildfire.Operation, error) {
	var op wildfire.Operation
	err := wildfire.Decode(raw, &op)
	return op, err
}

func TestObservedPracticePartialUpdates(t *testing.T) {
	for _, changes := range []string{`{"bright_ti5":272.92,"frp":8.68}`, `{"bright_ti4":318.05,"bright_ti5":275.58}`} {
		raw := []byte(`{"offset":68,"key":"N21|2026-11-18|2359|1.38352|97.58346","changes":` + changes + `}`)
		n, err := Normalize("practice", Event{ID: "68", Type: "update", Data: raw})
		require.NoError(t, err)
		op, err := decodeOperation(n.Operation)
		require.NoError(t, err)
		require.Nil(t, op.Detection)
		require.JSONEq(t, changes, string(op.Changes))
		require.Equal(t, raw, op.Payload)
		require.Equal(t, "N21|2026-11-18|2359|1.38352|97.58346", op.TargetKey)
	}
}

func TestNormalizedOperationsAreAcceptedByTheWildfireProcessor(t *testing.T) {
	events := []Event{
		{ID: "41", Type: "detection", Data: []byte(`{"offset":41,"virtual_now":"2026-11-12T02:51:08Z","satellite":"N20","acq_date":"2026-11-12","acq_time":"0243","latitude":-2.51230,"longitude":113.80440,"confidence":"h","frp":12.5}`)},
		{ID: "42", Type: "detection", Data: []byte(`{"detection":{"satellite":"N21","acq_date":"2026-11-12","acq_time":"0244","latitude":"-2.5124","longitude":"113.8045","confidence":"n","frp_mw":"3.1"}}`)},
		{ID: "43", Type: "update", Data: []byte(`{"target_key":"N20|2026-11-12|0243|-2.51230|113.80440","detection":{"satellite":"N20","acq_date":"2026-11-12","acq_time":"0243","latitude":-2.51230,"longitude":113.80440,"confidence":"h","frp_mw":20}}`)},
		{ID: "44", Type: "retract", Data: []byte(`{"satellite":"N21","acq_date":"2026-11-12","acq_time":"0244","latitude":"-2.5124","longitude":"113.8045"}`)},
		{ID: "45", Type: "heartbeat", Data: []byte(`{"virtual_now":"2026-11-12T02:52:00Z"}`)},
	}
	for _, e := range events {
		n, err := Normalize("session-a", e)
		require.NoError(t, err, e.ID)
		op, err := decodeOperation(n.Operation)
		require.NoError(t, err, e.ID)
		require.Equal(t, e.Data, op.Payload)
		switch e.ID {
		case "41":
			require.Equal(t, "-2.51230", op.Detection.Latitude)
			require.Equal(t, 12.5, op.Detection.FRPMW)
			require.NotNil(t, op.VirtualNow)
		case "42":
			require.Equal(t, 3.1, op.Detection.FRPMW)
		case "44":
			require.Equal(t, "N21|2026-11-12|0244|-2.5124|113.8045", op.TargetKey)
		}
	}
}

func TestNormalizeRejectsWhatItCannotRepresent(t *testing.T) {
	cases := map[string]Event{
		"no offset":        {Type: "detection", Data: []byte(`{"satellite":"N20"}`)},
		"offset mismatch":  {ID: "5", Type: "detection", Data: []byte(`{"offset":6}`)},
		"missing field":    {ID: "5", Type: "detection", Data: []byte(`{"satellite":"N20","acq_date":"2026-11-12","acq_time":"0243","latitude":1,"longitude":1,"confidence":"h"}`)},
		"unknown type":     {ID: "5", Type: "teleport", Data: []byte(`{}`)},
		"not json":         {ID: "5", Type: "detection", Data: []byte(`not-json`)},
		"retract no key":   {ID: "5", Type: "retract", Data: []byte(`{}`)},
		"textual frp junk": {ID: "5", Type: "detection", Data: []byte(`{"satellite":"N20","acq_date":"2026-11-12","acq_time":"0243","latitude":1,"longitude":1,"confidence":"h","frp":"hot"}`)},
	}
	for name, e := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Normalize("session-a", e)
			var u *Unreadable
			require.True(t, errors.As(err, &u), "%v", err)
		})
	}
}

func TestUnreadableEventPlaceholderKeepsOffsetsContiguous(t *testing.T) {
	data := []byte(`{"unexpected":true}`)
	placeholder, err := json.Marshal(map[string]any{"version": 1, "namespace": "session-a", "offset": int64(9), "kind": "heartbeat", "payload": data})
	require.NoError(t, err)
	op, err := decodeOperation(placeholder)
	require.NoError(t, err)
	require.Equal(t, data, op.Payload)
}

func TestMalformedEventPreservesItsSSEOffset(t *testing.T) {
	n, err := Normalize("session-a", Event{ID: "9", Type: "detection", Data: []byte(`{"broken":`)})
	require.Error(t, err)
	require.Equal(t, int64(9), n.Offset, "quarantine must not reset a known offset to zero")
	n, err = Normalize("session-a", Event{Type: "detection", Data: []byte(`not-json`)})
	require.Error(t, err)
	require.Equal(t, int64(-1), n.Offset, "an unknown offset must not be treated as an old duplicate")
}

func TestStreamBoundsMultilineEventSize(t *testing.T) {
	body := "id: 1\nevent: detection\n" + strings.Repeat("data: "+strings.Repeat("x", 30)+"\n", 100) + "\n"
	_, err := NewStream(strings.NewReader(body), 128).Next()
	require.Error(t, err, "short data lines must not bypass the event size limit")
}

func TestNormalizeRejectsTrailingJSON(t *testing.T) {
	_, err := Normalize("session-a", Event{ID: "9", Type: "heartbeat", Data: []byte(`{}]`)})
	require.Error(t, err)
}
