package wildfireprocessor

import (
	"aggregator/internal/rawinput"
	"aggregator/internal/wildfire"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestM6PreparedPagedComponentRestartAndTrace(t *testing.T) {
	l := databaseLab(t, true)
	config := l.e.Config
	config.PageRows = 2
	var err error
	l.e, err = New(l.s, config)
	require.NoError(t, err)
	m := oracle(t, config)
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	previous, propagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer func() {
		otel.SetTracerProvider(previous)
		otel.SetTextMapPropagator(propagator)
		_ = provider.Shutdown(context.Background())
	}()
	parent := "00-01234567890123456789012345678901-0123456789012345-01"
	started := time.Now()
	base := *fixtureOperations(t, "bridge")[0].Detection
	for i := 0; i < 10; i++ {
		d := base
		d.Satellite, d.Longitude = "same-satellite", fmt.Sprintf("0.%03d", i)
		op := wildfire.Operation{Version: 1, Namespace: config.Namespace, Offset: int64(i + 1), Kind: "detection", Detection: &d, Payload: []byte{byte(i), 0, 255}}
		env := envelope(t, op)
		env.Traceparent = &parent
		_, err = l.s.AppendHistory(context.Background(), rawinput.AppendRequest{Version: 1, Ownership: l.p, Messages: []rawinput.Envelope{env}})
		require.NoError(t, err)
		_, err = m.Apply(op)
		require.NoError(t, err)
		if i == 9 {
			stepTo(t, l, "graph")
			for step := 0; step < 4; step++ {
				_, err = l.e.Step(context.Background(), l.p)
				require.NoError(t, err)
			}
			var visited int
			require.NoError(t, l.c.QueryRow(context.Background(), `SELECT count(*) FROM wildfire_work WHERE position=10 AND component<>''`).Scan(&visited))
			require.Greater(t, visited, 0)
			// Recovery receives configuration and SQL only. The oracle never
			// supplies observations, memberships or its retained history.
			l.e, err = New(l.s, config)
			require.NoError(t, err)
			t.Logf("restarted paged component with %d persisted assigned nodes", visited)
		}
		l.prepare(t, op.Offset)
		l.deliver(t, op.Offset)
		compareOracle(t, l, m)
	}
	active := 0
	for _, fire := range l.fires(t) {
		if fire.Active {
			active++
			require.Len(t, fire.Members, 10)
		}
	}
	require.Equal(t, 1, active)
	var control, work int64
	require.NoError(t, l.c.QueryRow(context.Background(), `SELECT applied_through FROM wildfire_runs`).Scan(&control))
	require.NoError(t, l.c.QueryRow(context.Background(), `SELECT count(*) FROM wildfire_work WHERE position=10`).Scan(&work))
	require.Equal(t, int64(10), control)
	require.Equal(t, int64(10), work)
	preparation, delivery := false, false
	for _, span := range exporter.GetSpans() {
		if span.Name == "wildfire preparation step" || span.Name == "wildfire canonical batch" {
			require.Equal(t, "01234567890123456789012345678901", span.SpanContext.TraceID().String())
			preparation = preparation || span.Name == "wildfire preparation step"
			delivery = delivery || span.Name == "wildfire canonical batch"
		}
	}
	require.True(t, preparation && delivery, "original input trace must reach both independent loops")
	t.Logf("10-detection connected component, page_rows=2, SQL-only restart, prepared/applied=10; elapsed=%s; this is a paging proof, not production capacity", time.Since(started))
}
