package main

import (
	"bytes"
	"context"
	"go.opentelemetry.io/otel"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTraceBatchOverrideExportsBeforeDefaultFlushInterval(t *testing.T) {
	received := make(chan []byte, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		received <- body
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", server.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", server.URL+"/v1/traces")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "1")
	t.Setenv("OTEL_BSP_MAX_QUEUE_SIZE", "8")
	t.Setenv("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", "2")
	t.Setenv("OTEL_BSP_EXPORT_TIMEOUT", "500")
	t.Setenv("OTEL_BSP_SCHEDULE_DELAY", "5000")
	previous := otel.GetTracerProvider()
	defer otel.SetTracerProvider(previous)
	stop, err := setupTelemetry()
	if err != nil {
		t.Fatal(err)
	}
	defer stop(context.Background())
	for _, name := range []string{"configured-batch-first", "configured-batch-second"} {
		_, span := otel.Tracer("batch-test").Start(context.Background(), name)
		span.End()
	}
	select {
	case body := <-received:
		for _, name := range []string{"configured-batch-first", "configured-batch-second"} {
			if !bytes.Contains(body, []byte(name)) {
				t.Fatalf("missing span %s from export", name)
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("configured two-span batch was not exported before the five-second flush")
	}
}

func TestTraceSettingsRejectInvalidValues(t *testing.T) {
	for _, values := range [][4]string{{"0", "1", "500", "1"}, {"4", "5", "500", "1"}, {"4", "1", "0", "1"}, {"4", "1", "500", "NaN"}} {
		t.Run(values[0]+"/"+values[1]+"/"+values[2]+"/"+values[3], func(t *testing.T) {
			t.Setenv("OTEL_BSP_MAX_QUEUE_SIZE", values[0])
			t.Setenv("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", values[1])
			t.Setenv("OTEL_BSP_EXPORT_TIMEOUT", values[2])
			t.Setenv("OTEL_TRACES_SAMPLER_ARG", values[3])
			stop, err := setupTelemetry()
			if err == nil {
				stop(context.Background())
				t.Fatal("invalid trace settings were accepted")
			}
		})
	}
}
