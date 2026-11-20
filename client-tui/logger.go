package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"

	"golang.org/x/term"
)

func newLogger(out *os.File) *slog.Logger {
	if os.Getenv("CLIENT_TUI_LOG_FORMAT") == "json" || !term.IsTerminal(int(out.Fd())) {
		return slog.New(slog.NewJSONHandler(out, nil))
	}
	return slog.New(&callLine{out: out})
}

type callLine struct {
	mu  sync.Mutex
	out io.Writer
}

func (h *callLine) Enabled(context.Context, slog.Level) bool { return true }
func (h *callLine) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *callLine) WithGroup(string) slog.Handler            { return h }

func (h *callLine) Handle(_ context.Context, r slog.Record) error {
	fields := map[string]string{}
	r.Attrs(func(a slog.Attr) bool {
		fields[a.Key] = a.Value.String()
		return true
	})
	var line string
	if r.Message == "outbound call" {
		status := fields["status_code"]
		if status == "0" {
			status = "gagal terhubung"
		}
		line = fmt.Sprintf("%s %s · %s · %s ms · %s", fields["method"], fields["target_endpoint"], status, fields["latency_ms"], fields["correlation_id"])
	} else {
		parts := []string{r.Message}
		for key, value := range fields {
			parts = append(parts, key+"="+value)
		}
		line = strings.Join(parts, " ")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := fmt.Fprintf(h.out, "\033[2m  ↳ %s\033[0m\n", line)
	return err
}
