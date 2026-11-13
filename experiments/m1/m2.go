package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

func (l *Lab) m2(ctx context.Context, d map[string]any) error {
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	watchDone := make(chan struct{})
	go func() { defer close(watchDone); l.m2Faults(watchCtx) }()
	defer func() { cancel(); <-watchDone }()
	root := filepath.Clean(filepath.Join(l.Root, "..", ".."))
	failed := false
	for _, module := range []string{"aggregator", "consumer-dashboard", "consumer-notifier"} {
		pkg := "."
		if module == "aggregator" {
			pkg = "./internal/store"
		}
		args := []string{"-C", filepath.Join(root, module), "test", "-mod=readonly", "-count=1", "-timeout=150s", "-v", pkg, "-run", "^TestM2"}
		cmd := exec.CommandContext(ctx, "go", args...)
		cmd.Env = append(os.Environ(), "M2_DATABASE_URL="+l.DSN["coord"], "M2_WORKER1_URL="+l.DSN["worker1"], "M2_WORKER2_URL="+l.DSN["worker2"], "M2_EVIDENCE_DIR="+l.Dir, "TEST_POSTGRES_URL="+l.DSN["coord"], "DB_MAX_OPEN_CONNS=6")
		var out tailBuffer
		out.limit = 2 << 20
		cmd.Stdout = &out
		cmd.Stderr = &out
		start := time.Now()
		err := cmd.Run()
		name := module + "-tests.log"
		_ = os.WriteFile(filepath.Join(l.Dir, name), out.b, 0600)
		d[module] = map[string]any{"command": fmt.Sprintf("go -C %s test -mod=readonly -count=1 -timeout=150s -v %s -run ^TestM2", module, pkg), "elapsed_ms": time.Since(start).Milliseconds(), "passed": err == nil}
		if err != nil {
			failed = true
		}
	}
	for _, service := range l.DB {
		id, err := l.container(ctx, service)
		if err != nil {
			failed = true
			continue
		}
		raw, err := l.cmd(ctx, "docker", "inspect", "--format", "{{json .State}}", id)
		if err != nil {
			failed = true
			continue
		}
		var state struct{ OOMKilled bool }
		if json.Unmarshal(raw, &state) != nil || state.OOMKilled {
			failed = true
			d[service+"_resource_failure"] = true
		}
	}
	d["credentials"] = "run-owned child environment only"
	d["production_authority"] = "blocked: test adapters only"
	if failed {
		return errors.New("M2 assertions failed; see module test logs")
	}
	return nil
}
func (l *Lab) m2Faults(ctx context.Context) {
	seen := map[string]bool{}
	for ctx.Err() == nil {
		paths, _ := filepath.Glob(filepath.Join(l.Dir, "fault-request-*.json"))
		for _, path := range paths {
			if seen[path] {
				continue
			}
			seen[path] = true
			var r struct {
				Sequence int    `json:"sequence"`
				Action   string `json:"action"`
				Service  string `json:"service"`
			}
			raw, err := os.ReadFile(path)
			if err == nil {
				err = json.Unmarshal(raw, &r)
			}
			if err == nil {
				err = l.fault(ctx, r.Action, r.Service)
			}
			_ = writeJSON(filepath.Join(l.Dir, fmt.Sprintf("fault-response-%d.json", r.Sequence)), map[string]any{"error": errString(err), "action": r.Action, "service": r.Service, "dsn": l.DSN[r.Service]})
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}
