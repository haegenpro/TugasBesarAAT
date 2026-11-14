package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func (l *Lab) m34(ctx context.Context, d map[string]any) error {
	watch, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); l.m2Faults(watch) }()
	defer func() { cancel(); <-done }()
	root := filepath.Clean(filepath.Join(l.Root, "..", ".."))
	pattern := "^TestM4"
	if l.Suite == "m3" {
		pattern = "^(TestM3|TestM2Migrations|TestM2InterruptedDistributedCommit|TestM2RecoveryAndPublicationFence)"
	}
	args := []string{"-C", filepath.Join(root, "aggregator"), "test", "-mod=readonly", "-count=1", "-p=1", "-timeout=180s", "-v", "./internal/store", "./internal/ingestion", "./internal/pipeline", "-run", pattern}
	if l.Suite == "audit-analytics" {
		pattern = "^(TestM8|TestM9|TestPlatformAudit)"
		if selected := os.Getenv("M89_TEST_FILTER"); selected != "" {
			pattern = selected
		}
		args = []string{"-C", filepath.Join(root, "aggregator"), "test", "-mod=readonly", "-count=1", "-p=1", "-timeout=240s", "-v", "./internal/store", "-run", pattern}
	}
	if l.Suite == "recovery-core" {
		pattern = "^(TestM7|TestM4MissingWorkerPreservesContinuation)"
		args = []string{"-C", filepath.Join(root, "aggregator"), "test", "-mod=readonly", "-count=1", "-p=1", "-timeout=150s", "-v", "./internal/store", "-run", pattern}
	}
	if wildfireSuite(l.Suite) {
		pattern = map[string]string{
			"wildfire-replay":     "^(TestM6DurableReplay|TestM6Normalized|TestM6Configuration)",
			"wildfire-late":       "^(TestM6Late|TestM6Quota)",
			"wildfire-replies":    "^TestM6Lost",
			"wildfire-crashes":    "^(TestM6Killed|TestM6CrashHelper)",
			"wildfire-history":    "^TestM6Shared",
			"wildfire-boundaries": "^(TestM6Receipt|TestM6Takeover|TestM6Prepared|TestM6Interrupted)",
			"wildfire-migrations": "^(TestM2Migrations|TestM6Populated)",
		}[l.Suite]
		if pattern == "" {
			return fmt.Errorf("unknown wildfire scenario group")
		}
		args = []string{"-C", filepath.Join(root, "aggregator"), "test", "-mod=readonly", "-count=1", "-p=1", "-timeout=10m", "-v", "./internal/wildfireprocessor", "-run", pattern}
		if l.Suite == "wildfire-migrations" {
			args = append(args[:len(args)-2], "./internal/store", "-run", pattern)
		}
	}
	command := exec.CommandContext(ctx, "go", args...)
	command.Env = append(os.Environ(), "M2_DATABASE_URL="+l.DSN["coord"], "M2_WORKER1_URL="+l.DSN["worker1"], "M2_WORKER2_URL="+l.DSN["worker2"], "M2_EVIDENCE_DIR="+l.Dir, "DB_MAX_OPEN_CONNS=6")
	if l.Suite == "m3" {
		version, versionErr := l.compose(ctx, "exec", "-T", "broker1", "nats-server", "--version")
		if versionErr != nil {
			return versionErr
		}
		d["nats_runtime_version"] = strings.TrimSpace(string(version))
		b, err := l.compose(ctx, "port", "broker1", "4222")
		if err != nil {
			return err
		}
		u := url.URL{Scheme: "nats", Host: strings.TrimSpace(string(b)), User: url.UserPassword("raw", l.Password)}
		command.Env = append(command.Env, "M3_NATS_URL="+u.String())
	}
	var output tailBuffer
	output.limit = 2 << 20
	command.Stdout = &output
	command.Stderr = &output
	started := time.Now()
	err := command.Run()
	redacted := strings.ReplaceAll(string(output.b), l.Password, "[redacted]")
	_ = os.WriteFile(filepath.Join(l.Dir, l.Suite+"-tests.log"), []byte(redacted), 0600)
	if l.Suite == "recovery-core" && strings.Contains(redacted, "RECOVERY_BLOCKED:") {
		return fmt.Errorf("blocked: pinned Citus online movement unavailable; see redacted test log")
	}
	if l.Suite == "audit-analytics" && !strings.Contains(redacted, "=== RUN   Test") {
		return fmt.Errorf("no requested audit/analytics test executed")
	}
	if l.Suite == "recovery-core" && (!strings.Contains(redacted, "=== RUN   TestM7") || strings.Contains(redacted, "--- SKIP: TestM7")) {
		return fmt.Errorf("requested recovery assertions did not execute; inspect test log")
	}
	if wildfireSuite(l.Suite) && !strings.Contains(redacted, "=== RUN   TestM6") {
		return fmt.Errorf("requested wildfire scenarios did not execute; inspect test log")
	}
	d["command"] = "go -C aggregator test -mod=readonly -count=1 -p=1 -timeout=180s -v ./internal/store ./internal/ingestion ./internal/pipeline -run " + pattern
	if l.Suite == "audit-analytics" || l.Suite == "recovery-core" {
		d["command"] = "go -C aggregator " + strings.Join(args[2:], " ")
	}
	if wildfireSuite(l.Suite) {
		d["command"] = "go -C aggregator " + strings.Join(args[2:], " ")
	}
	d["elapsed_ms"] = time.Since(started).Milliseconds()
	if l.Suite == "recovery-core" {
		d["go_flags"] = os.Getenv("GOFLAGS")
	}
	d["scope"] = "isolated single-copy Citus/component proof; not replicated deployment acceptance"
	d["nats_image"] = os.Getenv("M3_NATS_IMAGE")
	if err != nil {
		return fmt.Errorf("%s assertions failed; see redacted test log", l.Suite)
	}
	for _, service := range l.Services {
		id, e := l.container(ctx, service)
		if e != nil {
			return e
		}
		raw, e := l.cmd(ctx, "docker", "inspect", "--format", "{{.State.OOMKilled}}", id)
		if e != nil {
			return e
		}
		if strings.TrimSpace(string(raw)) == "true" {
			return fmt.Errorf("blocked: resource failure in run-owned %s; no shared settings changed", service)
		}
	}
	return nil
}
