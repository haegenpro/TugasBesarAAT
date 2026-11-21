// satellite-probe performs a bounded read-only contract check against the feed.
package main

import (
	"aggregator/internal/satellite"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"time"
)

func run() error {
	cfg := satellite.ProbeConfig{}
	var tokenFile, output string
	flag.StringVar(&cfg.BaseURL, "base-url", "https://satellite.naufarrel.dev", "source origin; no credentials")
	flag.StringVar(&cfg.Namespace, "namespace", "", "agreed source history identity (required)")
	flag.Int64Var(&cfg.From, "from", 0, "first inclusive source offset")
	flag.IntVar(&cfg.Records, "records", 32, "records per leg, 2..1024")
	flag.DurationVar(&cfg.Duration, "duration", 45*time.Second, "total deadline, at most 5m")
	flag.DurationVar(&cfg.Pause, "disconnect-for", 0, "deliberate disconnect before resuming, at most 1m")
	flag.StringVar(&tokenFile, "token-file", "", "private bearer token file; never put its value in arguments")
	flag.StringVar(&output, "output", "", "new private evidence directory (required)")
	flag.BoolVar(&cfg.AllowLoopbackHTTP, "allow-loopback-http", false, "allow explicit local HTTP test origins only")
	flag.Parse()
	if tokenFile == "" || output == "" {
		return errors.New("--token-file and --output are required")
	}
	info, err := os.Lstat(tokenFile)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() > 16384 {
		return errors.New("token input must be a private regular file no larger than 16 KiB")
	}
	raw, err := os.ReadFile(tokenFile)
	if err != nil {
		return errors.New("cannot read private token file")
	}
	cfg.Token = strings.TrimSpace(string(raw))
	if err = os.Mkdir(output, 0o700); err != nil {
		return errors.New("output must be a new directory with an existing private parent")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	report, probeErr := satellite.Probe(ctx, cfg)
	evidence := struct {
		Result satellite.ProbeReport `json:"result"`
		Build  *debug.BuildInfo      `json:"build,omitempty"`
	}{Result: report}
	if build, ok := debug.ReadBuildInfo(); ok {
		evidence.Build = build
	}
	data, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		return errors.New("cannot encode probe evidence")
	}
	if err = os.WriteFile(filepath.Join(output, "result.json"), append(data, '\n'), 0o600); err != nil {
		return errors.New("cannot preserve probe evidence")
	}
	fmt.Printf("Satellite contract probe passed: %t; evidence: %s\n", report.Passed, filepath.Join(output, "result.json"))
	return probeErr
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "Satellite probe stopped:", err)
		os.Exit(1)
	}
}
