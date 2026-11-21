// wildfire-reference replays normalized synthetic fixtures. It is not a feed connector.
package main

import (
	"aggregator/internal/wildfire"
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type replayConfig struct {
	AlgorithmVersion       int    `json:"algorithm_version,omitempty"`
	Version                int    `json:"version"`
	Namespace              string `json:"namespace"`
	InitialOffset          *int64 `json:"initial_offset"`
	AllowedLatenessSeconds *int64 `json:"allowed_lateness_seconds"`
	ProvinceFile           string `json:"province_file"`
	ProvinceSHA256         string `json:"province_sha256"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(args []string, out, errOut io.Writer) error {
	if len(args) == 0 || args[0] != "replay" {
		return fmt.Errorf("use replay --config <file> --input <operations.jsonl>")
	}
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	fs.SetOutput(errOut)
	configFile := fs.String("config", "", "versioned fixture configuration")
	inputFile := fs.String("input", "", "normalized JSONL fixture operations")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 || *configFile == "" || *inputFile == "" {
		return fmt.Errorf("config and input are required")
	}
	raw, err := os.ReadFile(*configFile)
	if err != nil {
		return err
	}
	var c replayConfig
	if err = wildfire.Decode(raw, &c); err != nil {
		return err
	}
	if c.ProvinceFile == "" {
		return fmt.Errorf("province_file required")
	}
	provincePath := c.ProvinceFile
	if !filepath.IsAbs(provincePath) {
		provincePath = filepath.Join(filepath.Dir(*configFile), provincePath)
	}
	provinces, err := os.ReadFile(provincePath)
	if err != nil {
		return err
	}
	model, err := wildfire.New(wildfire.Config{Version: c.Version, AlgorithmVersion: c.AlgorithmVersion, Namespace: c.Namespace, InitialOffset: c.InitialOffset, AllowedLatenessSeconds: c.AllowedLatenessSeconds, ProvinceSHA256: c.ProvinceSHA256, ProvinceJSON: provinces})
	if err != nil {
		return err
	}
	input, err := os.Open(*inputFile)
	if err != nil {
		return err
	}
	defer input.Close()
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), 16<<20)
	encoder := json.NewEncoder(out)
	encoder.SetEscapeHTML(false)
	failed := false
	line := 0
	for scanner.Scan() {
		line++
		var op wildfire.Operation
		err = wildfire.Decode(scanner.Bytes(), &op)
		var result wildfire.Result
		if err == nil {
			result, err = model.Apply(op)
		}
		if err != nil {
			failed = true
			var typed *wildfire.Error
			var problem any = map[string]any{"code": "invalid_input", "message": err.Error()}
			if errors.As(err, &typed) {
				problem = typed
			}
			if e := encoder.Encode(map[string]any{"version": 1, "line": line, "rejected": true, "error": problem, "input_base64": base64.StdEncoding.EncodeToString(scanner.Bytes())}); e != nil {
				return e
			}
			continue
		}
		if result.Outcome == "blocked_output" {
			failed = true
		}
		if err = encoder.Encode(result); err != nil {
			return err
		}
	}
	if err = scanner.Err(); err != nil {
		return err
	}
	if failed {
		return fmt.Errorf("replay contains rejected operations or blocked outputs; inspect JSON diagnostics")
	}
	return nil
}
