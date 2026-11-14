// role1-m1 is a development-only lab. It never connects to an application DB.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx/v5"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if err := dispatch(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "role1-m1:", err)
		os.Exit(1)
	}
}

func dispatch(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("use doctor, run --suite core|ha|m2|m3|m4|wildfire|all, or down --run RUN_ID")
	}
	if args[0] == "_hold-gate" {
		return holdGate(ctx)
	}
	root, err := os.Getwd()
	if err != nil {
		return err
	}
	mod, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil || !strings.HasPrefix(string(mod), "module role1-m1\n") {
		return errors.New("run from experiments/m1 (go -C experiments/m1 run . ...)")
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	suite := fs.String("suite", "all", "core, ha, m2, m3, m4, wildfire, named wildfire scenario group, or all; suites run sequentially")
	runID := fs.String("run", "", "run ID for down; volumes are retained")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if !validSuite(*suite) {
		return fmt.Errorf("invalid suite %q", *suite)
	}
	switch args[0] {
	case "doctor":
		info, err := doctor(ctx, root, *suite)
		if err != nil {
			return err
		}
		b, _ := json.MarshalIndent(info, "", "  ")
		fmt.Println(string(b))
		return nil
	case "run":
		return runLab(ctx, root, *suite)
	case "down":
		return downRun(ctx, root, *runID)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func validSuite(s string) bool {
	return s == "all" || s == "core" || s == "ha" || s == "m2" || s == "m3" || s == "m4" || wildfireSuite(s)
}

func wildfireSuite(s string) bool {
	switch s {
	case "wildfire", "wildfire-replay", "wildfire-late", "wildfire-replies", "wildfire-crashes", "wildfire-history", "wildfire-boundaries", "wildfire-migrations":
		return true
	}
	return false
}

func holdGate(ctx context.Context) error {
	c, err := pgx.Connect(ctx, os.Getenv("M1_HOLD_DSN"))
	if err != nil {
		return errors.New("hold-gate connection failed")
	}
	defer c.Close(context.Background())
	if _, err = c.Exec(ctx, "SELECT pg_advisory_lock($1)", gateKey); err != nil {
		return err
	}
	fmt.Println("READY")
	<-ctx.Done()
	return ctx.Err()
}
