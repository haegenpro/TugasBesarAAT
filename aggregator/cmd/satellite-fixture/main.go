// satellite-fixture publishes replayable development input, never a live feed.
package main

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/rawinput"
	"aggregator/internal/wildfireprocessor"
	"bufio"
	"context"
	"flag"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"os"
	"os/signal"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	fs := flag.NewFlagSet("satellite-fixture", flag.ContinueOnError)
	dev := fs.Bool("development", false, "acknowledge normalized synthetic input only")
	config := fs.String("config", "", "pinned processor configuration")
	input := fs.String("input", "", "normalized JSONL")
	received := fs.String("received-at", "", "stable UTC receipt origin, preserved on retries")
	schemaFile := fs.String("schema", "", "RawInput Avro schema")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	if !*dev || *config == "" || *input == "" || *schemaFile == "" || *received == "" {
		return fmt.Errorf("require --development --config --input --schema --received-at; preserve every argument on retry")
	}
	c, err := wildfireprocessor.LoadConfig(*config)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	nc, err := nats.Connect(os.Getenv("NATS_URL"), nats.Timeout(2*time.Second))
	if err != nil {
		return fmt.Errorf("fixture broker unavailable")
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	call, cancel := context.WithTimeout(ctx, 2*time.Second)
	_, err = rawinput.Ensure(call, js, rawinput.Satellite, 3)
	cancel()
	if err != nil {
		return err
	}
	text, err := os.ReadFile(*schemaFile)
	if err != nil {
		return err
	}
	registry := avrowire.NewClient(os.Getenv("SCHEMA_REGISTRY_URL"))
	call, cancel = context.WithTimeout(ctx, 2*time.Second)
	schema, err := registry.Register(call, "bnpb.raw-input-value", string(text))
	cancel()
	if err != nil {
		return err
	}
	f, err := os.Open(*input)
	if err != nil {
		return err
	}
	defer f.Close()
	scan := bufio.NewScanner(f)
	scan.Buffer(make([]byte, 4096), rawinput.MaxMessage)
	next := *c.InitialOffset
	for scan.Scan() {
		e, err := wildfireprocessor.FixtureEnvelope(scan.Bytes(), *received, "acquisition:SATELIT", 1, "normalized-fixture-publisher")
		if err != nil {
			return err
		}
		if e.Namespace != c.Namespace || *e.Position != next {
			return fmt.Errorf("fixture namespace or contiguous offset mismatch")
		}
		frame, err := rawinput.Encode(schema, e)
		if err != nil {
			return err
		}
		if len(frame) > rawinput.MaxMessage {
			return fmt.Errorf("fixture frame exceeds broker limit; source file retained")
		}
		call, cancel = context.WithTimeout(ctx, 2*time.Second)
		_, err = js.Publish(call, rawinput.Satellite.Subject, frame, jetstream.WithMsgID(e.MessageID))
		cancel()
		if err != nil {
			return fmt.Errorf("publication unknown; retry identical file and arguments")
		}
		fmt.Println(e.MessageID)
		next++
	}
	return scan.Err()
}
