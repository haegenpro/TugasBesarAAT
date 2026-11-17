package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestEmptyDiskRecoveryPreservesMessagesAndDurableConsumer(t *testing.T) {
	server := os.Getenv("NATS_SERVER_BINARY")
	templatePath := os.Getenv("NATS_RECOVERY_CONFIG")
	if server == "" || templatePath == "" {
		t.Skip("run scripts/test-broker-recovery.py for isolated three-broker protocol validation")
	}
	root := t.TempDir()
	template, err := os.ReadFile(templatePath)
	if err != nil {
		t.Fatal(err)
	}
	helper := filepath.Join(root, "recover-node")
	if output, err := exec.Command("go", "build", "-mod=readonly", "-o", helper, ".").CombinedOutput(); err != nil {
		t.Fatalf("build helper: %v %s", err, output)
	}
	port := func() int {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		value := listener.Addr().(*net.TCPAddr).Port
		listener.Close()
		return value
	}
	ports := []int{port(), port(), port()}
	routes := []int{port(), port(), port()}
	processes := make([]*exec.Cmd, 3)
	start := func(index int) {
		name := fmt.Sprintf("broker-%c-0", 'a'+index)
		folder := filepath.Join(root, name)
		if err := os.MkdirAll(folder, 0700); err != nil {
			t.Fatal(err)
		}
		config := string(template)
		for i := range routes {
			placeholder := fmt.Sprintf("nats://broker-%c-0.broker-members.TEST_NAMESPACE.svc.cluster.local:6222", 'a'+i)
			config = strings.ReplaceAll(config, placeholder, fmt.Sprintf("nats://127.0.0.1:%d", routes[i]))
		}
		config = strings.ReplaceAll(config, "port: 4222", fmt.Sprintf("port: %d", ports[index]))
		config = strings.ReplaceAll(config, "http_port: 8222", "http_port: 0")
		config = "host: 127.0.0.1\n" + config
		config = strings.ReplaceAll(config, "port: 6222", fmt.Sprintf("port: %d", routes[index]))
		config = strings.ReplaceAll(config, `store_dir: "/data"`, fmt.Sprintf("store_dir: %q", filepath.Join(folder, "data")))
		config = strings.ReplaceAll(config, "max_file_store: 8589934592", "max_file_store: 16777216")
		path := filepath.Join(folder, "nats.conf")
		if err := os.WriteFile(path, []byte(config), 0600); err != nil {
			t.Fatal(err)
		}
		log, err := os.OpenFile(filepath.Join(root, name+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { log.Close() })
		cmd := exec.Command(server, "-c", path)
		cmd.Env = append(os.Environ(), "POD_NAME="+name, "NATS_APP_USER=app", "NATS_APP_PASSWORD=fixture-app", "NATS_SYSTEM_USER=system", "NATS_SYSTEM_PASSWORD=fixture-system")
		cmd.Stdout = log
		cmd.Stderr = log
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		processes[index] = cmd
	}
	t.Cleanup(func() {
		for _, cmd := range processes {
			if cmd != nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
			}
		}
		if t.Failed() {
			for i := 0; i < 3; i++ {
				data, _ := os.ReadFile(filepath.Join(root, fmt.Sprintf("broker-%c-0.log", 'a'+i)))
				t.Logf("broker %d: %s", i, data)
			}
		}
	})
	for i := 0; i < 3; i++ {
		start(i)
	}
	appURL := fmt.Sprintf("nats://app:fixture-app@127.0.0.1:%d", ports[0])
	systemURL := fmt.Sprintf("nats://system:fixture-system@127.0.0.1:%d", ports[1])
	deadline := time.Now().Add(30 * time.Second)
	var nc *nats.Conn
	for {
		nc, err = nats.Connect(appURL, nats.Timeout(time.Second), nats.NoReconnect())
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("broker startup deadline")
		}
		time.Sleep(100 * time.Millisecond)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	var account *nats.Msg
	for {
		account, err = nc.Request("$JS.API.INFO", nil, 3*time.Second)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("application account JetStream API unavailable: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("application account JetStream response: %s", account.Data)
	var stream jetstream.Stream
	for {
		attempt, end := context.WithTimeout(ctx, 3*time.Second)
		stream, err = js.CreateStream(attempt, jetstream.StreamConfig{Name: "RECOVERY_CHECK", Subjects: []string{"recovery.check"}, Storage: jetstream.FileStorage, Replicas: 3, MaxBytes: 1 << 20})
		end()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	consumer, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: "recovery-proof", AckPolicy: jetstream.AckExplicitPolicy})
	if err != nil {
		t.Fatal(err)
	}
	publish := func(first, last int) {
		for i := first; i <= last; i++ {
			ack, err := js.Publish(ctx, "recovery.check", []byte(fmt.Sprintf("original-operation-%03d", i)), jetstream.WithMsgID(fmt.Sprintf("operation-%03d", i)))
			if err != nil || ack.Sequence != uint64(i) {
				t.Fatalf("publish %d: %v %v", i, ack, err)
			}
		}
	}
	consume := func(count int) {
		batch, err := consumer.Fetch(count, jetstream.FetchMaxWait(5*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		got := 0
		for message := range batch.Messages() {
			if err := message.DoubleAck(ctx); err != nil {
				t.Fatal(err)
			}
			got++
		}
		if got != count || batch.Error() != nil {
			t.Fatalf("consumer received %d/%d: %v", got, count, batch.Error())
		}
	}
	fenced := false
	call := func(operation, system string) ([]byte, error) {
		args := []string{"--operation", operation, "--server", "broker-c-0", "--members", "broker-a-0,broker-b-0,broker-c-0", "--streams", "RECOVERY_CHECK"}
		if operation == "remove" && fenced {
			args = append(args, "--fenced")
		}
		cmd := exec.CommandContext(ctx, helper, args...)
		cmd.Env = append(os.Environ(), "NATS_URL="+appURL, "NATS_SYSTEM_URL="+system)
		return cmd.CombinedOutput()
	}
	publish(1, 32)
	consume(32)
	if output, err := call("remove", systemURL); err == nil {
		t.Fatalf("live unfenced member removal accepted: %s", output)
	}
	if err := processes[2].Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = processes[2].Wait()
	processes[2] = nil
	fenced = true
	deadline = time.Now().Add(30 * time.Second)
	for {
		output, err := call("observe", systemURL)
		if err == nil {
			var observed struct {
				Streams []streamState `json:"streams"`
			}
			if json.Unmarshal(output, &observed) != nil || len(observed.Streams) != 1 || len(observed.Streams[0].Consumers) != 1 || observed.Streams[0].Consumers[0].AckFloor != 32 {
				t.Fatalf("consumer recovery anchor missing: %s", output)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("original two-copy quorum not observed: %s", output)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if _, err := call("remove", ""); err == nil {
		t.Fatal("missing system credential accepted")
	}
	if _, err := call("remove", appURL); err == nil {
		t.Fatal("application credential accepted for peer removal")
	}
	publish(33, 40)
	// Fixed-name empty-disk replacement retains Raft membership. Explicit
	// peer removal is permanent decommission, not an empty-disk repair step.
	if err := os.RemoveAll(filepath.Join(root, "broker-c-0")); err != nil {
		t.Fatal(err)
	}
	start(2)
	deadline = time.Now().Add(45 * time.Second)
	for {
		output, err := call("verify", systemURL)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("empty-disk rejoin deadline: %s", output)
		}
		time.Sleep(200 * time.Millisecond)
	}
	info, err := consumer.Info(ctx)
	if err != nil || info.AckFloor.Stream != 32 {
		t.Fatalf("durable consumer ack floor changed: %v %v", info, err)
	}
	publish(41, 48)
	consume(16)
	for i := 1; i <= 48; i++ {
		message, err := stream.GetMsg(ctx, uint64(i))
		if err != nil || string(message.Data) != fmt.Sprintf("original-operation-%03d", i) {
			t.Fatalf("accepted operation %d lost/changed: %v", i, err)
		}
	}
	info, err = consumer.Info(ctx)
	if err != nil || info.AckFloor.Stream != 48 {
		t.Fatalf("durable consumer failed to continue: %v %v", info, err)
	}
	t.Log("three replicas current; all 48 accepted message payloads and durable consumer progress retained across empty storage replacement")
	// Separately exercise the privileged decommission API only after the full
	// recovery proof has completed, without trying to reuse its removed identity.
	if err := processes[2].Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = processes[2].Wait()
	processes[2] = nil
	deadline = time.Now().Add(30 * time.Second)
	for {
		output, err := call("observe", systemURL)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("decommission survivor deadline: %s", output)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if output, err := call("remove", systemURL); err != nil {
		t.Fatalf("explicit fenced decommission: %s", output)
	}
}
