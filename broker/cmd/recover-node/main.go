// recover-node inspects surviving JetStream quorum, removes only a fenced lost
// server, or verifies its empty-disk rejoin. The platform controller fences the
// VM and checks its ownership before invoking this administrative command.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type peer struct {
	Name    string `json:"name"`
	Current bool   `json:"current"`
	Offline bool   `json:"offline"`
	Lag     uint64 `json:"lag"`
}
type streamState struct {
	Name      string          `json:"name"`
	Replicas  int             `json:"replicas"`
	Leader    string          `json:"leader"`
	Peers     []peer          `json:"peers"`
	Last      uint64          `json:"last_sequence"`
	Consumers []consumerState `json:"consumers"`
}

type consumerState struct {
	Name     string `json:"name"`
	AckFloor uint64 `json:"ack_floor_stream"`
	Leader   string `json:"leader"`
	Peers    []peer `json:"peers"`
}

func validate(state streamState, lost string, expected map[string]bool, rejoined bool) error {
	if state.Replicas != 3 || state.Leader == "" || !expected[state.Leader] || len(state.Peers) != 2 {
		return fmt.Errorf("stream %s has no original three-member quorum", state.Name)
	}
	seen := map[string]bool{state.Leader: true}
	healthy := 1
	if !rejoined && state.Leader == lost {
		return fmt.Errorf("fenced member is still reported as leader")
	}
	for _, p := range state.Peers {
		if !expected[p.Name] || seen[p.Name] {
			return fmt.Errorf("unknown or repeated JetStream peer")
		}
		seen[p.Name] = true
		if p.Name != lost && p.Current && !p.Offline && p.Lag == 0 {
			healthy++
		}
		if rejoined && (!p.Current || p.Offline || p.Lag != 0) {
			return fmt.Errorf("replacement has not caught up")
		}
	}
	if !rejoined && healthy < 2 {
		return fmt.Errorf("two surviving current copies required")
	}
	return nil
}

// Prove that the privileged connection reaches the same cluster as the stream
// connection, rather than trusting a second URL with matching server names.
func systemConnection(ctx context.Context, app *nats.Conn, expected map[string]bool) (*nats.Conn, error) {
	url := os.Getenv("NATS_SYSTEM_URL")
	if url == "" {
		return nil, fmt.Errorf("NATS_SYSTEM_URL Secret required for recovery")
	}
	nc, err := nats.Connect(url, nats.Timeout(2*time.Second), nats.NoReconnect())
	if err != nil {
		return nil, fmt.Errorf("broker system connection unavailable")
	}
	probe, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	response, err := nc.RequestWithContext(probe, "$SYS.REQ.SERVER."+app.ConnectedServerId()+".STATSZ", nil)
	var status struct {
		Server struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"server"`
	}
	if err != nil || json.Unmarshal(response.Data, &status) != nil || status.Server.ID != app.ConnectedServerId() || !expected[status.Server.Name] {
		nc.Close()
		return nil, fmt.Errorf("system credential cannot verify the original application broker cluster")
	}
	return nc, nil
}

func run() error {
	operation := flag.String("operation", "observe", "observe, remove or verify")
	server := flag.String("server", "", "original fenced server_name")
	memberList := flag.String("members", "", "three original server names, comma separated")
	streamList := flag.String("streams", "", "expected stream names, comma separated")
	fenced := flag.Bool("fenced", false, "confirm that the parent has independently fenced the original server")
	flag.Parse()
	if *operation != "observe" && *operation != "remove" && *operation != "verify" {
		return fmt.Errorf("invalid recovery operation")
	}
	if *operation == "remove" && !*fenced {
		return fmt.Errorf("peer removal requires explicit --fenced acknowledgement from the parent")
	}
	names := strings.Split(*memberList, ",")
	expected := map[string]bool{}
	for _, name := range names {
		if !regexp.MustCompile(`^[a-z0-9-]{1,63}$`).MatchString(name) || expected[name] {
			return fmt.Errorf("invalid original server names")
		}
		expected[name] = true
	}
	if len(expected) != 3 || !expected[*server] {
		return fmt.Errorf("exactly three original members including fenced member required")
	}
	streams := strings.Split(*streamList, ",")
	seen := map[string]bool{}
	if len(streams) < 1 || len(streams) > 32 {
		return fmt.Errorf("bounded explicit stream list required")
	}
	for _, name := range streams {
		if !regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,63}$`).MatchString(name) || seen[name] {
			return fmt.Errorf("invalid or repeated stream name")
		}
		seen[name] = true
	}
	url := os.Getenv("NATS_URL")
	if url == "" {
		return fmt.Errorf("NATS_URL Secret required")
	}
	nc, err := nats.Connect(url, nats.Timeout(2*time.Second), nats.NoReconnect())
	if err != nil {
		return fmt.Errorf("broker connection unavailable")
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("JetStream client unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	states := []streamState{}
	for _, name := range streams {
		stream, err := js.Stream(ctx, name)
		if err != nil {
			return fmt.Errorf("required stream unavailable: %s", name)
		}
		info, err := stream.Info(ctx)
		if err != nil || info.Cluster == nil {
			return fmt.Errorf("stream quorum metadata unavailable: %s", name)
		}
		state := streamState{Name: name, Replicas: info.Config.Replicas, Leader: info.Cluster.Leader, Last: info.State.LastSeq}
		for _, p := range info.Cluster.Replicas {
			state.Peers = append(state.Peers, peer{Name: p.Name, Current: p.Current, Offline: p.Offline, Lag: p.Lag})
		}
		if err = validate(state, *server, expected, *operation == "verify"); err != nil {
			return err
		}
		consumers := stream.ListConsumers(ctx)
		for info := range consumers.Info() {
			if info.Config.Durable == "" {
				continue
			}
			if len(state.Consumers) >= 64 || info.Cluster == nil || info.Config.MemoryStorage {
				return fmt.Errorf("durable consumer quorum metadata unavailable or unsupported: %s", name)
			}
			replicas := info.Config.Replicas
			if replicas == 0 {
				replicas = state.Replicas
			}
			copyState := streamState{Name: name + "/" + info.Name, Replicas: replicas, Leader: info.Cluster.Leader}
			for _, p := range info.Cluster.Replicas {
				copyState.Peers = append(copyState.Peers, peer{Name: p.Name, Current: p.Current, Offline: p.Offline, Lag: p.Lag})
			}
			if err = validate(copyState, *server, expected, *operation == "verify"); err != nil {
				return err
			}
			state.Consumers = append(state.Consumers, consumerState{Name: info.Name, AckFloor: info.AckFloor.Stream, Leader: copyState.Leader, Peers: copyState.Peers})
		}
		if err = consumers.Err(); err != nil {
			return fmt.Errorf("durable consumer listing unavailable: %s", name)
		}
		states = append(states, state)
	}
	system, err := systemConnection(ctx, nc, expected)
	if err != nil {
		return err
	}
	defer system.Close()
	if *operation == "remove" {
		body, _ := json.Marshal(map[string]string{"peer": *server})
		response, err := system.RequestWithContext(ctx, "$JS.API.SERVER.REMOVE", body)
		if err != nil {
			return fmt.Errorf("fenced JetStream peer removal unavailable")
		}
		var reply struct {
			Success bool            `json:"success"`
			Error   json.RawMessage `json:"error"`
		}
		if json.Unmarshal(response.Data, &reply) != nil || !reply.Success || len(reply.Error) > 0 && string(reply.Error) != "null" {
			return fmt.Errorf("JetStream refused fenced peer removal")
		}
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"operation": *operation, "server": *server, "streams": states})
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "broker recovery: "+err.Error())
		os.Exit(1)
	}
}
