package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
)

const adminMessageLimit = 8192

type localResumeRequest struct {
	Operator      string `json:"operator"`
	Reason        string `json:"reason"`
	CorrelationID string `json:"correlation_id"`
}

type localResumeResponse struct {
	Status         string `json:"status,omitempty"`
	CorrelationID  string `json:"correlation_id,omitempty"`
	PreviousReason string `json:"previous_reason,omitempty"`
	Error          string `json:"error,omitempty"`
}

func (r *localResumeRequest) validate() error {
	r.Operator = strings.TrimSpace(r.Operator)
	r.Reason = strings.TrimSpace(r.Reason)
	r.CorrelationID = strings.TrimSpace(r.CorrelationID)
	if r.Operator == "" || len(r.Operator) > 256 {
		return errors.New("operator is required and must not exceed 256 bytes")
	}
	if r.Reason == "" || len(r.Reason) > 2048 {
		return errors.New("recovery reason is required and must not exceed 2048 bytes")
	}
	if r.CorrelationID == "" {
		r.CorrelationID = uuid.NewString()
	}
	if len(r.CorrelationID) > 256 {
		return errors.New("correlation ID must not exceed 256 bytes")
	}
	return nil
}

func adminSocketPath(dbPath string) string {
	return filepath.Join(filepath.Dir(dbPath), ".consumer-admin", filepath.Base(dbPath)+".sock")
}

// Access to the container and this owner-only Unix socket is the administrative
// boundary. No TCP port, downstream token, or Auth Service is involved.
func listenLocalAdmin(path string) (*net.UnixListener, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return nil, err
	}
	addr := &net.UnixAddr{Name: path, Net: "unix"}
	listener, err := net.ListenUnix("unix", addr)
	if err != nil {
		// A container crash can leave a socket file on its persistent volume.
		// Never unlink a regular file or a socket used by a live consumer.
		info, statErr := os.Lstat(path)
		if statErr != nil || info.Mode()&os.ModeSocket == 0 {
			return nil, err
		}
		conn, dialErr := net.DialTimeout("unix", path, 200*time.Millisecond)
		if dialErr == nil {
			conn.Close()
			return nil, fmt.Errorf("local administration socket already in use: %s", path)
		}
		if !errors.Is(dialErr, syscall.ECONNREFUSED) {
			return nil, err
		}
		if err := os.Remove(path); err != nil {
			return nil, err
		}
		listener, err = net.ListenUnix("unix", addr)
		if err != nil {
			return nil, err
		}
	}
	if err := os.Chmod(path, 0600); err != nil {
		listener.Close()
		return nil, err
	}
	return listener, nil
}

func (a *app) startLocalAdmin(parent context.Context) (func(), error) {
	listener, err := listenLocalAdmin(adminSocketPath(a.cfg.DBPath))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := listener.AcceptUnix()
			if err != nil {
				if ctx.Err() == nil {
					slog.Error("local administration listener failed", "service", a.cfg.Role, "error", err)
				}
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close()
				a.handleLocalResume(ctx, conn)
			}()
		}
	}()
	go func() {
		<-ctx.Done()
		listener.Close()
	}()
	return func() {
		cancel()
		listener.Close()
		wg.Wait()
	}, nil
}

func (a *app) handleLocalResume(parent context.Context, conn *net.UnixConn) {
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	generation := a.recoveryGeneration()
	raw, err := io.ReadAll(io.LimitReader(conn, adminMessageLimit+1))
	if err != nil || len(raw) > adminMessageLimit {
		_ = json.NewEncoder(conn).Encode(localResumeResponse{Error: "invalid local recovery request"})
		return
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var request localResumeRequest
	if err := decoder.Decode(&request); err != nil {
		_ = json.NewEncoder(conn).Encode(localResumeResponse{Error: "invalid local recovery request"})
		return
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		_ = json.NewEncoder(conn).Encode(localResumeResponse{Error: "expected one local recovery request"})
		return
	}
	if err := request.validate(); err != nil {
		_ = json.NewEncoder(conn).Encode(localResumeResponse{Error: err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, traceKey{}, messageTrace{Service: "consumer-" + a.cfg.Role, CorrelationID: request.CorrelationID})
	started := time.Now()
	response := localResumeResponse{CorrelationID: request.CorrelationID}
	resumed, previous, err := a.resume(ctx, request.Operator, request.Reason, generation)
	if errors.Is(err, errHaltChanged) {
		response.Error = "halt changed; inspect readiness and retry after fixing the new failure"
	} else if err != nil {
		response.Error = "recovery storage unavailable"
	} else {
		response.Status = "not_halted"
		if resumed {
			response.Status = "resume_requested"
		}
		response.PreviousReason = previous
	}
	level := slog.LevelInfo
	if err != nil {
		level = slog.LevelError
	}
	logEvent(ctx, level, "local consumer resume", "operator", request.Operator, "recovery_reason", request.Reason,
		"status", response.Status, "previous_reason", previous, "latency_ms", time.Since(started).Milliseconds(), "error", err)
	_ = json.NewEncoder(conn).Encode(response)
}

func requestLocalResume(ctx context.Context, path string, request localResumeRequest) (localResumeResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return localResumeResponse{}, fmt.Errorf("connect to running consumer's local administration socket: %w", err)
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return localResumeResponse{}, err
	}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return localResumeResponse{}, err
	}
	if err := conn.(*net.UnixConn).CloseWrite(); err != nil {
		return localResumeResponse{}, err
	}
	var response localResumeResponse
	if err := json.NewDecoder(io.LimitReader(conn, adminMessageLimit)).Decode(&response); err != nil {
		return response, err
	}
	if response.Error != "" {
		return response, errors.New(response.Error)
	}
	if response.Status != "resume_requested" && response.Status != "not_halted" {
		return response, errors.New("invalid local recovery response")
	}
	return response, nil
}

func runAdminCommand(ctx context.Context, args []string, output io.Writer) error {
	const usage = "usage: consumer resume --operator NAME --reason TEXT [--correlation-id ID]"
	if len(args) == 1 && (args[0] == "--help" || args[0] == "-h") {
		_, err := fmt.Fprintln(output, usage)
		return err
	}
	if len(args) == 0 || args[0] != "resume" {
		return errors.New(usage)
	}
	flags := flag.NewFlagSet("consumer resume", flag.ContinueOnError)
	flags.SetOutput(output)
	var request localResumeRequest
	flags.StringVar(&request.Operator, "operator", "", "operator label for the recovery audit")
	flags.StringVar(&request.Reason, "reason", "", "repair performed before resuming")
	flags.StringVar(&request.CorrelationID, "correlation-id", "", "optional audit correlation ID")
	if err := flags.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return errors.New(usage)
	}
	if err := request.validate(); err != nil {
		return err
	}
	values, err := requiredEnv("CONSUMER_DB_PATH")
	if err != nil {
		return err
	}
	response, err := requestLocalResume(ctx, adminSocketPath(values["CONSUMER_DB_PATH"]), request)
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(response)
}
