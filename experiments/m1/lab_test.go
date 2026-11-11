package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func TestRoutingEncoding(t *testing.T) {
	for _, tc := range []struct{ source, ref, want string }{{"PVMBG", "a", `["PVMBG","a"]`}, {"BMKG", "a|b", `["BMKG","a|b"]`}, {"SATELIT", "é火<&>\u2028", "[\"SATELIT\",\"é火<&>\u2028\"]"}, {"PVMBG", "a\"\\\n", `["PVMBG","a\"\\\n"]`}} {
		got, e := routeKey(tc.source, tc.ref)
		if e != nil || got != tc.want {
			t.Fatalf("%q: %q %v; want %q", tc.ref, got, e, tc.want)
		}
		if !json.Valid([]byte(got)) {
			t.Fatal("invalid JSON")
		}
	}
	if _, e := routeKey("BMKG", string([]byte{0xff})); e == nil {
		t.Fatal("invalid UTF-8 accepted")
	}
}
func TestResourceOwnership(t *testing.T) {
	good := map[string]string{ownerLabel: ownerValue, "com.docker.compose.project": "mine", "io.itb.role1-m1.run": "id"}
	if !owned(good, "mine", "id") {
		t.Fatal("owned rejected")
	}
	for _, key := range []string{ownerLabel, "com.docker.compose.project", "io.itb.role1-m1.run"} {
		copy := map[string]string{}
		for k, v := range good {
			copy[k] = v
		}
		copy[key] = "another"
		if owned(copy, "mine", "id") {
			t.Fatal("unowned accepted")
		}
	}
}
func TestProfilesAreIsolated(t *testing.T) {
	for _, suite := range []string{"core", "ha"} {
		l := newLab(t.TempDir(), "test", suite, "secret", "image", nil)
		config := l.composeConfig()
		services := config["services"].(map[string]any)
		want := 4
		if suite == "ha" {
			want = 6
		}
		if len(services) != want {
			t.Fatalf("services %d", len(services))
		}
		for name, raw := range services {
			s := raw.(map[string]any)
			if s["labels"].(map[string]string)[ownerLabel] != ownerValue {
				t.Fatal("missing owner")
			}
			if strings.HasPrefix(name, "etcd") {
				if s["mem_limit"] != "128m" {
					t.Fatal("unbounded DCS")
				}
				continue
			}
			if s["mem_limit"] != "1g" {
				t.Fatal("unbounded DB")
			}
			if s["ports"].([]string)[0] != "127.0.0.1::5432" {
				t.Fatal("nonloopback/fixed port")
			}
			if _, ok := s["env_file"]; ok {
				t.Fatal("application env imported")
			}
			if s["environment"].(map[string]string)["M1_SUITE"] != suite {
				t.Fatal("suite changed")
			}
		}
	}
}
func TestCanceledPolling(t *testing.T) {
	ctx, c := context.WithCancel(context.Background())
	c()
	calls := 0
	err := until(ctx, func() error { calls++; return nil })
	if !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatal(err, calls)
	}
}
func TestFailureCannotBecomePass(t *testing.T) {
	r := &Report{}
	l := &Lab{Dir: t.TempDir(), Report: r}
	l.result(context.Background(), "failed", func(context.Context, map[string]any) error { return errors.New("assertion failed") })
	l.result(context.Background(), "blocked", func(context.Context, map[string]any) error {
		return errors.New("blocked: missing experiment dependency")
	})
	if r.Results[0].Status != "failed" || r.Results[1].Status != "blocked" {
		t.Fatal(r.Results)
	}
}
func TestRunIDValidation(t *testing.T) {
	for _, s := range []string{"", "../../elsewhere", "MY_PROJECT", "20261111t123000-12345678/other"} {
		if runPattern.MatchString(s) {
			t.Fatal("unsafe run ID", s)
		}
	}
	if !runPattern.MatchString("20261111t123000-12345678") {
		t.Fatal("valid ID rejected")
	}
}
func TestCommandCancellation(t *testing.T) {
	if len(os.Args) > 1 && os.Args[len(os.Args)-1] == "m1-helper-sleep" {
		for {
			time.Sleep(time.Second)
		}
	}
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, e = command(ctx, t.TempDir(), exe, "-test.run=TestCommandCancellation", "--", "m1-helper-sleep")
	if e == nil || time.Since(start) > 2*time.Second {
		t.Fatal("subprocess cancellation failed", e)
	}
}
func TestCommitProxyDropsOnlyCommitReply(t *testing.T) {
	backend, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer backend.Close()
	go func() {
		c, e := backend.Accept()
		if e != nil {
			return
		}
		defer c.Close()
		for _, body := range []string{"UPDATE 1\x00", "COMMIT\x00"} {
			header := make([]byte, 5)
			header[0] = 'C'
			binary.BigEndian.PutUint32(header[1:], uint32(len(body)+4))
			c.Write(append(header, []byte(body)...))
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, e := newCommitProxy(ctx, "postgres://u:p@"+backend.Addr().String()+"/db?sslmode=disable")
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	u, _ := url.Parse(p.DSN)
	c, e := net.Dial("tcp", u.Host)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	header := make([]byte, 5)
	if _, e = io.ReadFull(c, header); e != nil {
		t.Fatal(e)
	}
	body := make([]byte, int(binary.BigEndian.Uint32(header[1:]))-4)
	if _, e = io.ReadFull(c, body); e != nil || string(body) != "UPDATE 1\x00" {
		t.Fatal("ordinary reply lost", e)
	}
	if _, e = c.Read(make([]byte, 1)); e != io.EOF {
		t.Fatal("COMMIT response leaked", e)
	}
	select {
	case <-p.Dropped:
	case <-time.After(time.Second):
		t.Fatal("commit drop not signalled")
	}
}
