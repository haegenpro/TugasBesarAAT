package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	baseImage        = "citusdata/citus@sha256:d107b77c5d07ed435b72da9c57f6e96494912a52dc84c6a4113de8eb920e7261"
	etcdImage        = "gcr.io/etcd-development/etcd@sha256:159cf260a40becd61d289ef582fe9339a53c7d1bd22d7cffebd213e095625183"
	ownerLabel       = "io.itb.role1-m1.owner"
	ownerValue       = "role1-m1-development"
	gateKey    int64 = 4810301
)

type Result struct {
	Name       string         `json:"name"`
	Status     string         `json:"status"`
	DurationMS int64          `json:"duration_ms"`
	Details    map[string]any `json:"details"`
	Error      string         `json:"error,omitempty"`
}
type Report struct {
	RunID       string         `json:"run_id"`
	Started     string         `json:"started_at"`
	Suite       string         `json:"suite"`
	Doctor      map[string]any `json:"doctor"`
	Results     []Result       `json:"results"`
	Pending     []string       `json:"pending_integration_gates"`
	LocalPassed bool           `json:"local_requested_suites_passed"`
}
type Lab struct {
	Root, Dir, ID, Project, Suite, Password, Image string
	Services                                       []string
	DB                                             []string
	DSN                                            map[string]string
	Report                                         *Report
	commandCount                                   int
}

type tailBuffer struct {
	b     []byte
	limit int
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	n := len(p)
	t.b = append(t.b, p...)
	if len(t.b) > t.limit {
		t.b = t.b[len(t.b)-t.limit:]
	}
	return n, nil
}

func command(ctx context.Context, root string, args ...string) ([]byte, error) {
	c := exec.CommandContext(ctx, args[0], args[1:]...)
	c.Dir = root
	var output tailBuffer
	output.limit = 2 << 20
	c.Stdout = &output
	c.Stderr = &output
	err := c.Run()
	if err != nil {
		return output.b, fmt.Errorf("%s: %w: %s", strings.Join(args, " "), err, string(output.b))
	}
	return output.b, nil
}
func (l *Lab) cmd(ctx context.Context, args ...string) ([]byte, error) {
	l.commandCount++
	b, err := command(ctx, l.Root, args...)
	// Arguments never contain DSNs/passwords. Generated Compose credentials stay in this ignored run directory.
	record := map[string]any{"at": time.Now().UTC(), "argv": args, "error": errString(err)}
	_ = writeJSON(filepath.Join(l.Dir, fmt.Sprintf("command-%03d.json", l.commandCount)), record)
	_ = os.WriteFile(filepath.Join(l.Dir, fmt.Sprintf("command-%03d.log", l.commandCount)), b, 0600)
	return b, err
}
func errString(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}
func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0600)
}

func doctor(ctx context.Context, root, suite string) (map[string]any, error) {
	b, err := command(ctx, root, "docker", "info", "--format", "{{json .}}")
	if err != nil {
		return nil, err
	}
	var info struct {
		MemTotal                            uint64
		NCPU                                int
		ServerVersion, OSType, Architecture string
	}
	if err = json.Unmarshal(b, &info); err != nil {
		return nil, err
	}
	if info.OSType != "linux" {
		return nil, errors.New("Docker must use Linux containers")
	}
	// Three 1-GiB DB limits plus 128-MiB etcd members and 256 MiB VM headroom.
	need := uint64(3456 << 20)
	if suite != "core" && suite != "m2" {
		need = 3712 << 20
	}
	if info.MemTotal < need {
		return nil, fmt.Errorf("blocked: Docker exposes %d MiB; %s needs at least %d MiB; settings were not changed", info.MemTotal>>20, suite, need>>20)
	}
	compose, err := command(ctx, root, "docker", "compose", "version", "--short")
	if err != nil {
		return nil, err
	}
	lock, err := os.ReadFile(filepath.Join(root, "requirements.lock"))
	if err != nil {
		return nil, err
	}
	for _, pin := range []string{"patroni[etcd3]==4.1.5", "psycopg2-binary==2.9.10", "python-etcd==0.4.5"} {
		if !strings.Contains(string(lock), pin+"\n") {
			return nil, fmt.Errorf("missing dependency pin %s", pin)
		}
	}
	tmp, err := os.MkdirTemp("", "role1-m1-doctor-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp)
	for _, s := range []string{"core", "ha"} {
		if suite != "all" && suite != s && !(suite == "m2" && s == "core") {
			continue
		}
		lab := newLab(root, "doctor", s, "doctor-only", "role1-m1-doctor", nil)
		path := filepath.Join(tmp, s+".json")
		if err = writeJSON(path, lab.composeConfig()); err != nil {
			return nil, err
		}
		if _, err = command(ctx, root, "docker", "compose", "-p", lab.Project, "-f", path, "config", "--quiet"); err != nil {
			return nil, err
		}
	}
	return map[string]any{"docker_version": info.ServerVersion, "docker_memory_mib": info.MemTotal >> 20, "cpus": info.NCPU, "compose": strings.TrimSpace(string(compose)), "base_image": baseImage, "etcd_image": etcdImage, "postgres": "16.14", "citus": "14.2.0", "patroni": "4.1.5", "etcd": "3.5.21", "development_only": true}, nil
}

func newLab(root, id, suite, password, image string, report *Report) *Lab {
	return &Lab{Root: root, ID: id, Suite: suite, Project: "r1m1-" + id + "-" + suite, Password: password, Image: image, Dir: filepath.Join(root, "..", "..", "evidence", "role1-m1", id, suite), DSN: map[string]string{}, Report: report}
}

func (l *Lab) composeConfig() map[string]any {
	services := map[string]any{}
	volumes := map[string]any{}
	etcdCount := 1
	if l.Suite == "ha" {
		etcdCount = 3
	}
	var peers, hosts []string
	for i := 1; i <= etcdCount; i++ {
		name := fmt.Sprintf("etcd%d", i)
		peers = append(peers, name+"=http://"+name+":2380")
		hosts = append(hosts, name+":2379")
	}
	labels := map[string]string{ownerLabel: ownerValue, "io.itb.role1-m1.run": l.ID}
	for i := 1; i <= etcdCount; i++ {
		name := fmt.Sprintf("etcd%d", i)
		l.Services = append(l.Services, name)
		volumes[name] = map[string]any{"labels": labels}
		services[name] = map[string]any{"image": etcdImage, "labels": labels, "mem_limit": "128m", "volumes": []string{name + ":/etcd-data"}, "command": []string{"/usr/local/bin/etcd", "--name", name, "--data-dir", "/etcd-data", "--listen-client-urls", "http://0.0.0.0:2379", "--advertise-client-urls", "http://" + name + ":2379", "--listen-peer-urls", "http://0.0.0.0:2380", "--initial-advertise-peer-urls", "http://" + name + ":2380", "--initial-cluster", strings.Join(peers, ","), "--initial-cluster-token", l.Project, "--auto-compaction-retention", "1", "--quota-backend-bytes", "16777216"}}
	}
	l.DB = []string{"coord", "worker1", "worker2"}
	if l.Suite == "ha" {
		l.DB = []string{"pg1", "pg2", "pg3"}
	}
	for i, name := range l.DB {
		group := i
		if l.Suite == "ha" {
			group = 0
		}
		volumes[name] = map[string]any{"labels": labels}
		l.Services = append(l.Services, name)
		services[name] = map[string]any{"image": l.Image, "labels": labels, "mem_limit": "1g", "mem_reservation": "512m", "stop_grace_period": "5s", "healthcheck": map[string]any{"disable": true}, "ports": []string{"127.0.0.1::5432"}, "volumes": []string{name + ":/var/lib/postgresql/data"}, "environment": map[string]string{"M1_NAME": name, "M1_SCOPE": l.Project, "M1_SUITE": l.Suite, "M1_GROUP": fmt.Sprint(group), "M1_ETCD": strings.Join(hosts, ","), "M1_PASSWORD": l.Password}}
	}
	return map[string]any{"services": services, "volumes": volumes, "networks": map[string]any{"default": map[string]any{"labels": labels}}}
}
func (l *Lab) compose(ctx context.Context, args ...string) ([]byte, error) {
	prefix := []string{"docker", "compose", "-p", l.Project, "-f", filepath.Join(l.Dir, "compose.json")}
	return l.cmd(ctx, append(prefix, args...)...)
}
func (l *Lab) start(ctx context.Context) error {
	startup, cancelStartup := context.WithTimeout(ctx, 5*time.Minute)
	defer cancelStartup()
	ctx = startup
	if err := os.MkdirAll(l.Dir, 0700); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(l.Dir, "compose.json"), l.composeConfig()); err != nil {
		return err
	}
	if _, err := l.compose(ctx, "config", "--quiet"); err != nil {
		return err
	}
	if _, err := l.compose(ctx, "up", "-d"); err != nil {
		return err
	}
	for _, service := range l.DB {
		b, err := l.compose(ctx, "port", service, "5432")
		if err != nil {
			return err
		}
		host := strings.TrimSpace(string(b))
		if _, _, err = net.SplitHostPort(host); err != nil {
			return fmt.Errorf("invalid mapped port for %s: %s", service, host)
		}
		u := url.URL{Scheme: "postgres", User: url.UserPassword("postgres", l.Password), Host: host, Path: "/m1", RawQuery: "sslmode=disable&connect_timeout=2"}
		l.DSN[service] = u.String()
	}
	ready, cancel := context.WithTimeout(ctx, 180*time.Second)
	defer cancel()
	if err := until(ready, func() error {
		for _, service := range l.DB {
			c, err := l.connect(ready, service)
			if err != nil {
				return err
			}
			err = c.Ping(ready)
			c.Close(context.Background())
			if err != nil {
				return err
			}
		}
		if l.Suite != "ha" {
			c, err := l.connect(ready, "coord")
			if err != nil {
				return err
			}
			defer c.Close(context.Background())
			var n int
			err = c.QueryRow(ready, "SELECT count(*) FROM pg_dist_node WHERE noderole='primary' AND groupid>0 AND isactive").Scan(&n)
			if err != nil {
				return err
			}
			if n != 2 {
				return fmt.Errorf("waiting for two Citus workers, have %d", n)
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("startup blocked: %w", err)
	}
	return l.versions(ctx)
}
func until(ctx context.Context, check func() error) error {
	var last error
	for {
		if ctx.Err() != nil {
			return fmt.Errorf("%w (last observation: %v)", ctx.Err(), last)
		}
		if last = check(); last == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w (last observation: %v)", ctx.Err(), last)
		case <-time.After(200 * time.Millisecond):
		}
	}
}
func (l *Lab) connect(ctx context.Context, name string) (*pgx.Conn, error) {
	c, err := pgx.Connect(ctx, l.DSN[name])
	if err != nil {
		return nil, fmt.Errorf("%s connection unavailable", name)
	}
	return c, nil
}
func (l *Lab) versions(ctx context.Context) error {
	versions := map[string]any{}
	for _, service := range l.DB {
		c, err := l.connect(ctx, service)
		if err != nil {
			return err
		}
		var pg, citus, full string
		err = c.QueryRow(ctx, "SELECT current_setting('server_version'),extversion,citus_version() FROM pg_extension WHERE extname='citus'").Scan(&pg, &citus, &full)
		c.Close(context.Background())
		if err != nil {
			return err
		}
		if !strings.HasPrefix(pg, "16.14") || !strings.HasPrefix(full, "Citus 14.2.0") {
			return fmt.Errorf("version mismatch %s: PostgreSQL=%s Citus=%s", service, pg, full)
		}
		versions[service] = map[string]string{"postgres": pg, "citus_extension": citus, "citus_build": full}
	}
	b, err := l.compose(ctx, "exec", "-T", l.DB[0], "/opt/patroni/bin/patroni", "--version")
	if err != nil {
		return err
	}
	if !strings.Contains(string(b), "4.1.5") {
		return errors.New("Patroni version mismatch")
	}
	versions["patroni"] = strings.TrimSpace(string(b))
	b, err = l.compose(ctx, "exec", "-T", "etcd1", "/usr/local/bin/etcd", "--version")
	if err != nil {
		return err
	}
	if !strings.Contains(string(b), "3.5.21") {
		return errors.New("etcd version mismatch")
	}
	versions["etcd"] = strings.TrimSpace(string(b))
	b, err = l.compose(ctx, "exec", "-T", l.DB[0], "/opt/patroni/bin/pip", "freeze")
	if err != nil {
		return err
	}
	_ = os.WriteFile(filepath.Join(l.Dir, "resolved-requirements.txt"), b, 0600)
	_, _ = l.compose(ctx, "exec", "-T", l.DB[0], "dpkg-query", "-W")
	return writeJSON(filepath.Join(l.Dir, "versions.json"), versions)
}

func owned(labels map[string]string, project, id string) bool {
	return labels[ownerLabel] == ownerValue && labels["com.docker.compose.project"] == project && labels["io.itb.role1-m1.run"] == id
}
func (l *Lab) container(ctx context.Context, service string) (string, error) {
	if !contains(l.Services, service) {
		return "", fmt.Errorf("service %q does not belong to this lab", service)
	}
	b, err := l.compose(ctx, "ps", "--all", "-q", service)
	if err != nil {
		return "", err
	}
	id := strings.TrimSpace(string(b))
	if id == "" || strings.ContainsAny(id, "\r\n ") {
		return "", errors.New("expected one owned container")
	}
	b, err = l.cmd(ctx, "docker", "inspect", "--format", "{{json .Config.Labels}}", id)
	if err != nil {
		return "", err
	}
	var labels map[string]string
	if json.Unmarshal(b, &labels) != nil || !owned(labels, l.Project, l.ID) {
		return "", errors.New("refusing action on an unowned container")
	}
	return id, nil
}
func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
func (l *Lab) fault(ctx context.Context, action, service string) error {
	if !contains([]string{"stop", "start", "pause", "unpause", "kill"}, action) {
		return errors.New("unsupported fault")
	}
	id, err := l.container(ctx, service)
	if err != nil {
		return err
	}
	args := []string{"docker", action}
	if action == "kill" {
		args = append(args, "--signal", "KILL")
	}
	if action == "stop" {
		args = append(args, "--time", "3")
	}
	_, err = l.cmd(ctx, append(args, id)...)
	if err == nil && action == "start" && contains(l.DB, service) {
		// Docker allocates a new random host binding when a stopped container
		// restarts. Internal Citus DNS stays stable; native probes must refresh.
		b, e := l.compose(ctx, "port", service, "5432")
		if e != nil {
			return e
		}
		host := strings.TrimSpace(string(b))
		if _, _, e = net.SplitHostPort(host); e != nil {
			return e
		}
		u := url.URL{Scheme: "postgres", User: url.UserPassword("postgres", l.Password), Host: host, Path: "/m1", RawQuery: "sslmode=disable&connect_timeout=2"}
		l.DSN[service] = u.String()
	}
	return err
}
func (l *Lab) collect(ctx context.Context) {
	_, _ = l.compose(ctx, "logs", "--no-color", "--tail", "300")
	b, err := l.compose(ctx, "ps", "--all", "-q")
	if err != nil {
		return
	}
	ids := strings.Fields(string(b))
	if len(ids) > 0 {
		_, _ = l.cmd(ctx, append([]string{"docker", "stats", "--no-stream", "--format", "{{json .}}"}, ids...)...)
		for _, id := range ids {
			_, _ = l.cmd(ctx, "docker", "inspect", "--format", "{{json .State}}", id)
		}
	}
}
func (l *Lab) stop(ctx context.Context) error {
	b, err := l.compose(ctx, "ps", "--all", "-q")
	if err != nil {
		return err
	}
	for _, id := range strings.Fields(string(b)) {
		labelsRaw, e := l.cmd(ctx, "docker", "inspect", "--format", "{{json .Config.Labels}}", id)
		if e != nil {
			return e
		}
		var labels map[string]string
		if json.Unmarshal(labelsRaw, &labels) != nil || !owned(labels, l.Project, l.ID) {
			return errors.New("refusing cleanup of unowned resource")
		}
	}
	for _, id := range strings.Fields(string(b)) {
		state, e := l.cmd(ctx, "docker", "inspect", "--format", "{{json .State.Paused}}", id)
		if e != nil {
			return e
		}
		if strings.TrimSpace(string(state)) == "true" {
			if _, e = l.cmd(ctx, "docker", "unpause", id); e != nil {
				return e
			}
		}
	}
	_, err = l.compose(ctx, "down", "--timeout", "5")
	return err // Deliberately no --volumes.
}
func (l *Lab) result(ctx context.Context, name string, fn func(context.Context, map[string]any) error) {
	caseCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	details := map[string]any{}
	start := time.Now()
	err := fn(caseCtx, details)
	status := "passed"
	if err != nil {
		status = "failed"
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "blocked:") {
			status = "blocked"
		}
	}
	r := Result{Name: name, Status: status, DurationMS: time.Since(start).Milliseconds(), Details: details, Error: errString(err)}
	l.Report.Results = append(l.Report.Results, r)
	_ = writeJSON(filepath.Join(l.Dir, name+".json"), r)
	fmt.Printf("%s %s (%d ms)\n", status, name, r.DurationMS)
}

func runLab(ctx context.Context, root, suite string) error {
	info, err := doctor(ctx, root, suite)
	if err != nil {
		return err
	}
	random := make([]byte, 16)
	if _, err = rand.Read(random); err != nil {
		return err
	}
	id := time.Now().UTC().Format("20060102t150405") + "-" + hex.EncodeToString(random[:4])
	password := hex.EncodeToString(random)
	dir := filepath.Join(root, "..", "..", "evidence", "role1-m1", id)
	if err = os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	report := &Report{RunID: id, Started: time.Now().UTC().Format(time.RFC3339Nano), Suite: suite, Doctor: info, Results: []Result{}, Pending: []string{"Integrated Citus coordinator/worker failover and distributed transaction-decision durability", "Four nonempty worker groups, three copies each, on four separate-kernel nodes", "Role 2 ownership/expiry, deployment/capacity and shared evidence agreements 5/15/16"}}
	defer func() {
		report.LocalPassed = true
		for _, r := range report.Results {
			if r.Status != "passed" {
				report.LocalPassed = false
			}
		}
		_ = writeJSON(filepath.Join(dir, "summary.json"), report)
	}()
	hash := sha256.New()
	for _, name := range []string{"Dockerfile", "requirements.lock", "entrypoint.py"} {
		b, e := os.ReadFile(filepath.Join(root, name))
		if e != nil {
			return e
		}
		hash.Write(b)
	}
	image := "role1-m1-lab:" + hex.EncodeToString(hash.Sum(nil))[:16]
	build := newLab(root, id, "build", password, image, report)
	build.Dir = dir
	buildCtx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	_, err = build.cmd(buildCtx, "docker", "build", "--tag", image, root)
	cancel()
	if err != nil {
		report.Results = append(report.Results, Result{Name: "image_build", Status: "blocked", Error: errString(err)})
		return fmt.Errorf("build blocked; evidence %s", dir)
	}
	_, _ = build.cmd(ctx, "docker", "image", "inspect", image, "--format", "{{json .}}")
	suites := []string{"core", "ha"}
	if suite != "all" {
		suites = []string{suite}
	}
	for _, s := range suites {
		if ctx.Err() != nil {
			report.Results = append(report.Results, Result{Name: s + "_startup", Status: "blocked", Error: ctx.Err().Error()})
			break
		}
		lab := newLab(root, id, s, password, image, report)
		err = lab.start(ctx)
		if err != nil {
			report.Results = append(report.Results, Result{Name: s + "_startup", Status: "blocked", Error: errString(err)})
		} else if s == "m2" {
			lab.result(ctx, "m2_write_core", lab.m2)
		} else if s == "core" {
			lab.runCore(ctx)
		} else {
			lab.result(ctx, "durable_failover", lab.ha)
		}
		cleanup, c := context.WithTimeout(context.Background(), 90*time.Second)
		lab.collect(cleanup)
		stopErr := lab.stop(cleanup)
		c()
		if stopErr != nil {
			report.Results = append(report.Results, Result{Name: s + "_cleanup", Status: "failed", Error: errString(stopErr)})
			break
		}
	}
	fmt.Println("Evidence:", dir)
	for _, r := range report.Results {
		if r.Status != "passed" {
			return errors.New("one or more requested local proofs failed or were blocked; inspect summary.json")
		}
	}
	return nil
}

var runPattern = regexp.MustCompile(`^[0-9]{8}t[0-9]{6}-[a-f0-9]{8}$`)

func downRun(ctx context.Context, root, id string) error {
	if !runPattern.MatchString(id) {
		return errors.New("down requires the exact --run ID printed by run")
	}
	for _, suite := range []string{"core", "ha", "m2"} {
		l := newLab(root, id, suite, "", "", nil)
		data, err := os.ReadFile(filepath.Join(l.Dir, "compose.json"))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		var cfg struct {
			Services map[string]json.RawMessage `json:"services"`
		}
		if err = json.Unmarshal(data, &cfg); err != nil {
			return err
		}
		for service := range cfg.Services {
			l.Services = append(l.Services, service)
		}
		sort.Strings(l.Services)
		b, err := l.compose(ctx, "ps", "--all", "-q")
		if err != nil {
			return err
		}
		if len(bytes.TrimSpace(b)) == 0 {
			continue
		}
		if err = l.stop(ctx); err != nil {
			return err
		}
	}
	return nil
}
