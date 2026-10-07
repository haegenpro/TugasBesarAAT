// monitoring-launch validates configuration before starting an upstream binary.
// It contains infrastructure configuration only, no application business logic.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"text/template"
	"time"
)

var streamName = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

var required = map[string][]string{
	"nats-exporter": {"NATS_MONITOR_URL"},
	"grafana":       {"GRAFANA_BIND_ADDRESS", "GRAFANA_HOST_PORT", "GF_SECURITY_ADMIN_USER", "GF_SECURITY_ADMIN_PASSWORD", "PROMETHEUS_URL", "LOKI_URL", "DLQ_STREAM_NAME"},
	"prometheus":    {"METRICS_RETENTION_DAYS", "ALLOY_METRICS_URL", "LOKI_METRICS_URL", "NATS_EXPORTER_METRICS_URL"},
	"loki":          {"LOG_RETENTION_DAYS"},
	"alloy":         {"COMPOSE_PROJECT_NAME", "PROMETHEUS_WRITE_URL", "LOKI_PUSH_URL", "BMKG_HEALTH_URL", "PVMBG_HEALTH_URL", "AGGREGATOR_HEALTH_URL", "AGGREGATOR_READY_URL", "AUTH_HEALTH_URL", "CLIENT_API_HEALTH_URL", "CLIENT_API_READY_URL", "DASHBOARD_HEALTH_URL", "DASHBOARD_READY_URL", "NOTIFIER_HEALTH_URL", "NOTIFIER_READY_URL", "REGISTRY_READY_URL", "NATS_HEALTH_URL"},
}

func validate(service string, get func(string) string) (map[string]string, error) {
	keys, ok := required[service]
	if !ok {
		return nil, fmt.Errorf("unknown monitoring service %q", service)
	}
	values := make(map[string]string, len(keys))
	var missing []string
	for _, key := range keys {
		values[key] = get(key)
		if strings.TrimSpace(values[key]) == "" {
			missing = append(missing, key)
		}
	}
	if len(missing) != 0 {
		return nil, fmt.Errorf("required environment variables missing or empty: %s", strings.Join(missing, ", "))
	}
	for _, key := range keys {
		value := values[key]
		switch {
		case strings.HasSuffix(key, "_URL"):
			u, err := url.Parse(value)
			if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Fragment != "" {
				return nil, fmt.Errorf("%s must be an absolute HTTP(S) URL without credentials or a fragment", key)
			}
			if port := u.Port(); port != "" {
				if err := validPort(key, port); err != nil {
					return nil, err
				}
			}
		case strings.HasSuffix(key, "RETENTION_DAYS"):
			days, err := strconv.Atoi(value)
			if err != nil || days < 1 || days > 36500 {
				return nil, fmt.Errorf("%s must be an integer between 1 and 36500", key)
			}
		case key == "DLQ_STREAM_NAME":
			if !streamName.MatchString(value) {
				return nil, fmt.Errorf("%s must contain only letters, digits, underscores, or hyphens", key)
			}
		case key == "GRAFANA_HOST_PORT":
			if err := validPort(key, value); err != nil {
				return nil, err
			}
		case key == "GRAFANA_BIND_ADDRESS":
			if net.ParseIP(value) == nil {
				return nil, fmt.Errorf("%s must be an explicit IP address", key)
			}
		}
	}
	return values, nil
}

func validPort(key, value string) error {
	n, err := strconv.Atoi(value)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("%s must contain a port between 1 and 65535", key)
	}
	return nil
}

func render(service string, values map[string]string, source, dest string) error {
	if err := os.MkdirAll(dest, 0700); err != nil {
		return err
	}
	funcs := template.FuncMap{
		"quote":     func(v string) string { b, _ := json.Marshal(v); return string(b) },
		"hours":     func(v string) string { d, _ := strconv.Atoi(v); return strconv.Itoa(d*24) + "h" },
		"urlHost":   func(v string) string { u, _ := url.Parse(v); return u.Host },
		"urlScheme": func(v string) string { u, _ := url.Parse(v); return u.Scheme },
		"urlPath": func(v string) string {
			u, _ := url.Parse(v)
			if u.Path == "" {
				return "/"
			}
			return u.EscapedPath()
		},
	}
	matches, err := filepath.Glob(filepath.Join(source, service, "*.tmpl"))
	if err != nil {
		return err
	}
	if len(matches) == 0 {
		return fmt.Errorf("no configuration templates for %s", service)
	}
	for _, path := range matches {
		t, err := template.New(filepath.Base(path)).Funcs(funcs).Option("missingkey=error").ParseFiles(path)
		if err != nil {
			return fmt.Errorf("parse %s", filepath.Base(path))
		}
		name := strings.TrimSuffix(filepath.Base(path), ".tmpl")
		out, err := os.OpenFile(filepath.Join(dest, name), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		err = t.Execute(out, values)
		closeErr := out.Close()
		// Never report template execution errors, which can contain environment values.
		if err != nil {
			return fmt.Errorf("render %s failed", name)
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func prepare(service string, values map[string]string, source, dest string) error {
	// The exporter takes its configuration as arguments rather than a file.
	if service == "nats-exporter" {
		return nil
	}
	if err := render(service, values, source, dest); err != nil {
		return err
	}
	if service != "grafana" {
		return nil
	}
	for _, kind := range []string{"datasources", "dashboards", "alerting"} {
		dir := filepath.Join(dest, "provisioning", kind)
		if err := os.MkdirAll(dir, 0700); err != nil {
			return err
		}
		if err := os.Rename(filepath.Join(dest, kind+".yml"), filepath.Join(dir, "monitoring.yml")); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(filepath.Join(dest, "dashboards"), 0700); err != nil {
		return err
	}
	if err := os.Rename(filepath.Join(dest, "dashboard.json"), filepath.Join(dest, "dashboards", "monitoring.json")); err != nil {
		return err
	}
	return os.Setenv("GF_PATHS_PROVISIONING", filepath.Join(dest, "provisioning"))
}

func probe(endpoint string) error {
	client := &http.Client{Timeout: 4 * time.Second}
	resp, err := client.Get(endpoint)
	if err != nil {
		return fmt.Errorf("health endpoint unavailable")
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health endpoint returned %d", resp.StatusCode)
	}
	return nil
}

func run(args []string) error {
	if len(args) == 2 && args[0] == "probe" {
		return probe(args[1])
	}
	if len(args) < 2 {
		return fmt.Errorf("usage: monitoring-launch SERVICE PROGRAM [ARGS...]")
	}
	values, err := validate(args[0], os.Getenv)
	if err != nil {
		return err
	}
	if err := prepare(args[0], values, "/etc/monitoring/templates", "/tmp/monitoring"); err != nil {
		return err
	}
	if args[0] == "nats-exporter" {
		args = append(args, values["NATS_MONITOR_URL"])
	}
	return syscall.Exec(args[1], args[1:], os.Environ())
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "invalid monitoring startup:", err)
		os.Exit(1)
	}
}
