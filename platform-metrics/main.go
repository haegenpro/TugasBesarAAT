// platform-metrics exports namespaced metrics-server observations. It cannot
// read Secrets, execute in pods, change workloads, or acknowledge application data.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type pod struct {
	Metadata struct {
		Name      string            `json:"name"`
		Namespace string            `json:"namespace"`
		Labels    map[string]string `json:"labels"`
	} `json:"metadata"`
	Spec struct {
		Node string `json:"nodeName"`
	} `json:"spec"`
}
type usage struct {
	Metadata struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"metadata"`
	Containers []struct {
		Name  string            `json:"name"`
		Usage map[string]string `json:"usage"`
	} `json:"containers"`
}
type collector struct {
	mu              sync.RWMutex
	text            string
	updated         time.Time
	success         bool
	client          *http.Client
	base, namespace string
}

func quantity(raw string, cpu bool) (float64, error) {
	suffixes := map[string]float64{"": 1, "n": 1e-9, "u": 1e-6, "m": 1e-3}
	if !cpu {
		suffixes = map[string]float64{"": 1, "Ki": 1024, "Mi": 1 << 20, "Gi": 1 << 30, "Ti": 1 << 40, "k": 1000, "M": 1e6, "G": 1e9}
	}
	// Longest suffix first prevents an empty suffix from consuming a unit.
	for _, suffix := range []string{"Ki", "Mi", "Gi", "Ti", "n", "u", "m", "k", "M", "G", ""} {
		factor, ok := suffixes[suffix]
		if !ok || !strings.HasSuffix(raw, suffix) {
			continue
		}
		value, err := strconv.ParseFloat(strings.TrimSuffix(raw, suffix), 64)
		if err == nil && value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0) {
			return value * factor, nil
		}
	}
	return 0, fmt.Errorf("invalid resource quantity")
}

func (c *collector) get(ctx context.Context, path string, out any) error {
	token, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	reply, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer reply.Body.Close()
	if reply.StatusCode != 200 {
		return fmt.Errorf("metrics API status %d", reply.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(reply.Body, (8<<20)+1))
	if err != nil || len(raw) > 8<<20 {
		return fmt.Errorf("metrics response unavailable or too large")
	}
	return json.Unmarshal(raw, out)
}

func (c *collector) poll(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	var pods struct {
		Items []pod `json:"items"`
	}
	var metrics struct {
		Items []usage `json:"items"`
	}
	err := c.get(ctx, "/api/v1/namespaces/"+c.namespace+"/pods", &pods)
	if err == nil {
		err = c.get(ctx, "/apis/metrics.k8s.io/v1beta1/namespaces/"+c.namespace+"/pods", &metrics)
	}
	text := ""
	missing := 0
	if err == nil {
		lookup := map[string]pod{}
		for _, p := range pods.Items {
			lookup[p.Metadata.Name] = p
		}
		var b strings.Builder
		for _, m := range metrics.Items {
			p, ok := lookup[m.Metadata.Name]
			if !ok || m.Metadata.Namespace != c.namespace {
				missing++
				continue
			}
			role := p.Metadata.Labels["role"]
			switch role {
			case "aggregator", "store", "broker", "consumer", "api", "mock", "sensor", "other":
			default:
				role = "other"
			}
			for _, container := range m.Containers {
				cpu, e1 := quantity(container.Usage["cpu"], true)
				memory, e2 := quantity(container.Usage["memory"], false)
				if e1 != nil || e2 != nil {
					missing++
					continue
				}
				labels := fmt.Sprintf("namespace=%q,pod=%q,node=%q,container=%q,role=%q", c.namespace, m.Metadata.Name, p.Spec.Node, container.Name, role)
				fmt.Fprintf(&b, "platform_pod_cpu_cores{%s} %g\nplatform_pod_memory_bytes{%s} %g\n", labels, cpu, labels, memory)
			}
		}
		fmt.Fprintf(&b, "platform_metrics_missing_samples %d\n", missing)
		text = b.String()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.success = err == nil
	if err == nil {
		c.text = text
		c.updated = time.Now()
	}
}

func (c *collector) metrics(w http.ResponseWriter, _ *http.Request) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	ok := 0
	if c.success {
		ok = 1
	}
	fmt.Fprintf(w, "platform_metrics_collection_success %d\n", ok)
	if !c.updated.IsZero() {
		fmt.Fprintf(w, "platform_metrics_snapshot_age_seconds %g\n", time.Since(c.updated).Seconds())
		fmt.Fprint(w, c.text)
	}
}

func main() {
	namespace := os.Getenv("POD_NAMESPACE")
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT_HTTPS")
	if namespace == "" || host == "" || port == "" {
		log.Fatal("in-cluster namespace and API environment required")
	}
	ca, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	if err != nil {
		log.Fatal("cluster CA unavailable")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		log.Fatal("invalid cluster CA")
	}
	c := &collector{namespace: namespace, base: "https://" + net.JoinHostPort(host, port), client: &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}}}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		c.poll(ctx)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.poll(ctx)
			}
		}
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", c.metrics)
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "ok") })
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		c.mu.RLock()
		defer c.mu.RUnlock()
		if !c.success {
			w.WriteHeader(503)
		}
		fmt.Fprint(w, "metrics collector")
	})
	server := &http.Server{Addr: ":9101", Handler: mux, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(cleanup)
	}()
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal("metrics listener failed")
	}
}
