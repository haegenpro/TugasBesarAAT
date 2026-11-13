package store

import (
	"aggregator/internal/audit"
	"aggregator/internal/canonical"
	"aggregator/internal/pagecursor"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type KubernetesInventory struct {
	Store                     *Store
	API, Namespace, TokenFile string
	Kube, Patroni             *http.Client
	replication               *replicatedDurability
}
type podList struct {
	Metadata struct {
		Continue string `json:"continue"`
	} `json:"metadata"`
	Items []struct {
		Metadata struct {
			Name   string            `json:"name"`
			Labels map[string]string `json:"labels"`
		} `json:"metadata"`
		Spec struct {
			Node string `json:"nodeName"`
		} `json:"spec"`
		Status struct {
			Phase string `json:"phase"`
		} `json:"status"`
	} `json:"items"`
}

func (s *Store) ConfigureLiveReads(key []byte) error {
	if len(key) < 32 {
		return fmt.Errorf("AUDIT_CURSOR_KEY requires at least 32 bytes")
	}
	replication, ok := s.Canonical.Durability.(*replicatedDurability)
	if !ok {
		return fmt.Errorf("audit reads require captured storage identities")
	}
	ns := os.Getenv("POD_NAMESPACE")
	if ns == "" {
		return fmt.Errorf("POD_NAMESPACE required for runtime node lookup")
	}
	root := "/var/run/secrets/kubernetes.io/serviceaccount/"
	caFile := os.Getenv("AUDIT_KUBERNETES_CA_FILE")
	if caFile == "" {
		caFile = root + "ca.crt"
	}
	ca, err := os.ReadFile(caFile)
	if err != nil {
		return fmt.Errorf("Kubernetes CA unavailable")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return fmt.Errorf("invalid Kubernetes CA")
	}
	api := os.Getenv("AUDIT_KUBERNETES_URL")
	if api == "" {
		api = "https://kubernetes.default.svc"
	}
	u, err := url.Parse(api)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("trusted HTTPS Kubernetes API required")
	}
	tokenFile := os.Getenv("AUDIT_KUBERNETES_TOKEN_FILE")
	if tokenFile == "" {
		tokenFile = root + "token"
	}
	provider := &KubernetesInventory{Store: s, API: strings.TrimRight(api, "/"), Namespace: ns, TokenFile: tokenFile, replication: replication, Kube: &http.Client{Timeout: 600 * time.Millisecond, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}}, Patroni: &http.Client{Timeout: 400 * time.Millisecond}}
	groups := map[int]*pgxpool.Pool{}
	for i, g := range replication.cfg.Groups {
		groups[g.ID] = replication.pools[i]
	}
	s.Reads = ReadOptions{Signer: pagecursor.Signer{Key: key}, Groups: groups, Inventory: provider.Discover, Authority: provider.Authoritative}
	return nil
}
func (k *KubernetesInventory) request(ctx context.Context, client *http.Client, endpoint, token string, out any) error {
	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("runtime metadata unavailable")
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 512<<10)).Decode(out)
}
func (k *KubernetesInventory) Authoritative(ctx context.Context) error {
	status, err := readReplication(ctx, k.Store.Pool)
	if err != nil {
		return canonical.Unavailable()
	}
	g := k.replication.cfg.Groups[0]
	if !status.Primary || status.SystemID != g.SystemID || status.Formation != k.replication.cfg.Formation || status.GroupID != 0 {
		return canonical.Unavailable()
	}
	for _, member := range g.Members {
		if member.Name == status.Member {
			return k.request(ctx, k.Patroni, "http://"+member.Host+":8008/primary", "", nil)
		}
	}
	return canonical.Unavailable()
}
func (k *KubernetesInventory) Discover(ctx context.Context) (audit.Inventory, error) {
	out := audit.Inventory{Units: []audit.Unit{}}
	topology, _, err := k.Store.primaryTopology(ctx)
	if err != nil || k.replication.validateTopology(topology) != nil {
		return out, canonical.Unavailable()
	}
	out.Topology = topology
	token, err := os.ReadFile(k.TokenFile)
	if err != nil {
		return out, canonical.Unavailable()
	}
	var pods podList
	endpoint := k.API + "/api/v1/namespaces/" + url.PathEscape(k.Namespace) + "/pods?limit=100&labelSelector=" + url.QueryEscape("platform.aat/formation="+k.replication.cfg.Formation+",component=storage-postgres")
	if err = k.request(ctx, k.Kube, endpoint, strings.TrimSpace(string(token)), &pods); err != nil || pods.Metadata.Continue != "" {
		return out, canonical.Unavailable()
	}
	nodes := map[string]string{}
	for _, p := range pods.Items {
		if p.Metadata.Labels["platform.aat/formation"] == k.replication.cfg.Formation && p.Metadata.Labels["component"] == "storage-postgres" {
			nodes[p.Metadata.Name] = p.Spec.Node
		}
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, group := range k.replication.cfg.Groups {
		if group.ID == 0 && k.replication.cfg.Mode == "citus" {
			continue
		}
		for _, member := range group.Members {
			wg.Add(1)
			go func(g ReplicationGroup, m ReplicationMember) {
				defer wg.Done()
				unit := audit.Unit{Name: m.Name, Node: nodes[m.Name], Host: m.Host, Group: g.ID}
				var p struct {
					Role  string `json:"role"`
					State string `json:"state"`
				}
				err := k.request(ctx, k.Patroni, "http://"+m.Host+":8008/patroni", "", &p)
				if err == nil && p.State == "running" && unit.Node != "" {
					unit.Available = true
					switch p.Role {
					case "master", "primary":
						unit.Primary = true
						unit.RoleKnown = k.request(ctx, k.Patroni, "http://"+m.Host+":8008/primary", "", nil) == nil
					case "replica", "sync_standby", "quorum_standby":
						unit.RoleKnown = true
					}
					if !unit.RoleKnown {
						unit.Available = false
					}
				}
				mu.Lock()
				out.Units = append(out.Units, unit)
				mu.Unlock()
			}(group, member)
		}
	}
	wg.Wait()
	// Match any claimed primary to the actual routed PostgreSQL backend and its
	// immutable system identity. Multiple leaders or mismatched routing fail shut.
	statuses := make([]replicationStatus, len(k.replication.cfg.Groups))
	statusErrors := make([]error, len(statuses))
	for i := range statuses {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			call, stop := context.WithTimeout(ctx, 450*time.Millisecond)
			defer stop()
			statuses[i], statusErrors[i] = readReplication(call, k.replication.pools[i])
		}(i)
	}
	wg.Wait()
	for i, g := range k.replication.cfg.Groups {
		if g.ID == 0 && k.replication.cfg.Mode == "citus" {
			continue
		}
		status, err := statuses[i], statusErrors[i]
		primary := ""
		if err == nil && status.Primary && status.SystemID == g.SystemID && status.Formation == k.replication.cfg.Formation && status.GroupID == g.ID {
			primary = status.Member
		}
		count := 0
		for _, u := range out.Units {
			if u.Group == g.ID && u.Primary && u.RoleKnown && u.Available {
				count++
			}
		}
		for j := range out.Units {
			u := &out.Units[j]
			if u.Group == g.ID && u.Primary && (u.Name != primary || count != 1) {
				u.RoleKnown = false
				u.Available = false
			}
		}
	}
	return out, nil
}
