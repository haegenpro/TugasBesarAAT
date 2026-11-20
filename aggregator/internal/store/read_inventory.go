package store

import (
	"aggregator/internal/audit"
	"aggregator/internal/canonical"
	"aggregator/internal/pagecursor"
	"aggregator/internal/platform"
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
	placement                 *podPlacementCache
}

// Pod-to-node binding is immutable for a pod's lifetime. Refresh this placement
// metadata outside the read budget; all physical-primary, lease and row checks
// remain live on every read. Expired metadata is never served indefinitely.
type podPlacementCache struct {
	mu      sync.RWMutex
	nodes   map[string]string
	updated time.Time
	ready   chan struct{}
	done    chan struct{}
	cancel  context.CancelFunc
}

const placementMaxAge = 10 * time.Second

func newPodPlacementCache(load func(context.Context) (map[string]string, error)) *podPlacementCache {
	ctx, cancel := context.WithCancel(context.Background())
	c := &podPlacementCache{ready: make(chan struct{}), done: make(chan struct{}), cancel: cancel}
	go func() {
		defer close(c.done)
		first := true
		for ctx.Err() == nil {
			call, stop := context.WithTimeout(ctx, 5*time.Second)
			nodes, err := load(call)
			stop()
			if err == nil {
				c.mu.Lock()
				c.nodes = nodes
				c.updated = time.Now()
				c.mu.Unlock()
				platform.Add("read_placement_refresh_success_total", 1)
				if first {
					close(c.ready)
					first = false
				}
			} else {
				platform.Add("read_placement_refresh_failures_total", 1)
			}
			timer := time.NewTimer(time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
	return c
}

func (c *podPlacementCache) current() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return !c.updated.IsZero() && time.Since(c.updated) <= placementMaxAge
}

func (c *podPlacementCache) read(ctx context.Context) (map[string]string, error) {
	select {
	case <-ctx.Done():
		return nil, canonical.Unavailable()
	case <-c.ready:
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	age := time.Since(c.updated)
	platform.Gauge("read_placement_age_seconds", age.Seconds())
	if c.updated.IsZero() || age > placementMaxAge {
		return nil, canonical.Unavailable()
	}
	out := make(map[string]string, len(c.nodes))
	for name, node := range c.nodes {
		out[name] = node
	}
	return out, nil
}

func (c *podPlacementCache) close() { c.cancel(); <-c.done }

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
	provider := &KubernetesInventory{Store: s, API: strings.TrimRight(api, "/"), Namespace: ns, TokenFile: tokenFile, replication: replication, Kube: &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, ForceAttemptHTTP2: true}}, Patroni: &http.Client{Timeout: 600 * time.Millisecond}}
	if s.readPlacement == nil {
		s.readPlacement = newPodPlacementCache(provider.loadPodNodes)
		s.readPlacementOwner = true
	}
	provider.placement = s.readPlacement
	groups := map[int]*pgxpool.Pool{}
	for i, g := range replication.cfg.Groups {
		groups[g.ID] = replication.pools[i]
	}
	s.Reads = ReadOptions{Signer: pagecursor.Signer{Key: key}, Groups: groups, Inventory: provider.Discover, Authority: provider.Authoritative, Cut: provider.ReadCut, MetadataReady: s.readPlacement.current}
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
		// Patroni sends a small status body. Finish it so repeated authority
		// checks can reuse the idle connection instead of redialing each time.
		_, err = io.Copy(io.Discard, io.LimitReader(resp.Body, 16<<10))
		return err
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 512<<10)).Decode(out)
}
func (k *KubernetesInventory) Authoritative(ctx context.Context, q readQuerier) (replicationStatus, error) {
	ctx, span := platform.Start(ctx, "canonical read authority")
	defer span.End()
	status, err := readReplication(ctx, q)
	if err != nil {
		return status, canonical.Unavailable()
	}
	return status, k.checkAuthority(ctx, status, true)
}

func (k *KubernetesInventory) checkAuthority(ctx context.Context, status replicationStatus, lease bool) error {
	g := k.replication.cfg.Groups[0]
	if !status.Primary || status.SystemID != g.SystemID || status.Formation != k.replication.cfg.Formation || status.GroupID != 0 {
		return canonical.Unavailable()
	}
	for _, member := range g.Members {
		if member.Name == status.Member {
			if !lease {
				return nil
			}
			return k.request(ctx, k.Patroni, "http://"+member.Host+":8008/primary", "", nil)
		}
	}
	return canonical.Unavailable()
}

func (k *KubernetesInventory) ReadCut(ctx context.Context, q readQuerier, final bool) (int64, bool, replicationStatus, error) {
	ctx, span := platform.Start(ctx, "canonical visibility and authority")
	defer span.End()
	var generation int64
	var pending bool
	var raw []byte
	var status replicationStatus
	err := q.QueryRow(ctx, `SELECT generation,EXISTS(SELECT 1 FROM canonical_attempts WHERE state='pending'),public.platform_replication_status() FROM canonical_visibility WHERE singleton`).Scan(&generation, &pending, &raw)
	if err == nil {
		err = json.Unmarshal(raw, &status)
	}
	if err == nil {
		err = k.checkAuthority(ctx, status, final)
	}
	return generation, pending, status, err
}
func (k *KubernetesInventory) podNodes(ctx context.Context) (map[string]string, error) {
	if k.placement != nil {
		return k.placement.read(ctx)
	}
	return k.loadPodNodes(ctx)
}

func (k *KubernetesInventory) loadPodNodes(ctx context.Context) (map[string]string, error) {
	ctx, span := platform.Start(ctx, "canonical pod placement")
	defer span.End()
	token, err := os.ReadFile(k.TokenFile)
	if err != nil {
		return nil, canonical.Unavailable()
	}
	var pods podList
	endpoint := k.API + "/api/v1/namespaces/" + url.PathEscape(k.Namespace) + "/pods?limit=100&labelSelector=" + url.QueryEscape("platform.aat/formation="+k.replication.cfg.Formation+",component=storage-postgres")
	if err = k.request(ctx, k.Kube, endpoint, strings.TrimSpace(string(token)), &pods); err != nil || pods.Metadata.Continue != "" {
		return nil, canonical.Unavailable()
	}
	nodes := map[string]string{}
	for _, p := range pods.Items {
		if p.Metadata.Labels["platform.aat/formation"] == k.replication.cfg.Formation && p.Metadata.Labels["component"] == "storage-postgres" {
			nodes[p.Metadata.Name] = p.Spec.Node
		}
	}
	return nodes, nil
}

// /primary includes the same Patroni state document as /patroni, with status
// 200 only while this member owns the leader lock. Replicas return 503 with
// their state document. One exchange obtains both role and lease evidence.
func (k *KubernetesInventory) memberUnit(ctx context.Context, group int, member ReplicationMember) audit.Unit {
	u := audit.Unit{Name: member.Name, Host: member.Host, Group: group}
	req, err := http.NewRequestWithContext(ctx, "GET", "http://"+member.Host+":8008/primary", nil)
	if err != nil {
		return u
	}
	resp, err := k.Patroni.Do(req)
	if err != nil {
		return u
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusServiceUnavailable {
		return u
	}
	var p struct {
		Role  string `json:"role"`
		State string `json:"state"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 512<<10)).Decode(&p) != nil || p.State != "running" {
		return u
	}
	switch p.Role {
	case "master", "primary":
		u.Primary = true
		u.RoleKnown = resp.StatusCode == http.StatusOK
	case "replica", "sync_standby", "quorum_standby":
		u.RoleKnown = resp.StatusCode == http.StatusServiceUnavailable
	}
	return u
}

func (k *KubernetesInventory) Discover(ctx context.Context) (audit.Inventory, error) {
	ctx, span := platform.Start(ctx, "canonical read inventory")
	defer span.End()
	out := audit.Inventory{Units: []audit.Unit{}}
	var wg sync.WaitGroup
	var nodes map[string]string
	var nodesErr, topologyErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		nodes, nodesErr = k.podNodes(ctx)
	}()
	go func() {
		defer wg.Done()
		if topology, ok := ctx.Value(inventoryTopologyKey{}).(string); ok {
			out.Topology = topology
		} else {
			out.Topology, _, topologyErr = k.Store.primaryTopology(ctx)
		}
	}()
	units := make([][]audit.Unit, len(k.replication.cfg.Groups))
	statuses := make([]replicationStatus, len(units))
	statusErrors := make([]error, len(units))
	for i, group := range k.replication.cfg.Groups {
		if group.ID == 0 && k.replication.cfg.Mode == "citus" {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			call, stop := context.WithTimeout(ctx, 750*time.Millisecond)
			defer stop()
			statuses[i], statusErrors[i] = readReplication(call, k.replication.pools[i])
		}()
		units[i] = make([]audit.Unit, len(group.Members))
		for j, member := range group.Members {
			wg.Add(1)
			go func() {
				defer wg.Done()
				call, stop := context.WithTimeout(ctx, 600*time.Millisecond)
				defer stop()
				units[i][j] = k.memberUnit(call, group.ID, member)
			}()
		}
	}
	wg.Wait()
	if nodesErr != nil || topologyErr != nil || k.replication.validateTopology(out.Topology) != nil {
		return out, canonical.Unavailable()
	}
	// Match any claimed primary to the actual routed PostgreSQL backend and its
	// immutable system identity. Multiple leaders or mismatched routing fail shut.
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
		for _, u := range units[i] {
			if u.Primary && u.RoleKnown {
				count++
			}
		}
		for _, u := range units[i] {
			u.Node = nodes[u.Name]
			if u.Primary && (u.Name != primary || count != 1) {
				u.RoleKnown = false
			}
			u.Available = u.RoleKnown && u.Node != ""
			out.Units = append(out.Units, u)
		}
	}
	return out, nil
}
