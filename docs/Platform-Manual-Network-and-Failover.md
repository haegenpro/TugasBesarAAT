# Manual network checks and administration without A

Continue after the [manual K3s bootstrap](Platform-Manual-K3s-Setup.md). All four
nodes must be Ready. These are instructions for the operator; they have not
been executed against the physical cluster by the repository agent.

## Administer from B or C now

On either Ubuntu server, run:

```bash
sudo k3s kubectl get nodes -o wide
sudo k3s kubectl get --raw=/readyz
```

Each server has its own local API and administrator kubeconfig. Using A's
address in the original join command does not make it a permanent leader.
Registered K3s agents discover the server endpoints and maintain a client-side
load balancer. Any two of the three healthy etcd servers can maintain quorum;
one-server failure tolerance is a topology property until actually tested.
See [K3s architecture](https://docs.k3s.io/architecture).

## Spread DNS before testing an A outage

The operator's first output showed only one CoreDNS pod, on A. That is still a
DNS dependency even though etcd has three servers. Run on **B or C**:

```bash
sudo k3s kubectl scale deployment coredns -n kube-system --replicas=3
sudo k3s kubectl rollout status deployment/coredns -n kube-system --timeout=300s
sudo k3s kubectl get pods -n kube-system -l k8s-app=kube-dns -o wide
```

Expect three Ready DNS pods on three different nodes. The pinned K3s CoreDNS
manifest already has a hard hostname topology-spread constraint, so retain that
constraint rather than adding another scheduling rule. If the replicas do not
spread or remain Pending, inspect the deployment/events before fault testing.
Record this replica setting in the later managed deployment and verify it
after infrastructure reconciliation/reconfiguration. Source:
[pinned CoreDNS manifest](https://github.com/k3s-io/k3s/blob/v1.37.2%2Bk3s1/manifests/coredns.yaml).

## Verify ordinary pod traffic across all four nodes

The Tester Agent uses host networking and cannot establish this result. Use
the [temporary network probe](../deployment/platform/manual-network-probe.yaml)
to create one ordinary pod per cluster-labelled node, plus a ClusterIP Service.
Each pod serves its actual node name on port 8080. The image digest was checked
against [Docker Official Images metadata](https://github.com/docker-library/repo-info/blob/master/repos/busybox/remote/1.37.0.md).
Requests/limits are 128/256 MiB per pod; persistent application data is not used.

From a Mint terminal in the repository root, copy the manifest to **B**, replacing
`B_IP` with its actual guest address:

```bash
scp -o PubkeyAuthentication=no deployment/platform/manual-network-probe.yaml aat@B_IP:~/aat-manual-network-probe.yaml
```

On B, apply and check placement:

```bash
sudo k3s kubectl apply -f ~/aat-manual-network-probe.yaml
sudo k3s kubectl rollout status daemonset/aat-manual-netcheck -n sistem-informasi-kebencanaan-bnpb --timeout=300s
sudo k3s kubectl get pods -n sistem-informasi-kebencanaan-bnpb -l app=aat-manual-netcheck -o wide
```

Verify four Ready pods, one on each expected node. Then run this block on B:

```bash
AAT_NS=sistem-informasi-kebencanaan-bnpb
AAT_PODS=$(sudo k3s kubectl get pods -n "$AAT_NS" -l app=aat-manual-netcheck -o jsonpath='{range .items[*]}{.metadata.name}{" "}{end}')
AAT_TARGETS=$(sudo k3s kubectl get pods -n "$AAT_NS" -l app=aat-manual-netcheck -o jsonpath='{range .items[*]}{.status.podIP}={.spec.nodeName}{" "}{end}')
for AAT_POD in $AAT_PODS; do
  if sudo k3s kubectl exec -n "$AAT_NS" "$AAT_POD" -- sh -ec '
    nslookup kubernetes.default.svc.cluster.local >/dev/null
    for target in "$@"; do
      address=${target%%=*}
      expected=${target#*=}
      actual=$(wget -q -T 5 -O - "http://$address:8080/")
      test "$actual" = "$expected"
    done
    actual=$(wget -q -T 5 -O - http://aat-manual-netcheck.sistem-informasi-kebencanaan-bnpb.svc.cluster.local:8080/)
    case "$actual" in
      aat-fourhost-a|aat-fourhost-b|aat-fourhost-c|aat-fourhost-d) ;;
      *) exit 1 ;;
    esac
  ' sh $AAT_TARGETS; then
    printf '%s: PASS (pod peers, DNS, Service)\n' "$AAT_POD"
  else
    printf '%s: FAIL; inspect before continuing\n' "$AAT_POD"
    break
  fi
done
```

Require **four PASS lines**, not merely successful command termination: the
interactive loop intentionally stops on the first failure without exiting the
operator's shell. This is 16 HTTP pod-to-pod identity checks plus DNS/Service
checks from each pod. It checks Service reachability, not distribution to every
backend through that Service, load capacity, all protocols or failover.
If policies already restrict this namespace, allow the probe's TCP 8080 peer
traffic and UDP/TCP 53 to CoreDNS without disabling the application policies.

Clean up only the two resources defined by the copied manifest:

```bash
sudo k3s kubectl delete -f ~/aat-manual-network-probe.yaml
```

## Verify control-plane continuity with A paused

After baseline/network checks pass, keep B and C running. From B, confirm all
four nodes are Ready, then use VirtualBox on A's physical host to **Pause A**.
With A paused, run on B:

```bash
sudo k3s kubectl get --raw=/readyz --request-timeout=20s
AAT_CHECK_NAME="manual-api-check-$(date +%s)"
sudo k3s kubectl create configmap "$AAT_CHECK_NAME" -n sistem-informasi-kebencanaan-bnpb --from-literal=checked-from=B --request-timeout=20s
sudo k3s kubectl get configmap "$AAT_CHECK_NAME" -n sistem-informasi-kebencanaan-bnpb --request-timeout=20s
```

Resume A in VirtualBox immediately after the check, including on any failure.
Then verify recovery and remove the probe ConfigMap on B:

```bash
sudo k3s kubectl wait --for=condition=Ready nodes --all --timeout=300s
sudo k3s kubectl get nodes -o wide
sudo k3s kubectl delete configmap "$AAT_CHECK_NAME" -n sistem-informasi-kebencanaan-bnpb --ignore-not-found
```

Success demonstrates a control-plane read/write during this A pause. It does
not establish full application availability, a maximum outage bound, or a
redundant external API endpoint. No application workload has been deployed yet.

## Fixed API address is optional for this manual phase

SSH into B or C and use local `sudo k3s kubectl` to administer without A. For
one external kubeconfig address with automatic failover, later provide a
redundant load-balancer/VIP endpoint and add it to every server's TLS SANs. A
single proxy on A would still depend on A. A VIP over bridged Wi-Fi needs
actual network verification; the VPS is not required as a cluster member.
See [K3s API load-balancer setup](https://docs.k3s.io/datastore/cluster-loadbalancer).
