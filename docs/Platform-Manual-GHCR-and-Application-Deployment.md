# GHCR preparation and application deployment after manual K3s

Current target: four existing Ubuntu Server VMs in VirtualBox over Wi-Fi, one
disk per VM, K3s servers A/B/C and agent D. Kubernetes bootstrap is complete by
operator report; the BNPB application is not deployed. Use private GHCR packages
for this walkthrough. Publishing images does not deploy them.

**Execution boundary:** steps 1–6 below can use the existing repository tools.
Application release/apply is not yet integrated with the manual provider. Step 7
identifies the code work required, and step 8 records the deployment order once
that integration exists. Do not run the legacy workflow's `cluster` or `all`
phase against these VMs, or substitute fictional libvirt/Windows host data to
make inventory validation pass.

## 1. Build on the physical Mint computer

Keep one source checkout/build machine. Nodes pull images from GHCR; they do
not need Docker Engine, the Go compiler or separate repository builds.
In the repository root on **physical Mint**, check:

```bash
docker version
python3 --version
git status --short
df -h .
docker system df
```

Docker Engine must be accessible to this user. These checks do not install or
delete anything. Image builds need additional physical-host disk/RAM; dynamic
VDI maxima are not free host space. Do not prune existing Docker/VM state as an
implicit part of setup.

Use this same Mint terminal through the image steps. Replace the username:

```bash
export AAT_GH_USER='YOUR_GITHUB_USERNAME'
export AAT_DEPLOY_DIR="$HOME/aat-deploy"
mkdir -p "$AAT_DEPLOY_DIR"
chmod 700 "$AAT_DEPLOY_DIR"
```

The generated prefix is `ghcr.io/<lowercase-username>/aat`. This guide uses a
personal account; an organization requires the corresponding package-publish
permissions and any required SSO authorization.

## 2. Authenticate to GHCR

Create a **personal access token (classic)** with `write:packages` permission
for the builder. A separate `read:packages` token will be used by the nodes.
Keep both private. The packages are created by the first successful push;
there is no registry server to install on a VM. Keep their visibility private.

```bash
docker login ghcr.io --username "$AAT_GH_USER"
```

At Docker's password prompt, paste the write-capable token, not the GitHub
account password. This avoids putting the token in the command history.
Source: [GitHub Container registry authentication](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry).

## 3. Resolve the two externally supplied monitoring images

The release script builds 16 images, but requires actual digest references for
Tempo and node-exporter. Reuse the repository's Tempo 2.9.0 and explicitly select
node-exporter v1.12.1, verified in Quay's tag API on 19 November 2026. The
previously suggested v1.13.0 returned `not found` when pulled from Quay:

```bash
docker pull --platform linux/amd64 grafana/tempo:2.9.0
docker pull --platform linux/amd64 quay.io/prometheus/node-exporter:v1.12.1
```

After both pulls succeed, generate the image configuration using real inspected
digests rather than placeholder hashes:

```bash
python3 - <<'PY'
import json
import os
from pathlib import Path
import subprocess

def digest(image):
    refs = json.loads(subprocess.check_output(
        ['docker', 'image', 'inspect', image, '--format', '{{json .RepoDigests}}'],
        text=True))
    if not refs or '@sha256:' not in refs[0]:
        raise SystemExit('No registry digest found for ' + image)
    return refs[0]

config = {
    'version': 1,
    'registry': 'ghcr.io/' + os.environ['AAT_GH_USER'].lower() + '/aat',
    'platform': 'linux/amd64',
    'external_images': {
        'tempo': digest('grafana/tempo:2.9.0'),
        'node-exporter': digest('quay.io/prometheus/node-exporter:v1.12.1'),
    },
}
path = Path(os.environ['AAT_DEPLOY_DIR']) / 'images-release.json'
path.write_text(json.dumps(config, indent=2) + '\n')
path.chmod(0o600)
print('Image configuration written to', path)
PY
```

This selects versions but does not establish monitoring compatibility on the
physical cluster. Source: [Quay node-exporter v1.12.1 tag metadata](https://quay.io/api/v1/repository/prometheus/node-exporter/tag/?specificTag=v1.12.1).

## 4. Review, build and publish

Review the build plan first:

```bash
python3 scripts/platform-images.py plan \
  --config "$AAT_DEPLOY_DIR/images-release.json"
```

Then publish into a new local evidence directory:

```bash
export AAT_IMAGE_RUN="$AAT_DEPLOY_DIR/images-$(date +%Y%m%d-%H%M%S)"
python3 scripts/platform-images.py publish \
  --config "$AAT_DEPLOY_DIR/images-release.json" \
  --output "$AAT_IMAGE_RUN"
```

The 16 builds cover broker, mocks, Auth, Client API, consumers, platform metrics,
Schema Registry, the Patroni/storage image, the combined Aggregator/ingestion
image (`role1`), and five monitoring wrappers. Tempo and node-exporter remain
the two supplied upstream references; they are not copied to GHCR by this script.

The script stages filtered **tracked source files only**, captures the source
revision/content hash, builds `linux/amd64`, pushes, and checks registry-confirmed
digests. Local untracked/private files do not enter those contexts. Successful
output contains `images.json`, `result.json` and per-image logs. A partial image
map is not a complete release; require `result.json` to report `passed: true`.
Failed output is retained and cannot be overwritten by a rerun. Inspect the
specific failure before selecting a fresh output directory.

Print the actual storage reference for the node pull check:

```bash
python3 - <<'PY'
import json, os
from pathlib import Path
run = Path(os.environ['AAT_IMAGE_RUN'])
assert json.loads((run / 'result.json').read_text())['passed'] is True
print(json.loads((run / 'images.json').read_text())['storage'])
PY
```

## 5. Give all four K3s nodes GHCR pull access

Docker login on Mint does not authenticate the guests' containerd runtimes.
For these existing renderers, node-level registry configuration also covers the
custom monitoring ServiceAccount; patching only the default ServiceAccount
would not cover every workload.

Inside **each Ubuntu VM**, configure a read-only GHCR token:

```bash
sudo mkdir -p /etc/rancher/k3s
sudo touch /etc/rancher/k3s/registries.yaml
sudo chmod 600 /etc/rancher/k3s/registries.yaml
sudo nano /etc/rancher/k3s/registries.yaml
```

Add this configuration, replacing the username/token in the editor. Preserve
any existing unrelated registry entries:

```yaml
configs:
  ghcr.io:
    auth:
      username: YOUR_GITHUB_USERNAME
      password: YOUR_READ_PACKAGES_TOKEN
```

Use the GitHub username owning the token, and a token with `read:packages` and
permission to read the packages. Do not paste the real file into chat or Git.

Restart **one VM's K3s service at a time**. On A, then B, then C:

```bash
sudo systemctl restart k3s
sudo k3s kubectl get --raw=/readyz
sudo k3s kubectl get nodes -o wide
```

Wait for the restarted server's API to respond `ok` and all three servers to
be Ready before restarting another. On D:

```bash
sudo systemctl restart k3s-agent
```

Verify all four Ready from B or C. Source:
[K3s private registry configuration](https://docs.k3s.io/installation/private-registry).

## 6. Verify a private pull on every node

Inside A/B/C/D, replace the placeholder with the **actual storage reference**
printed in step 4:

```bash
sudo k3s crictl pull 'PASTE_ACTUAL_STORAGE_IMAGE_REFERENCE_HERE'
```

Require success on all four. This verifies private-image access; it does not
start a database or prove the application is healthy. Keep the successful
`images.json` path for release preparation.

## 7. Required implementation before live application apply

This work remains; the current scripts do not provide an end-to-end command for
these manually created guests:

- **Existing-cluster inventory/client:** accept the actual node addresses,
  physical-host labels and allocations, and use an existing admin kubeconfig or
  authenticated guest access. Existing v1/v2 validation and `Cluster(...)`
  select libvirt/multi-host ownership workflows, not this manual topology.
- **Guest management:** prepare authenticated, noninteractive administration
  without exposing/reusing personal secrets. Current helpers call `sudo -n`;
  a normal password-based SSH login alone does not satisfy that contract.
- **Single-disk storage:** update `platform-storage.py prepare-disks` and
  `platform-deploy.py prepare-volumes` to validate owned directories on the root
  filesystem instead of requiring `/var/lib/aat` to be a separate mount. Keep
  ownership and existing-data checks; do not bypass them with fake markers.
- **Capacity/apply/maintenance:** use actual Kubernetes capacity and this
  existing-cluster client without invoking the wired physical-host doctor or
  provisioning/replacing the VMs. Retain bundle, migration and durability gates.

Image preparation above is independent of these blockers. The legacy
`platform-workflow.py run --phase all` is not a workaround; its cluster phase
tries to provision provider-owned VMs.

## 8. Application deployment order after integration

1. Capture actual node IPs, labels, allocatable resources and available guest
   disk space. The current full bundle requests about 25.25 GiB before guest
   overhead, rollout/failover headroom; four 4 GiB guests cannot schedule it.
2. Generate one private release configuration with the actual image map and
   topology. Preserve its passwords, PKIs and stable source identities across
   subsequent stages. Complete required source/Audit credentials and bounded
   external egress inputs without committing them.
3. Render the **infrastructure** release and review its plan. Prepare owned
   local directories and apply Secrets/isolation plus the six HA database
   formations: canonical Citus, ingestion, Auth, Registry, Dashboard and Notifier.
4. With application writers stopped, initialize owner schemas/Registry, capture
   original database identities, and execute canonical/ingestion migrations and
   BMKG cutover. Preserve the verified maintenance receipt.
5. Render the full release using that receipt and captured identities. Prepare
   application/monitoring volumes, then apply the reviewed stages including
   broker/Registry, ingestion, Aggregator, Auth, API, consumers and monitoring.
   Dependencies and initialization jobs may cross these broad categories; use
   the actual bundle stage order rather than a handwritten directory-wide apply.
6. Configure client access, verify authenticated ingestion/read/report flows,
   then run source correctness, failure/recovery and load acceptance.

These stages follow [integrated release preparation](../deployment/platform/RELEASE.md)
and [the input-driven workflow](Platform-Input-Driven-Workflow.md), whose provider
restrictions still apply until step 7 is implemented. No database/apply command
in this document is claimed to work around that missing integration.
