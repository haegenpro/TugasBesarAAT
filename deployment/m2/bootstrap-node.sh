#!/usr/bin/env bash
# Run on the intended Ubuntu VM as root after establishing its WireGuard tunnel.
# This prepares a node; it never wipes existing application or Kubernetes data.
set -euo pipefail
role="${1:?Usage: sudo -E bash bootstrap-node.sh a|b}"
[[ "$role" == a || "$role" == b ]] || { echo 'Node role must be a or b' >&2; exit 2; }
[[ "$(id -u)" == 0 ]] || { echo 'Run on the Ubuntu VM as root' >&2; exit 2; }
: "${M2_NODE_NAME:?Set the Kubernetes node name}"
: "${M2_NODE_IP:?Set this VM WireGuard/LAN IPv4 address}"
: "${M2_K3S_VERSION:?Set an explicit K3s release such as v1.35.0+k3s1}"
[[ "$M2_NODE_NAME" =~ ^[a-z0-9][a-z0-9.-]*$ ]] || exit 2
[[ "$M2_NODE_IP" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || exit 2
vpn_interface="${M2_VPN_IFACE:-wg0}"
ip -4 address show dev "$vpn_interface" | grep -F "$M2_NODE_IP/" >/dev/null || {
  echo 'Expected node address is not configured on the VPN/LAN interface' >&2; exit 2;
}
storage_root=/var/lib/aat-m2
[[ ! -L "$storage_root" ]] || { echo 'Application storage root cannot be a symlink' >&2; exit 2; }
if [[ -e /etc/systemd/system/k3s.service && "$role" == b ]] || \
   [[ -e /etc/systemd/system/k3s-agent.service && "$role" == a ]]; then
  echo 'Existing K3s service has a different node role; refusing to overwrite it' >&2; exit 2
fi
install -d -m 0755 "$storage_root"
install -d -m 0700 /etc/aat-m2
token_file="${M2_JOIN_TOKEN_FILE:-/etc/aat-m2/k3s-token}"
if [[ "$role" == b && ! -s "$token_file" ]]; then
  echo 'Copy A join token into the private token file before joining B' >&2; exit 2
fi
if [[ "$role" == a && ! -s "$token_file" ]]; then
  umask 077
  openssl rand -hex 32 > "$token_file"
fi
chmod 0600 "$token_file"
installer="$(mktemp /tmp/aat-m2-k3s.XXXXXX)"
trap 'rm -f "$installer"' EXIT
curl --fail --silent --show-error --location https://get.k3s.io --output "$installer"
export INSTALL_K3S_VERSION="$M2_K3S_VERSION"
export K3S_TOKEN_FILE="$token_file"
if [[ "$role" == a ]]; then
  sh "$installer" server --node-name "$M2_NODE_NAME" --node-ip "$M2_NODE_IP" \
    --advertise-address "$M2_NODE_IP" --flannel-iface "$vpn_interface" --tls-san "$M2_NODE_IP" \
    --write-kubeconfig-mode 600 --disable traefik --disable servicelb \
    --node-external-ip "$M2_NODE_IP" \
    --cluster-cidr "${M2_POD_CIDR:-10.42.0.0/16}" --service-cidr "${M2_SERVICE_CIDR:-10.43.0.0/16}"
else
  : "${M2_SERVER_URL:?Set https://A-VPN-IP:6443}"
  export K3S_URL="$M2_SERVER_URL"
  sh "$installer" agent --node-name "$M2_NODE_NAME" --node-ip "$M2_NODE_IP" --flannel-iface "$vpn_interface"
fi
echo 'Node service installed. Validate both Ready nodes from A before deploying the application.'
echo 'Join token and kubeconfig remain private; this script does not print them.'
