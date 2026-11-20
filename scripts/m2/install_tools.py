#!/usr/bin/env python3
"""Install checksum-verified Kubernetes tools into the ignored project directory."""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
import platform
import urllib.request

from common import LOCAL


def download(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=90) as response:
        return response.read()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kubectl-version', default='v1.35.0')
    parser.add_argument('--minikube-version', default='v1.38.1')
    args = parser.parse_args()
    arch = {'x86_64': 'amd64', 'aarch64': 'arm64'}.get(platform.machine())
    if platform.system() != 'Linux' or arch is None:
        parser.error('this installer supports Linux amd64/arm64; use official installers elsewhere')
    directory = LOCAL / 'bin'
    directory.mkdir(parents=True, exist_ok=True)
    releases = {
        'kubectl': f'https://dl.k8s.io/release/{args.kubectl_version}/bin/linux/{arch}/kubectl',
        'minikube': f'https://github.com/kubernetes/minikube/releases/download/{args.minikube_version}/minikube-linux-{arch}',
    }
    for name, url in releases.items():
        path = directory / name
        checksum = download(url + '.sha256').decode().split()[0]
        if path.exists() and hashlib.sha256(path.read_bytes()).hexdigest() == checksum:
            print(f'{name}: verified existing binary')
            continue
        body = download(url)
        if hashlib.sha256(body).hexdigest() != checksum:
            raise RuntimeError(f'{name}: checksum mismatch')
        temporary = path.with_suffix('.download')
        temporary.write_bytes(body)
        temporary.chmod(0o755)
        temporary.replace(path)
        print(f'{name}: installed verified binary in {directory.relative_to(directory.parents[1])}')


if __name__ == '__main__':
    main()
