# Build with the repository root as context. The adjacent Dockerfile.dockerignore
# limits that context to these sources; no build is run by the render command.
FROM citusdata/citus@sha256:d107b77c5d07ed435b72da9c57f6e96494912a52dc84c6a4113de8eb920e7261
USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-venv \
    && python3 -m venv /opt/patroni && rm -rf /var/lib/apt/lists/*
COPY experiments/m1/requirements.lock /tmp/requirements.lock
RUN /opt/patroni/bin/pip install --no-cache-dir -r /tmp/requirements.lock
COPY deployment/storage/entrypoint.py deployment/storage/bootstrap.py /opt/platform/
COPY store/migrations/0005_replication_probe.sql /opt/platform/replication-probe.sql
USER postgres
ENTRYPOINT ["/opt/patroni/bin/python", "/opt/platform/entrypoint.py"]
