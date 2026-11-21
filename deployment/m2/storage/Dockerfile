FROM citusdata/citus@sha256:d107b77c5d07ed435b72da9c57f6e96494912a52dc84c6a4113de8eb920e7261
USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-venv \
    && python3 -m venv /opt/patroni && rm -rf /var/lib/apt/lists/*
COPY deployment/m2/storage/requirements.txt /tmp/patroni-requirements.txt
RUN /opt/patroni/bin/pip install --no-cache-dir -r /tmp/patroni-requirements.txt
COPY deployment/m2/storage/entrypoint.py /opt/m2/entrypoint.py
COPY deployment/m2/storage/guardian.py /opt/m2/guardian.py
USER postgres
ENTRYPOINT ["/opt/patroni/bin/python", "/opt/m2/entrypoint.py"]
