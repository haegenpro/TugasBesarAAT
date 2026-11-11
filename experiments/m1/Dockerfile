# Development experiment only. This is not a BNPB deployment image.
FROM citusdata/citus@sha256:d107b77c5d07ed435b72da9c57f6e96494912a52dc84c6a4113de8eb920e7261
USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-venv \
    && python3 -m venv /opt/patroni && rm -rf /var/lib/apt/lists/*
COPY requirements.lock /tmp/requirements.lock
RUN /opt/patroni/bin/pip install --no-cache-dir -r /tmp/requirements.lock \
    && /opt/patroni/bin/pip check \
    && /opt/patroni/bin/pip freeze > /opt/patroni/resolved-requirements.txt
COPY entrypoint.py /opt/m1/entrypoint.py
USER postgres
ENTRYPOINT ["/opt/patroni/bin/python", "/opt/m1/entrypoint.py"]
