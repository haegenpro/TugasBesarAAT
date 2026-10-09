# Schema Registry

Apicurio Registry 3.3.3 uses its own `registry-db` PostgreSQL 16 container and
persistent `registry-data` volume. Its API is internal to the backend network;
neither API nor management port is published to the host. Compose generates
`SCHEMA_REGISTRY_URL` for each caller from root `SCHEMA_REGISTRY_PORT`.
`SCHEMA_REGISTRY_MANAGEMENT_PORT` controls the readiness listener.

The Registry trusts services attached to the backend network. Downstream clients
use Client API on the separate client network. Docker/host administrators can
inspect the Registry from its container:

```bash
docker compose exec -T schema-registry sh -c \
  'curl --fail --silent "http://127.0.0.1:${QUARKUS_HTTP_PORT:?}/apis/ccompat/v7/subjects"'
```

Removing the host mapping does not change source registration, live schema
evolution, consumer decoding, or monitoring; they use internal Docker addresses.

Copy the registry block from the root `.env.example` to `registry/.env` before
running Compose. Each producer registers its schema on first Avro use. The
PVMBG admin toggle registers v2 on first activation.

## Independent startup

`docker compose up -d --build schema-registry` starts only the Registry.
There is no `depends_on` gate. Its supervisor validates the explicit SQL
configuration, launches Apicurio, and relaunches it after an exit, waiting
the required `REGISTRY_RETRY_DELAY_SEC` between attempts (example `5`).
The supervisor forwards stop signals to its child and stops without retrying.

When `registry-db` is absent, the container stays running while initializing
or retrying. Its schema API and management readiness endpoint can be
unreachable; the Compose readiness health check is not expected to pass.
Once the database appears, initialization completes automatically. Storage
remains PostgreSQL; no temporary or in-memory registry is substituted.

Apicurio also has its own finite startup connection retries, documented in
the [upstream configuration reference](https://www.apicur.io/registry/docs/apicurio-registry/3.3.x/getting-started/assembly-config-reference.html).
The supervisor allows recovery after those attempts are exhausted.
