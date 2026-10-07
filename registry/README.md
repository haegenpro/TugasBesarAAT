# Schema Registry

Apicurio Registry uses its own `registry-db` Postgres container and persistent
`registry-data` volume. It is reachable by services at
`http://schema-registry:8080/apis/ccompat/v7` and from the host at
`http://localhost:18084/apis/ccompat/v7`. The registry's readiness endpoint is
on its management port, `9000/health/ready`.

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
