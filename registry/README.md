# Schema Registry

Apicurio Registry uses its own `registry-db` Postgres container and persistent
`registry-data` volume. It is reachable by services at
`http://schema-registry:8080/apis/ccompat/v7` and from the host at
`http://localhost:8084/apis/ccompat/v7`. The registry's readiness endpoint is
on its management port, `9000/health/ready`.

Copy the registry block from the root `.env.example` to `registry/.env` before
running Compose. Each producer registers its schema on first Avro use. The
PVMBG admin toggle registers v2 on first activation.
