# Port configuration

Root `.env` controls published ports and internal listener ports. Compose constructs matching upstream URLs, probes, Prometheus scrape targets and Tempo endpoints. Per-service `.env` files hold credentials and service-specific settings. Use `python3 scripts/setup-env.py` to generate a coherent set.

Default published ports are Client API 18080, BMKG 18081, PVMBG 18082 and Grafana 13000. All PostgreSQL instances use POSTGRES_PORT on separate container addresses; they do not share databases or host ports. Registry's database uses REGISTRY_DB_PORT. Tempo uses TEMPO_PORT, OTLP_HTTP_PORT and OTLP_GRPC_PORT internally.

Run `docker compose --profile monitoring --profile consumer-demo --profile tools config --quiet` after changing configuration. Never print the full expanded configuration into evidence, since it contains credentials.
