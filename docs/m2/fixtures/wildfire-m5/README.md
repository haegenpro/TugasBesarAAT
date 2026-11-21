# Synthetic wildfire reference fixtures

These inputs test lexical identity, connected components, late changes and stable fire lifecycle behavior. They are synthetic and do not replace the course reference or the live satellite protocol.

From `aggregator/`:

```bash
go run ./cmd/wildfire-reference replay --config ../docs/m2/fixtures/wildfire-m5/config.json --input ../docs/m2/fixtures/wildfire-m5/bridge.jsonl
```

The reference model keeps fixture history in memory. Production workers use the database journal and load affected components only. The broader historical-restoration fixtures exercise the reference algorithm; production restoration considers immediate predecessors.
