# Cross-Team Contracts: Aggregator API & Broker

*As of 2026-09-24*

## Overview

This fixes the two contracts that cross ownership boundaries under the current split (Anggota 1: Mock + Aggregator + Store, Anggota 2: Client + Auth, Anggota 3: Broker + Consumer):

1. **Aggregator → Client-API**: how Anggota 2's Client-API reads `HazardEvent` data from Anggota 1's Aggregator (per spec, Client-Facing API never touches the Canonical Store directly)
2. **Aggregator → Broker**: what Anggota 1 publishes and how Anggota 3's broker/consumers receive it

Everything else (Auth internals, Consumer internals) stays inside one person's ownership and doesn't need a cross-team contract. These two do, because code on both sides depends on the shape agreeing exactly — get this wrong and integration breaks late, not early.

## Contract 1: Aggregator read API

**Base URL:** `http://aggregator:<port>` (internal-only, not exposed outside the compose network — Client-API is the only caller)

| Endpoint | Method | Query params | Returns |
| --- | --- | --- | --- |
| `/internal/hazards` | GET | `since` (ISO 8601, optional), `hazard_type` (SEISMIC\|VOLCANIC, optional), `severity` (optional), `area_name` (optional, substring match) | List of `HazardEvent` (full Mentah+Ringkasan shape — scope filtering happens in Client-API, not here) |
| `/internal/hazards/{hazard_id}` | GET | — | Single `HazardEvent` or 404 |
| `/health` | GET | — | `{ "status": "ok", "service": "aggregator" }` |

**Response envelope for the list endpoint:**

```json
{
  "data": [ /* HazardEvent[] per the wire format spec */ ],
  "count": 42,
  "as_of": "2026-09-24T10:15:00.000Z",
  "source_status": {"PVMBG": {"state": "fresh"}}
}
```

`as_of` is the timestamp the Aggregator generated this response at — Client-API uses it, not `Date.now()`/`time.Now()` on its own side, when it needs to reason about data freshness (avoids clock-skew arguments between the two services).

`source_status.PVMBG` becomes `{"state":"stale","stale_since":"<ISO8601>"}` during an outage, including when the result has no volcanic rows. Client-API forwards this metadata outside the hazard objects, so Media can see availability without seeing raw `attributes`.

Renaming or removing a required field is a breaking contract change. The
Client API rejects an Aggregator response missing a required canonical field
and returns `503`; it does not substitute a Go zero value. Likewise, the
Aggregator rejects source records missing fields its mapping requires. Extra
source fields without a canonical top-level mapping enter `attributes`, so
additive source fields do not require an Aggregator redeploy. Client API
projects Media responses from an explicit summary field list.

**Pagination:** not required for M1's data volumes (≥20 seed records per source, low event rate) — explicitly out of scope. If the list grows large during testing, add `limit`/`offset` later without breaking existing callers (additive query params only).

## Contract 1: staleness & degraded-state representation

This is the field that makes P2's outage criterion demonstrable end-to-end, so it needs to be exact, not just "some indicator."

When PVMBG is in outage and a `HazardEvent` in the response is volcanic data that hasn't been refreshed since the outage began, the Aggregator adds two fields to **that event's `attributes` map** (not top-level — keeps the core `HazardEvent` shape from the wire spec untouched):

```json
{
  "attributes": {
    "eruption_count_24h": 3,
    "ash_column_height_m": 1200,
    "stale": true,
    "stale_since": "2026-09-24T09:58:12.000Z"
  }
}
```

- `stale` (bool): only present and `true` when the underlying source is known to be unreachable and this is last-known data
- `stale_since` (ISO 8601): when the Aggregator first observed the PVMBG outage

**Client-API's job:** surface this to the end client as an explicit status (e.g. include `"data_status": "stale"` in its own response, or a `Warning` header) rather than silently passing it through inside `attributes` — that's specifically what P2's criterion 3 asks for ("disertai penanda kebaruan data ... bukan error 500").

If PVMBG is unreachable and there is **no** last-known data at all (e.g. first boot before any successful poll), the Aggregator omits that source's events entirely rather than fabricating a placeholder — Client-API should treat an empty/partial volcanic result set plus a healthy `/health` check as "no data yet," not an error.

## Contract 1: client examples

**Go (Client-API calling the Aggregator):**

```go
type HazardListResponse struct {
	Data  []HazardEvent `json:"data"`
	Count int           `json:"count"`
	AsOf  time.Time     `json:"as_of"`
}

func FetchHazards(ctx context.Context, since time.Time) (*HazardListResponse, error) {
	url := fmt.Sprintf("http://aggregator:%s/internal/hazards?since=%s",
		aggregatorPort, since.UTC().Format(time.RFC3339))
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	req.Header.Set("X-Correlation-ID", correlationIDFromCtx(ctx))
	// ... do request, decode JSON into HazardListResponse
}
```

**Python (Client-API calling the Aggregator):**

```python
async def fetch_hazards(since: datetime, correlation_id: str) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"http://aggregator:{AGGREGATOR_PORT}/internal/hazards",
            params={"since": since.astimezone(timezone.utc).isoformat()},
            headers={"X-Correlation-ID": correlation_id},
        )
        resp.raise_for_status()
        return resp.json()
```

Both examples forward the correlation ID per the logging conventions doc — this call is exactly the kind of cross-service hop that needs to show up correlated in both services' logs.

## Contract 2: broker choice & subject design

**Chosen broker: NATS with JetStream enabled.** This follows the canonical choice in `Technical Stack Specifications.md` and replaces the earlier RabbitMQ decision:

| | Kafka | RabbitMQ | NATS + JetStream |
| --- | --- | --- | --- |
| Setup complexity | High (Zookeeper/KRaft, config-heavy) | Medium | Low — single binary, minimal config |
| Client maturity | Good | Good | Good |
| Replay/at-least-once support | Yes (native) | Yes (with manual ack) | Yes (JetStream persists + replays) |
| Resource footprint | Heavy for a laptop-run demo | Medium | Light |

**Decision: NATS + JetStream.** NATS has the lowest setup and resource footprint in the comparison above, while JetStream provides the durable stream, replay, and explicit-ack behavior required for at-least-once delivery. The broker image extends official `nats:2.15.0-alpine3.22`, mounts `broker/nats.conf` read-only to enable JetStream, and initializes the stream on each start. The separate `broker-init` service remains available for manual reinitialization. The Go client is `github.com/nats-io/nats.go`.

**Subject name:** configured by `NATS_SUBJECT` (the example is `hazard.events`). Consumers filter by `hazard_type` after decoding. The stream name is configured by `NATS_STREAM`; neither has a code default.

**Stream and subject structure:** create one durable JetStream stream for the configured subject; no partitioning is required for M1's event rate.

## Contract 2: message envelope

Publish the `HazardEvent` in an Avro record wrapped in a thin envelope carrying delivery metadata. The registry subject is `<NATS_SUBJECT>-value` and uses `FULL_TRANSITIVE` compatibility. Each NATS message starts with a zero magic byte and a four-byte schema ID, followed by an Avro datum. Resolve the ID through Apicurio's Confluent-compatible API before decoding. The logical value is:

```json
{
  "envelope_version": 1,
  "published_at": "2026-09-24T10:15:03.501Z",
  "correlation_id": "a1b2c3d4-...",
  "payload": { /* HazardEvent, exact wire-format-spec shape */ }
}
```

- `envelope_version`: lets the envelope shape change later without guessing from message content — start at `1`, bump only on a breaking envelope change (not on `HazardEvent` field additions, which are already handled by the wire spec's additive-field policy)
- `published_at`: broker-level publish timestamp, distinct from the event's own `ingested_at`
- `correlation_id`: propagates the ID from whichever poll cycle produced this event, so a consumer's log line can be grepped against the Aggregator's log for the same event
- `payload`: the canonical `HazardEvent` fields; `attributes` is an Avro `map<string, bytes>` whose values are individual JSON-encoded attribute values. Consumers decode each value to recover its string, number, or boolean type. The Aggregator read API and Client API continue to use the JSON representation in the wire format spec.

## Contract 2: delivery semantics & idempotency

**Decision: at-least-once delivery**, via a durable JetStream consumer with explicit ack (`AckExplicit`) — consumers ack only after successfully processing a message; an unacked message (consumer crash, disconnect) remains in the stream and is redelivered according to the consumer's redelivery policy.

Why at-least-once over at-most-once: P5's criterion 3 explicitly requires explaining what happens to events published while a consumer was down ("tersusul atau hilang sesuai konfigurasi broker") — at-least-once with a durable JetStream stream and consumer means a reconnecting consumer catches up on missed events, which is both the safer default for a disaster-coordination system (losing a tsunami warning silently is bad) and gives a clean, demoable answer to that criterion.

**Consequence: every consumer must be idempotent.** A message can be delivered more than once (e.g. consumer acks, then crashes before committing its own side-effect, broker redelivers). Both consumers should key on `payload.hazard_id` to detect duplicates:

- **Dashboard updater:** upsert by `hazard_id` (safe by construction — applying the same update twice is a no-op)
- **Notifier:** keep a small "already notified" set/table keyed by `hazard_id` (+ maybe `envelope.published_at` if the same hazard can legitimately update twice) and skip sending a duplicate notification

This is worth stating explicitly in the report per P5's criterion 5 ("apakah consumer perlu bersifat idempoten dan bagaimana kelompok menyikapinya") — it's a direct rubric line, not just good practice.

## Contract 2: publish/consume sequence

The Aggregator serializes the envelope with its registered Avro schema and
publishes it on `NATS_SUBJECT`, using `hazard_id` as the JetStream message ID.
If the registry has never been available, the publisher keeps the JSON outbox
row pending and retries registration. Once registered, it can continue using
its cached ID and codec during a registry outage.

A consumer reads the frame's schema ID, resolves the writer schema from the
registry (or a previously cached copy), decodes the Avro record, then decodes
each `attributes` value as JSON. It deduplicates by `hazard_id` and acknowledges
only after its own processing succeeds. An unknown ID during a registry outage
must remain unacknowledged for retry.

## Open decisions to confirm as a team

- [x] Broker technology — settled: NATS + JetStream
- [ ] **Aggregator internal port** — pick a value for `<port>` in Contract 1's base URL and add it to `.env.example`
- [ ] Confirm the `stale`/`stale_since` fields inside `attributes` are acceptable to both Anggota 1 (producing them) and Anggota 2 (consuming them) — this is the one field genuinely new since the original wire format spec was written

## Checklist before integration

- [ ] Anggota 2's Client-API can call `GET /internal/hazards` against a running Aggregator and parse the `data`/`count`/`as_of` envelope
- [ ] Anggota 2's Client-API correctly surfaces `attributes.stale`/`stale_since` as an explicit client-facing status, not passed through silently
- [ ] Anggota 1's Aggregator publishes Avro to `NATS_SUBJECT` in the envelope shape above, verified by a consumer that resolves the schema ID
- [ ] Anggota 3's consumers deserialize `payload` using the same `HazardEvent` model from the wire format spec (no separate ad hoc parsing)
- [ ] Both consumers (dashboard updater, notifier) key on `hazard_id` for dedup and can be shown handling a redelivered/duplicate message without double-applying it
- [ ] Correlation ID from the envelope shows up in both the Aggregator's publish log line and the consumer's processing log line for the same event
