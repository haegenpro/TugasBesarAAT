# Reporting lifecycle and recovery contract

An observation may produce a preliminary notice while its warning assessment or volcano reference mapping is unavailable. Notices retain identity and transition history. A later assessment resolves or corrects the existing record and publishes a versioned lifecycle event. The canonical HazardEvent payload remains the M1 contract.

Source live APIs retain bounded windows. Authenticated `/history?entity=<name>&after=<sequence>&limit=<1..1000>` retrieves source-owned observations from SQLite archives that survive live eviction and process restart. Each source keeps its own credentials, delay and outage behavior; history does not bypass PVMBG outages. Archive writes precede live exposure. Avro source/history pages use the explicit reader policy documented in [Schema Validation](Schema%20Validation.md).

Aggregator stores validated observations, replay cursors, notices, revisions and pending publications in its own PostgreSQL. A replay cursor advances in the same transaction as its observation and resulting state/outbox work. Failed processing does not advance progress. Freshness expiry changes presentation but never silently resolves a hazard or stops recovery.

Consumers use separate PostgreSQL databases. Each transaction locks the affected record keys, records the message stream/sequence/digest, and applies only newer revisions. An original canonical message cannot overwrite a later correction. Broker ACK follows commit; ACK failure produces safe redelivery. Notification records are simulated and deduplicate lifecycle event IDs.

Outbox publication is at least once. Trace context is persisted in outboxes and carried in NATS headers without modifying business schemas. Replay remains limited by source retention and broker stream retention; there is no promise of unlimited recovery after manual deletion.

Failed ingestion responses are captured in Aggregator `failed_ingestions`, including original bytes, source/entity, reason, retry classification, attempts and receipt timestamps. Oversized bodies retain at most MAX_PAYLOAD_BYTES and explicitly report truncation. This is storage groundwork: automated parked-message reprocessing and M2 inspection APIs are not implemented.

See [Contracts](Contracts.md), [consumer recovery](consumer-recovery.md), and the [runbook](Test%20Runbook.md).
