# Consumer recovery

Consumers retain a shared halt reason for their canonical and lifecycle subscriptions in their own PostgreSQL. Unknown schema storms, incompatible registered schemas and unrecoverable local configuration/storage failures halt processing. Temporary connection/serialization/lock failures retry. Individual invalid messages use the durable DLQ policy.

Repair the underlying cause, then run the local administrative command inside the owning container:

```bash
docker compose exec consumer-notifier consumer resume --operator <name> --reason <reason>
```

The client uses `CONSUMER_ADMIN_SOCKET` (normally `/data/admin/consumer.sock`), not a database path. The socket directory is 0700 and the socket 0600. The socket is ephemeral; halt state and the most recent resume audit persist in PostgreSQL. There is no HTTP resume endpoint. Resume does not delete projections, reset deduplication, or replay previously quarantined DLQ entries. Readiness returns only after subscriptions can operate again.
