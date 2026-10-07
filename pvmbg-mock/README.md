# pvmbg-mock

## Durable history

`SOURCE_ARCHIVE_PATH` selects this service's SQLite archive (required; example
`/data/history.db`). Compose mounts a separate source-owned volume. Generated
observations commit before live exposure. Archives retain history beyond the
existing 100-record live window and survive restart. Authenticated `GET /history`
uses sequence cursors and each source's existing credentials/delays; PVMBG history
also respects outage and confidence visibility. Live responses add an
`X-Source-Sequence` header without changing their required bodies. See
[Reporting Lifecycle](../docs/Reporting%20Lifecycle.md).
