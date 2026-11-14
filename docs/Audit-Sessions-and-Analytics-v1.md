# Audit sessions and analytics interfaces v1

This additive contract preserves the canonical object, existing source/type
enums and prescribed live audit responses. It implements the existing explicit
session design and adds aggregate-only access through Client API.

## Analytics

Opt-in `GET /analytics` serves local browser assets from Client API. Existing
login/refresh/logout and `hazards:summary` or `hazards:raw` scopes authorize
`GET /analytics/summary`. The proxy uses a distinct `ANALYTICS_SERVICE_TOKEN`
for `GET /internal/analytics/summary`; it never sends that token to the browser.
No record IDs, coordinates or raw attributes are in the aggregate response.

Filters are UTC `from`, `to`, `interval=hour|day`, optional canonical `source`
and optional ready `snapshot_id`. Defaults are the preceding seven days ending
at request time with daily buckets. Population is active canonical records with
`from <= occurred_at < to`; bucket origins are UTC hours/days. Maximum range is
366 days and maximum bucket count is 1,000. Unknown/repeated/invalid filters are
400. Snapshot source filtering cannot broaden its captured scope.

The response contains `filters`, `total`, `by_type`, `by_severity`, `by_area`,
`series`, `tsunami_warning_overlay`, `as_of`, `mode`, `generation`,
`data_complete`, `partial`, `unavailable_units`, and `source_freshness`.
`series` entries contain `at`, four canonical `counts` and
`tsunami_warning_overlay`; area entries contain `area_name` and `count`.
Totals and panels are exact for the completed logical units in the response.
Missing logical units set `data_complete:false`; replica availability loss also
sets `partial:true` even when logical data is complete. No replica is counted
twice. An uncertain control/visibility state returns structured 503.

The tsunami overlay counts BMKG SEISMIC records having `potential_tsunami:true`,
a nonempty string `warning_id`, and `related_event_id == source_ref_id`.
Each canonical record counts once, regardless of warning history. The overlay
overlaps earthquake totals, is not added to total records and does not assert
that a tsunami occurred. It creates no TSUNAMI canonical enum.

All live panels share one authoritative generation validation cycle, not a
retained distributed snapshot. Separate live requests can differ. Snapshot mode
uses the published cut; freshness metadata remains current operational metadata.
New-source freshness without an agreed policy remains unavailable.

Default budgets: two seconds per aggregate, one concurrent request per
Aggregator, separate query pools capped at two connections per configured
database, 4 MiB SQL work memory, no parallel gather workers, 5,000 area groups,
and a 2 MiB response. Concurrency exhaustion is 429; grouping/memory/response
quota exhaustion is explicit 503. There is no silent truncation or derived
counter pipeline. Browser credentials stay in memory and logout hides results.
The [synthetic logical fixture](fixtures/audit-analytics-v1.json) fixes counts,
the exclusive upper-bound example and the independent audit checksum.

## Explicit sessions

Audit bearer authentication remains unchanged. Optional routes are:

- `POST /internal/audit/snapshots` with `{ "source": null }` or a canonical source;
  returns 202 only after a durable building descriptor and real pinned views.
- `GET /internal/audit/snapshots/{snapshot_id}` returns status, source, captured
  time, idle/absolute expiry and a reason when present.
- Events/checksum accept `snapshot_id`; `X-Audit-Snapshot-ID` echoes it.

Existing live requests do not start captures. `/location` remains current and
does not accept snapshot selection. Building data returns 503
`snapshot_not_ready`; aborted/expired reads return 400 `cursor_expired`.
Unknown established identities return 404; control uncertainty returns 503.
Cursors bind endpoint, scope, original filters, precise ordering position,
required units and snapshot identity. A live cursor cannot select a snapshot.

Capture drains shared canonical admission and distributed decisions, pins an
actual repeatable-read view on each authoritative primary, then releases
admission. Record copying and destination digest verification use separately
checkpointed pages of at most 100 records/1 MiB; a larger individual record
aborts explicitly. Frozen rows/markers follow source-pair Citus placement.
Coordinator records contain manifests and progress only.

Publication verifies complete destination counts/content digests, marker values,
stable ownership and retained views under the existing fenced commit boundary.
Metadata becomes ready only after the completeness/durability proof. A lost view
aborts; restart never fills an old cut with newer rows. Published replica reads
require trusted group identity and the matching immutable completion marker.
Metadata renewal/read guards require the coordinator group's durable authority,
without requiring unrelated worker write quorums. This provider extension still
requires replicated runtime validation.

Budgets retain the agreed two-second boundary/read/step deadlines, one concurrent
capture, five-minute copy cap, five-second owner renewals, 15-second owner/view
idle expiry, 15-minute sliding published retention and one-hour absolute cap.
Only authorized filter-valid use renews retention. Read guards exclude GC until
COMMIT; stale cleanup cannot affect a successor. Cleanup removes exact terminal
capture IDs in bounded idempotent pages.

Missing logical units pause ordered listing with an empty partial page and the
same continuation position. Partial checksum/count cover completed reachable
streams and are explicitly incomplete; a midstream uncertainty returns 503
instead of a misleading digest. Empty complete input hashes the empty bytes.
Live-feed, evaluator, replicated failover and four-node evidence remain separate.
