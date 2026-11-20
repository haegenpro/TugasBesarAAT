# Source schema validation

The Aggregator reads the writer schema identified by each Avro frame. A cached,
immutable plan validates application rules and normalizes values after the Avro
codec decodes their types. It then resolves against an explicitly configured
reader schema. There is no `confidence_level` branch in the Aggregator.

The frame's writer ID never selects a reader version. `BMKG_SEISMIC_READER_SCHEMA`,
`BMKG_TSUNAMI_READER_SCHEMA`, and `PVMBG_READER_SCHEMA` select startup reader files.
The default PVMBG reader is v1. Successful live polling discovers a second distinct,
bidirectionally compatible writer schema as a cached candidate without activating
it; no bundled v2 reader is needed. History uses a reader derived from that same
source record schema and does not discover candidates from page envelope IDs.

The internal Aggregator's `POST /admin/schema-version`, with a dedicated Bearer
`AGGREGATOR_ADMIN_TOKEN`, independently toggles its reader v1/v2. It returns 409 if
no second schema has been discovered, 401 for invalid credentials, or 503 if
in-flight work cannot drain within 10 seconds; errors preserve the active reader.
The gate spans live/history fetch, mapping and database commit. Both reader
snapshots stay cached for repeated toggles, regardless of PVMBG's current writer.
This demo control keeps only two reader snapshots; further distinct schemas do not
replace them. Restart restores the configured baseline and clears discovery.
Switching does not reset cursors or backfill dropped fields. Compare newly generated
PVMBG records before and after reader activation to demonstrate JSONB storage.

**Unknown-field policy:** drop fields absent from the reader. Fields accepted by
both schemas and without a canonical destination go to `attributes`. Reader-only
fields require defaults; defaults are applied and validated but excluded from
source attributes because they were not sent by the writer. Presence tracking is
recursive through records, arrays, and maps, including history payloads. Non-null
defaults do not fabricate source fields. The rule is symmetric when a field exists
in v1 but not v2, and independent of the field's name.

Resolution supports record names/reader aliases, field names/reader aliases,
arrays, maps, unions and primitive promotions. The reader codec applies defaults
and enforces its types; its rules validate the resolved datum. Incompatible changes
fail the batch instead of advancing polling or history progress. All writer rules
run before projection, even for writer-only fields.

A PVMBG v2 field declares its rule alongside its type:

```json
{"name":"confidence_level","type":["null","double"],"default":null,
 "x-validation":{"minimum":0,"maximum":1}}
```

`"ABC"` fails the Avro double type check; `1.5` fails the custom maximum rule.
`0`, `1`, and null are valid. An omitted field needs an Avro default in the
writer schema. The Client API's arbitrary JSON `attributes` object does not
independently interpret source schemas; enforcement occurs before canonical writes.

## Rules and types

| Rule | Supported values | Meaning |
| --- | --- | --- |
| `minimum`, `maximum` | Avro int, long, float, double | Inclusive bounds; both apply when present |
| `allowedValues` | Numeric, string, boolean, enum scalars | Nonempty typed allowlist; all declared rules apply together |
| Optional null | Nullable union | Null is accepted by the schema and skips non-null constraints |

For example, `{"allowedValues":[0.25,0.5,0.75],"minimum":0,"maximum":1}`
restricts a double to those three values. A string rule can use
`{"allowedValues":["preliminary","confirmed"]}`; a boolean rule can use
`{"allowedValues":[true]}`. No string-to-number conversion occurs. Float
members and bounds use the field's declared precision; membership uses exact
comparison at that precision. Long allowlists and bounds preserve integers above
2^53. Nonfinite floating values are rejected because canonical JSON cannot encode them.

Rules can be declared on record fields or type objects. To restrict each array
item, put the rule on `items`, not on the array:

```json
{"type":"array","items":{"type":"double",
 "x-validation":{"allowedValues":[0.25,0.5,0.75]}}}
```

Records, arrays, maps, nullable/mixed scalar unions, and named/recursive records
are traversed according to their schemas. Avro itself enforces enum symbols,
fixed sizes, primitive types, and union branches. Rules on containers are rejected
when incompatible with their type. Allowlists describe scalar alternatives;
whole-array membership, object equality, array lengths, patterns, and arbitrary
cross-field constraints are not implemented. Unknown rule keys, explicit null
rule objects, incompatible types, nonnumeric bounds, inverted bounds, empty
allowlists, and unrepresentable floating bounds fail schema compilation.

Standard Avro does not interpret `x-validation`. The registry's FULL_TRANSITIVE
check covers Avro schema compatibility; semantic rule changes still require
application review. Registration and fresh lookup tests verify that this registry
preserves custom metadata, including a metadata-only schema revision.

## Normalization

Union wrappers are unwrapped only at union nodes. Real maps retain one-key entries
such as `{"double":"ABC"}`, as well as null-valued entries. Optional null record
fields are omitted to preserve existing attributes behavior. Arrays keep null items.
Int/long values retain their integer representation, including history cursors.

The current goavro logical representations normalize explicitly: date becomes
`YYYY-MM-DD`, timestamps become UTC RFC3339Nano strings, time values become
integer milliseconds/microseconds, and decimals become exact scaled strings.
Bytes/fixed become JSON base64 through Go's byte-slice JSON encoding. Unknown
logical annotations follow goavro's underlying Avro type behavior, without added
semantic guarantees. Custom constraints on logical types are rejected explicitly;
this is not a general logical-type validation framework. Schema nesting is capped
at 128 and decoded value nesting at 256 to bound recursive processing.

## History and failure policy

The deployed Aggregator requires `application/avro` for live and history responses.
Registry-less JSON clients remain available for compatibility tests; they do not
provide writer-schema constraint enforcement and are not the deployed configuration.

History uses a registered page record with `data: array<{cursor:long,payload:record}>`,
`next_cursor:long`, and `has_more:boolean`. Payload definitions come from the live
source schemas, including custom rules. SQLite archives remain JSON and require
no migration. PVMBG history reflects the currently visible schema version; the
page schema ID describes that rendered response, not the archive's original version.
The schema toggle pre-registers live/history v1/v2 before changing visibility
under the schema lock. It emits a new report when enabled and never revises existing IDs. Registration failure leaves visibility
unchanged. Schema-registry requests on history carry the request correlation ID.

An invalid live batch is rejected before any canonical write or last-success
advance. An invalid Avro history page is rejected before replay commits any item
from that page. Earlier committed pages remain; the durable cursor never skips the
invalid page. Existing retries and backoff retry from that cursor. Contract mapping
errors after page decoding retain the existing per-record commit/cursor behavior.
Errors include source, field path, and violated rule, such as
`pvmbg history: $.data[0].payload.confidence_level: maximum rejects 1.5`.
No record is silently quarantined or skipped. Known schemas remain usable during a
registry outage; an unknown schema cannot be accepted until lookup succeeds.

## Verification

Run `go test -race -count=1 ./...` and `go vet ./...` in `aggregator`, `bmkg-mock`,
`pvmbg-mock`, and `client-api`. Set `TEST_SCHEMA_REGISTRY_URL` to the registry's
`/apis/ccompat/v7` URL for source v1/v2/history and metadata retention integration
tests. Set `TEST_DATABASE_URL` to a **scratch database** initialized with
`store/init.sql`; store tests modify poll state and must not target a running
performance database. Regression tests cover live/history validation failures
before commits, bounds, scalar allowlists, nested maps/arrays, named recursive
unions, logical values, precision, registry caching, and toggle registration failure.

Reader regression tests cover the four writer/reader combinations in both
field-addition and field-removal directions, arbitrary field names, non-null
defaults, recursive presence, alias/promotion resolution, and matching live/history
`attributes` projections. See `internal/avrowire/reader_test.go` and
`internal/clients/reader_test.go` in the Aggregator module.
