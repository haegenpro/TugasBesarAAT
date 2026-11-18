# Static province reference

[`deployment/role1/provinces.json`](../deployment/role1/provinces.json) is the
default static reference for wildfire `area_name`. It contains all 38 rows of
the province-center table supplied by the project owner on 2026-11-18. Names and
coordinates are preserved from that table. The original columns map as follows:

| Supplied column | Runtime field |
| --- | --- |
| `province_name` | `name` |
| `latitude_center` | `latitude` |
| `longitude_center` | `longitude` |

Each row also has a stable local `id` derived from its name. These IDs are
project identifiers, not official administrative codes. The supplied table is
the selected project reference; its BNPB provenance and agreement with course
evaluation data have not been independently verified.

## Mapping

The wildfire mapper calculates an equal-weight spherical vector mean of the
included unique detections. It measures the WGS84 ellipsoidal geodesic distance
from this fire center to each province center and assigns the nearest row's
`name` to `area_name`. Exact distance ties use name, then ID, in lexical order.
The event retains the fire center as its latitude/longitude. This assigns the
nearest reference center; it does not test administrative boundary polygons.
BMKG and PVMBG retain their existing source-specific mappings.

The reference implementation is in
[`mapping.go`](../aggregator/internal/wildfire/mapping.go); the resumable runtime
uses [`Aggregate.Event`](../aggregator/internal/wildfire/aggregate.go).

## Configuration and release preparation

[`wildfire.integrated.example.json`](../deployment/role1/wildfire.integrated.example.json)
loads the adjacent `provinces.json` and pins its exact bytes with SHA-256:

```text
a82363d09ff6f8e85d48db143e4e6a99d6be4283769574fd4a802f82f647cf20
```

`.gitattributes` enforces LF for the table so the digest is identical on Windows
and Linux. Preserve the exact bytes when copying it; reformatting changes the
hash. Startup rejects a missing, invalid, empty or hash-mismatched table.

`platform-configure.py` copies this table into new private release inputs by
default and calculates the copied table's hash. `--province-reference PATH`
selects an explicit alternative. `platform-inputs.py` uses an explicit
`province_reference` input first, otherwise preserves the existing release's
table, otherwise copies the bundled table for an older scaffold without one.
It clears the old generated missing-table blocker without rotating credentials.
The release renderer embeds the chosen table and configuration in an immutable
ConfigMap mounted into the Aggregator.

When assembling a private release manually, copy both `provinces.json` and
`wildfire.integrated.example.json` (as `wildfire.json`) into its input directory.
Other source/session settings still need to match the selected feed.

The table is immutable for an established processing run. To change a table
later, update its hash and plan a compatible processing-state transition; the
processor binds its durable configuration fingerprint to the table's digest.
The synthetic `(0,0)` tables under `docs/fixtures/` remain replay-test inputs.
