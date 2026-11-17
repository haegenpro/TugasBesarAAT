# Multi-entity canonical integration

New production multi-record batches use the replicated durability provider's
`PrepareBatch` capability. The old `MultiEntityValidated` compatibility field is
retained only for existing single-copy test fixtures; setting it cannot bypass a
provider that rejects its live capability check. There is no operator-facing
"tests passed" or "enable unsafe writes" environment switch.

For the distributed formation, every batch checks the actual coordinator
transaction and all configured primary endpoints for the pinned Citus 14.2
extension family, enabled prepared transactions and active 2PC recovery. The
pinned image's extension version is `14.2-1`, distinct from release `14.2.0`.
`pg_settings.setting` supplies normalized milliseconds for recovery checks;
formatted settings such as `1s` are not cast directly to integers.

The existing mutation admission, owner checks at COMMIT, whole-batch transaction,
distributed-decision recovery, immutable batch identities and all-group WAL
confirmation remain mandatory. The one-group PostgreSQL development formation
uses one ordinary transaction with its existing synchronous-replication proof.
Unavailable or mismatched participants leave the same logical batch pending.

## Focused checks executed

`python3 scripts/test-platform-integration.py --citus` creates uniquely labelled
temporary Citus coordinator/workers and a Go runner with source mounted read-only.
It binds databases only on loopback and removes only its own containers/network.
It never uses application databases or starts VMs.

Run `2e7581e9726c`: 12 tests passed, no failures or skips. Coverage includes the
live pinned protocol configuration, two-worker atomic batch application,
unchanged-identity duplicate retry, rollback of the whole batch on a conflicting
revision, lost COMMIT reply recovery, and replication-evidence rejection cases.
The run also checked the normalized observed practice-feed payload. Raw evidence
is under ignored `evidence/platform-integration/<run-id>/`.

Earlier failed runs are retained: the first version check rejected the extension's
`14.2-1` notation. Those failures are not counted as successful validation.

The temporary database cluster is single-copy. These checks do not establish
three-copy WAL durability, physical node/disk replacement, pause/skew recovery,
10,000-VU throughput or live-feed completeness. Run those on the actual formation.

Protocol reference: [Citus distributed transactions and commit decisions](https://learn.microsoft.com/en-us/postgresql/citus/api-udf?view=citus-14).
