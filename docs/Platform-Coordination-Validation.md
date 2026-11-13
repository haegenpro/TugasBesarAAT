# First coordination handoff: local validation

Validation date: 13 November 2026. This report covers the first platform
coordination implementation, not the complete Platform role or M2 acceptance.

These results cover the first handoff committed as `61807f4`. Subsequent
[frozen-worker cleanup changes](Platform-Recovery-Implementation.md) and the
[replicated storage draft](../deployment/storage/README.md) are untested under
the development-first workflow. The subsequent
[acquisition and Aggregator draft](Platform-Acquisition-Implementation.md) is
also unexecuted. The results below do not validate that later code, its new
migrations, manifests or deferred checks.

## Executed results

| Check | Result |
| --- | --- |
| Aggregator `go test -mod=readonly -count=1 ./...` | Passed; external-database tests skip without their required settings |
| Aggregator `go vet -mod=readonly ./...` | Passed |
| `python3 scripts/test-coordination.py` | Passed against an isolated real PostgreSQL instance |
| `python3 scripts/test-coordination.py --race` | Passed, including eight platform integration tests and migration regressions |
| Python runner compilation and `git diff --check` | Passed |

The ordinary regression suite needs loopback sockets for existing HTTP tests;
those tests passed with network access. No application stack or application
database was used. The runner removed its labelled containers and temporary
database storage. Docker cache cleanup was unnecessary.

The race run's local evidence is
`evidence/platform-ownership/28957f9720fe4de6be134e5838dbf664/`:

- `result.json`: `passed: true`, `race: true`, `oom_killed: false`.
- PostgreSQL runtime: `16.15` from cached `postgres:16-alpine3.24`.
- Resolved image ID:
  `sha256:0cefaabd882e42bcb2a2f3dc56d3ce4770560ba62fd7f197f1a2e008fd5731ab`.
- `tests.log`: redacted test outcomes and assertions.

Raw evidence is ignored/local and is not embedded in this report. Reproduce
using the [runner](../scripts/test-coordination.py); it records the actual
runtime/image identity rather than assuming a tag always resolves identically.

## What was demonstrated

1. Concurrent acquisition has one winner; live acquisition retries retain their
   epoch/expiry, renewals extend expiry, and release preserves epoch history.
2. A successor receives a newer epoch; the old holder cannot renew, release the
   successor, or apply new canonical work.
3. A successor can deliver the original pending batch identity. Previously
   committed batches still return their original result without another effect.
4. Source-scoped authority rejects cross-source mutations.
5. Ownership release waits while an actual mutation transaction holds the fence,
   through COMMIT. This is not a detached lease preflight.
6. An active transaction that exceeds its lease fails its deferred commit check;
   canonical rows and output intents roll back together.
7. A stalled client transaction loses its backend to the database idle timeout.
   Its resumed transaction and old token are rejected; replacement work succeeds.
8. The ownership API and canonical writes work together through the real HTTP
   adapter and with restricted runtime database grants. Runtime DDL stays denied.
9. Unknown scope, invalid holder and unavailable database fail explicitly.
   Authentication, strict JSON/body limits and client-selected expiry rejection
   are tested at the HTTP boundary.
10. Fresh/M1-upgrade/repeated offline migrations and runtime grants pass local
    regression checks with additive `0004_ownership.sql`.

## Limits and next integration

The integration uses the existing **test-only single-copy durability adapter**
to exercise the actual canonical store/HTTP code. The default production path
still lacks a verified durability adapter and returns unavailable for new
batches. The ownership feature is opt-in.

No four-node deployment, physical/VM freeze, host clock modification, synchronous
replication, distributed Citus commit recovery, upstream single acquisition,
audit barrier cleanup or 10,000-VU load was established. A stalled database
client is a useful protocol test, not a VM suspend demonstration.

The existing canonical admission protocol also holds a separate session advisory
lock on its control connection. The data-backend idle timeout alone does not
expire that separate connection. The stalled-client test resumes/cleans up the
old transaction before asserting replacement canonical writes. A permanently
frozen process still needs bounded control-session cleanup and attempt recovery;
that must be proved before claiming automatic freeze recovery or audit safety.

Next: provision the portable four-node environment, integrate/verify HA durability
with the storage owner, and connect acquisition/processor/audit authority at
their own protected boundaries. See the [deployment contract](Platform-Deployment-Contract.md)
and [acceptance matrix](Platform-Acceptance-Plan.md).
