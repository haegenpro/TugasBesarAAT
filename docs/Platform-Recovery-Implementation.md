# Frozen-worker cleanup implementation draft

Status: **implemented, unverified**. The user requested development first and
deferred test execution. No test suite, build, database experiment or VM exercise
was run for this change. Formatting and source review do not establish runtime
correctness. Historical ownership/bootstrap results cover their earlier commits,
not this draft.

## Problem and implemented changes

An admitted mutation uses two PostgreSQL connections: a data transaction and a
control session holding session advisory locks. Expiring only an ownership-
protected data transaction does not close the separate control session. A
permanently frozen application can therefore keep recovery from acquiring its
attempt lock even after its data backend is gone.

`Store.New` now configures application-local PostgreSQL session budgets:

| Setting | Default | Scope |
| --- | --- | --- |
| `DB_IDLE_TRANSACTION_TIMEOUT` | `15s` | Aggregator data/control connections idle inside a transaction |
| `DB_ADMISSION_IDLE_TIMEOUT` | `30s` | Control connections idle outside a transaction, including sessions holding advisory locks |

Defaults apply when the optional environment values are absent. Transaction idle
time must be greater than ten seconds and at most thirty seconds. Control idle
time must be at least five seconds longer and at most forty-five seconds. Values
must resolve to whole milliseconds; invalid values fail Store construction.
The current publisher uses a ten-second attempt budget, which the initial
transaction timeout preserves. Changing that I/O budget requires reviewing
these values together.

The timeouts are connection startup parameters, covering a freeze immediately
after BEGIN. `beginMutation` also sets a transaction-local timeout for admitted
mutations; the canonical ownership fence can subsequently shorten it. The data
pool default applies to other Aggregator transactions too, including outbox
work, so those idle windows require regression coverage before deployment.

The control pool retires idle returned connections before server expiry and
pings a candidate on acquisition. A process resuming with server-expired pool
connections should discard them instead of handing them to new callers. No
background heartbeat prolongs a frozen process's checked-out control session.

Before writing its atomic commit marker, `mutationTx.Commit` checks its own
control connection. A lost session triggers a bounded best-effort data rollback
and a retryable unavailable result. It leaves the pending attempt to the normal
recovery protocol; it does not invent a terminal outcome.

An active, unusually long transaction can also outlive the control idle budget.
Its commit then fails this check. The existing canonical API has a two-second
deadline, but bulk/replay paths and distributed recovery need separate budget
validation. These defaults are development choices, not a demonstrated takeover
SLA or production tuning result.

## Recovery invariants retained

Session expiry releases locks; it is not evidence of whether a transaction
committed. `RecoverAttempts` still needs the attempt session lock, the attempt
row lock, settled distributed decisions and the atomic commit marker before
recording `committed` or `aborted`. Unknown group/topology changes, missing workers
and prepared transactions still keep an uncertain attempt pending. The later
[replicated storage draft](../deployment/storage/README.md) adds bounded promotion
handling within configured groups and replicated publication checks; both
drafts remain unverified. Output publication remains
gated by terminal commit resolution, and retries keep original batch/operation
identities. No durability adapter or multi-entity success gate was bypassed.

PostgreSQL enforces expiry, so application cleanup need not run on a frozen
worker. A frozen/unavailable database primary, safe promotion, coordinator
durability and host clock shifts remain infrastructure integration requirements.

## Deferred verification backlog

- Freeze a process with data and control sessions checked out; do not resume it.
  Prove another instance can reclaim and resolve the pending attempt.
- Freeze during registration, between BEGIN and local timeout setup, after data
  mutation, and around COMMIT acknowledgement. Check canonical/output effects.
- Let the data session expire before the control session and verify no false
  success or premature terminal resolution.
- Expire control admission while a data transaction is still active. Its commit
  must be rejected and publication must remain pending until safe resolution.
- Resume with idle pool connections already closed by PostgreSQL. Verify fresh
  acquisition, no returned broken sessions and no pool resource leak.
- Preserve successful normal outbox publication within its ten-second budget;
  check slower/unknown acknowledgements retain retryable obligations.
- Check configuration boundaries, restricted credentials, cancellation, race
  behavior, Citus prepared decisions, database failover and VM clock/freeze cases.

Run these only in the later validation phase. No new passing-result claim should
be added to the [acceptance matrix](Platform-Acceptance-Plan.md) until executed.

Reference: [PostgreSQL session timeout semantics and pool considerations](https://www.postgresql.org/docs/16/runtime-config-client.html).
