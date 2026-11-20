# Broker account wiring and empty-storage recovery

The Kubernetes broker profile defines an `APP` account with JetStream enabled
and a separate `SYS` system account. These are distinct authorities: application
clients inspect/publish/consume their streams; only the platform recovery command
uses the system connection for fenced server removal.

Prepare the private `broker-runtime` Secret from
`deployment/services/broker-runtime.env.example`, replacing every placeholder.
All three brokers use the same account credentials. Use distinct generated
hexadecimal passwords so they can be embedded safely in the private URLs.
The broker configuration contains environment references, not credential values.

The separate `broker-init-runtime` Secret contains only the APP `NATS_URL`, as
shown in its env template. Aggregator, ingestion, coastal processing and both
consumers also receive this APP URL through their own runtime Secrets. The
Aggregator's authenticated connection comes from its runtime Secret rather than
the public ConfigMap endpoint. No application or broker-init pod receives the
broker system Secret. Live bundle preflight checks required Secret key names.

This account layout is for the integrated deployment's first bootstrap. Existing
JetStream data in another account, including the former default account, needs
an explicit data/account migration before changing configuration. No existing
cluster or stored stream is migrated by the renderer.

`recover-node observe` checks the explicit original stream/member list and two
current surviving copies. The parent independently checks that the owned target VM is fenced shut off. It also
requires `NATS_SYSTEM_URL` and sends a read-only system request addressed to the
application connection's actual server ID. The response must identify that exact
server and an original member name. This rejects missing/wrong credentials or a
system URL pointing at another cluster before the controller holds workloads.

Normal empty-disk replacement retains original Raft membership and server names;
it does not call peer removal. Permanently removing a name can prevent it from
rejoining as an ordinary replacement.

The separate `recover-node remove --fenced` decommission command repeats the
checks and sends the system API's required
`{"peer":"ORIGINAL_SERVER_NAME"}` payload. It does not use the application
connection for this operation. Parent orchestration independently requires the
owned VM fenced shut off before calling it. `verify` checks that all three
original server names have current stream replicas after empty-storage rejoin.
The helper checks every durable consumer's original three-copy quorum as well,
refuses memory-only durable state and bounds enumeration to 64 durables per
stream. The controller checks recorded stream sequence numbers and durable
acknowledgement prefixes for regression, including missing consumers.

## Focused protocol check

```sh
python3 scripts/test-broker-recovery.py
```

The script uses cached Go/NATS images and a labelled temporary runner with no
external network. Three actual NATS processes have separate ports and data
directories. Their configuration comes from the service renderer; only test
addresses, local paths and a small storage cap are substituted. The test rejects
removal without explicit fence acknowledgement, missing system credentials and using APP credentials
for administration. It publishes and acknowledges messages, kills one broker,
continues publishing through the surviving quorum, retains membership, erases only the test broker's temporary storage, and rejoins it empty.
It then checks every original message payload and durable-consumer progress.
Only after that proof does it separately exercise privileged fenced decommission,
without attempting to reuse the permanently removed identity.

Logs/results are retained under ignored `evidence/platform-broker-recovery/`.
Cleanup only removes containers bearing that run's exact ownership label. These
are broker protocol checks on one isolated runner. Physical networking, disks,
VM fencing, Kubernetes reconciliation, interrupted replacement phases and
full-system accepted-operation zero-loss evidence still require later execution.

Successful focused run: `evidence/platform-broker-recovery/a7b4c298ac21`, two
tests passed, no failures/skips and no cleanup required. Earlier failed runs are
retained, including the counterexample where permanent peer removal prevented
same-name recovery. The original-membership rejoin is also separately exercised
in `8e74832fb916` before durable-consumer quorum checks were added.

API references: [NATS 2.15 peer-removal request and system-account handler](https://github.com/nats-io/nats-server/blob/v2.15.0/server/jetstream_api.go),
[system server-ID requests](https://github.com/nats-io/nats-server/blob/v2.15.0/server/events.go).
