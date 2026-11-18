# Input-driven deployment and validation

The remaining image, maintenance and source/game-day automation is now written.
The four-machine allocation remains an assumption: no hardware questionnaire is
needed to prepare or review this workflow. Inventory values describe requested
VM allocation, addresses, providers and ownership paths. Existing runtime
capacity checks still stop an operation that cannot run on the actual hosts.
No VM, registry push or live fault has been executed for this change.

The workflow targets first deployment on the Linux controller, with the existing
Windows/VirtualBox and Linux/libvirt host helpers and trusted SSH configuration.
Python, Docker, OpenSSL, provider prerequisites and k6 must be installed for their
respective phases. The current database topology and resource requests are kept.

Copy these examples to a private directory and supply their values:

| File | Inputs |
| --- | --- |
| [Four-host inventory](../deployment/platform/inventory.fourhost.example.json) | Actual machine/VM addresses, provider/bridge/SSH paths and declared VM allocation; one VM per physical host |
| [Image release](../deployment/platform/images.release.example.json) | Registry/repository prefix and actual pinned Tempo/node-exporter images; other application and monitoring images are built |
| [External inputs](../deployment/platform/inputs.example.json) | Audit/satellite token **file paths**, approved province reference, bounded satellite egress CIDRs, image-map path and chosen stable source identities |
| [Source reference](../deployment/platform/source-reference.example.json) | Independent expected satellite operations, critical coastal identities and course/reference Audit count/checksum |
| [Workflow](../deployment/platform/workflow.example.json) | The paths above, private output directory and selected validation stages; a different compatible rollout image, owned pause target and later generator link budget |

Keep the existing private release scaffold as `private_release_config`; input
completion preserves its internal passwords and keys. Its generated
`client-credentials.json` lets validation log in automatically for fresh Field
and Media sessions before each load stage and log out afterward. A private
`validation.load.token_file` remains an alternative. Generated sessions use the
configured clients and their normal per-client limits; they do not create new
client identities. Never put token values in command arguments or commit private
configuration. Audit token and real province reference are still pending inputs.

Set the external input's `images` path to `OUTPUT/images/images.json` when using
the workflow's image phase. Registry login uses the local Docker credential
configuration. Docker build contexts contain filtered **tracked files only**;
private/untracked evidence and `.env` files are excluded. Registry-confirmed
digests, build logs and source identity are retained. A failed push cannot emit a
successful complete image map. No image is published by `plan`.

```sh
# Read-only command plan, including the assumed allocation:
python3 scripts/platform-workflow.py plan --config /private/workflow.json

# Later, once corresponding inputs/prerequisites exist:
python3 scripts/platform-workflow.py run --config /private/workflow.json --phase images
python3 scripts/platform-workflow.py run --config /private/workflow.json --phase inputs
python3 scripts/platform-workflow.py run --config /private/workflow.json --phase cluster
python3 scripts/platform-workflow.py run --config /private/workflow.json --phase bootstrap
python3 scripts/platform-workflow.py run --config /private/workflow.json --phase maintenance
python3 scripts/platform-workflow.py run --config /private/workflow.json --phase activate
python3 scripts/platform-workflow.py run --config /private/workflow.json --phase access
python3 scripts/platform-workflow.py run --config /private/workflow.json --phase validate
```

`run --phase all` performs that sequence when all inputs are ready. Use individual
phases for the first real deployment. `cluster` provisions/starts the owned VMs,
bootstraps K3s and installs the unchanged Tester Agent. `bootstrap` prepares all
six owned database formations, local paths, Secrets and isolation without starting
application writers. `activate` prepares application/monitoring volumes and
applies the reviewed full bundle after maintenance evidence is verified.

`maintenance` refuses active application controllers/pods. It captures original
identities for all six formations, bootstraps the pinned Registry, initializes
restricted owner schemas, runs canonical and ingestion migrations and performs
the restartable BMKG cutover. It then reads actual migration journals and cutover
state, rechecks original identities and emits `release-ready.json`. The full
renderer resolves initialization blockers only from a successful receipt matching
current migration/tool hashes, images, inventory, namespaces and original
durability captures. No manual deletion of those blockers is needed. A different
source/image/schema/capture refuses the receipt.

`access` creates labelled validation NodePort Services (31080/31081), precise
ingress policies allowing only the controller and owned VM IPv4 addresses, and a
separate pinned HAProxy container on the controller. Its frontends bind only
`127.0.0.1:18080` for Client API and `127.0.0.1:18081` for Aggregator/Audit and
balance over all four VMs. It does not change the management API proxy. The host
container has a 256 MiB memory limit, read-only configuration and bounded logs;
include this controller-side cost in later observation. Existing unowned or
different resources/proxies are refused. Configured external validation URLs may
override these defaults. This entry point covers VM faults while the controller
host remains available; it does not prove controller-host-loss continuity.
The workflow's `access` block can choose different frontend/NodePort values to
avoid existing port allocations. This changes input data, not scripts.

Each workflow command has a durable intent/completion journal and a private log.
Completed commands are skipped on a repeat with unchanged workflow inputs.
Interrupted/failed commands are not blindly repeated: inspect their retained
artifacts before selecting the next phase or a new output directory. Never
delete original database identities or recovery journals to make a retry pass.
Supply final input files before execution; use a new output directory when
changing inputs. Existing deployed credentials require explicit rotation.
Signals are forwarded to child runners so their normal VM/ingestion cleanup can
run. SIGKILL/controller loss still requires inspection. This is first-deployment
automation, not an upgrade procedure for a running or previously repaired cluster.

The validation phase reads an independent reference dataset, not success flags.
Satellite hashes cover **normalized operation bytes** persisted as
`input_history.original_payload`, not raw SSE frames or encoded Avro frames.
Offsets must form a nonempty contiguous range for the configured namespace.
The bounded probe's `operation_sha256` is the corresponding hash for an observed
valid event; it is not an independent correctness oracle. Use the prescribed
source/course data for expected results. Critical coastal IDs use the project's
`warning-v1:` identity contract. Templates deliberately contain invalid
placeholders and cannot pass as real evidence.

The source runner queries captured original PostgreSQL members using read-only,
bounded statements/pages. It compares every supplied satellite offset/hash and
applied frontier, raw critical reference IDs, ingestion expectation rows and
durable recipient commits. Missing/rejected operations, changed timing origins,
recipient conflicts and partial/wrong Audit checksums fail. It measures current
database clock offset intervals from controller monotonic round trips and adds
their uncertainty to delivery latency before checking p95 <=5 seconds and >=95%
timely delivery. Deliberate clock shifts require separate timing evidence; current
offset samples do not establish historical synchronization.

The 60x test checks actual live session speed/group, stops only the UID-guarded
ingestion deployment, waits for its pods to terminate, observes new source offsets
during a 2..30-second disconnect, restores the original replica count and measures
canonical catch-up to the observed release-time frontier within 60 seconds.
Cleanup reconciles a lost scale reply and refuses a recreated/concurrently changed
controller. This exercises 60x backlog/replay recovery; it does not control the
provider's speed or claim correctness beyond the supplied source-reference range.
For newly applied offsets across the disconnect, it also measures first-BNPB-
acquisition to observed committed applied-state latency, with a conservative p95
upper bound. Original envelope timestamps and one captured PostgreSQL clock
witness on each VM bound the clock uncertainty. The reference must cover that
whole new range; missing timings or witnesses fail completeness. Existing
historical timings are not fabricated. An optional
`satellite.entry_p95_max_ms` in the reference applies a supplied evaluator limit;
no unspecified satellite latency target is invented.

Load stages issue at least 10,000 VUs for at least 120 seconds. `combined` scales
to five, rolls to a different immutable image, pauses/resumes an owned VM and
scales to two in the same window. Readiness during load, actual new-image readiness,
Field/Media successes, unexpected errors and generator CPU/memory/network samples
are checked. The generator link budget is an input for that later measurement,
not a request to reassess the four hosts now. Unknown budget or saturated/missing
samples cannot pass headroom analysis.

One-/three-replica comparisons require unchanged canonical generation with no
pending attempts throughout/between baselines, identical image/template and
upstream specification hashes, matching workload/revision/reference and measured
generator headroom. Active source sessions must reach the agreed stable reference
checkpoint before throughput comparisons; ongoing canonical writes fail this
comparison rather than being labelled an unchanged dataset. The successful
throughput ratio must reach 2x. The supplied rollout image must preserve the
current schema/protocol contracts.

Results cover only the selected scenarios and declared reference dataset. Empty
physical disk replacement, all-group storage HA, partition/clock/freeze cases and
the physical demonstration still use their existing dedicated runners and
[acceptance matrix](Platform-Acceptance-Plan.md). A selected suite result does not
mark every project acceptance row passed. Real-node failures may still require
integration fixes after measurement.

Local verification covers sequencing, bad receipts/changed inputs, failed pushes,
token cleanup, incomplete faults, readiness/headroom gates and interrupted scale
replies. Real disposable PostgreSQL checks verify the source observer SQL,
missing receipts, changed payloads and wrong physical identities; successful run
`evidence/platform-source-validation/fef669e0574b` cleaned up its containers.
Those checks do not establish physical deployment or HA/load acceptance.
