# Deferred M2 load and fault evidence

Implementation only: no workload, VM fault, rollout or comparison has been run.
Do not use the lightweight laptop profile for the full 10,000-VU acceptance run.
Measure capacity first and preferably use a separate load-generator machine.

## Workload preparation

Provide a private token inventory containing objects with `role` (`field` or
`media`) and `access_token`. Use enough distinct authenticated clients that the
per-client limiter does not become an accidental benchmark of one identity.
Tokens must stay valid for the whole window; 401 is an unexpected error. The
inventory is read through k6 SharedArray and excluded from summary output.

Supply the actual common dataset digest and shared Client API URL. The workload
uses 95% Media / 5% field VUs, 50ms think time and authenticated hazard reads.
It does not submit field reports, generate satellite/coastal input, or fabricate
recipient delivery evidence. Owners run their identified source/replay fixtures
concurrently and retain the original expected operation IDs and event timestamps.

```sh
python3 scripts/platform-game-day.py plan --scenario scale
# Later, on adequately sized infrastructure:
python3 scripts/platform-game-day.py run --scenario scale --seconds 150 \
  --base-url http://CLIENT-ENTRY --token-file /private/tokens.json \
  --dataset-sha256 ACTUAL_DATASET_SHA256
```

Other scenarios are `rollout --image repository@sha256:...`,
`node-pause --node aat-m2-a`, and `baseline --baseline-replicas 1` or `3`.
Prepare the requested baseline before execution; this runner never starts VMs.
The scale scenario requests five replicas at 30% and two at 65% of the window.
The node scenario pauses at 30% and resumes at 55%, with an additional finally
cleanup on ordinary errors, SIGINT, SIGTERM and SIGHUP. SIGKILL/host failure
cannot run cleanup: inspect and resume the recorded owned VM manually.

Runs capture revision, workload hash, dataset hash, images, desired/observed pod
and deployment status, node state, periodic `kubectl top`, action timestamps and
k6 results under ignored private `evidence/platform-game-day/<run-id>/`.
Record load-generator CPU/RAM/network samples separately in that directory.
The script does not collect Secrets or container environments. Scale/rollout
changes remain in place, with original replicas/images recorded for an explicit
later restoration. It never automatically rolls back a successful new version.

## Reconciliation and interpretation

Every initial result has `passed:false`. A zero k6 exit code means only that the
read workload and its thresholds completed. Inspect whether new replicas became
Ready while load was active, rollout completed, acquisition stayed unique, and
recovery occurred without manual intervention in the defined recovery window.

The owning services produce a reconciliation JSON with `run_id`, matching
`dataset_sha256`, nonempty distinct `expected_critical_ids`, and
`critical_receipts` containing `operation_id` and original-event `latency_ms`.
Also provide `polling_unique:true`, `logical_reconciliation_passed:true`, measured
`acquisition_takeover_max_seconds` and `freshness_catchup_seconds`, and
`load_generator:{"saturated":false,"samples_artifact":"generator-samples.json"}`.
These values are assertions backed by retained owner artifacts, not platform
measurements. Review their event coverage, timestamps and denominator.

```sh
python3 scripts/platform-evidence.py reconcile evidence/platform-game-day/RUN \
  /private/owner-reconciliation.json
python3 scripts/platform-evidence.py compare evidence/platform-game-day/ONE \
  evidence/platform-game-day/THREE
```

Reconciliation rejects missing inputs, lost critical IDs, p95 above five seconds,
less than 95% timely delivery, takeover/catch-up over 60 seconds, and unexpected
read errors >=1%. Duplicate receipts are reported separately; this does not prove
all idempotent effects. Controlled 429s are counted separately and excluded from
the unexpected-error denominator; successful throughput excludes them entirely.

The 1-vs-3 comparison requires the same revision, workload and dataset and reports
the >=2x numeric criterion. Its `passed` remains false pending review of unchanged
upstream capacity and load-generator headroom. A full project acceptance also
needs disk-loss, clock-shift, freeze/resume, audit expiry/partial reads, source
burst, isolation, acknowledged-write recovery and Tester Agent evidence from the
acceptance matrix. A successful read workload cannot substitute for these.
