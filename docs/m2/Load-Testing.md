# M2 load testing

## Sustained load and rolling replacement (P2-6, P2-8)

The runner uses the public Client API NodePort and a pinned k6 Docker image. It
keeps credentials in a private output directory, bounds generator memory/CPU,
stops on low host memory, and records actual runtime overrides and node limits.
The generator and cluster share this PC; repeat performance checks from the
separate generator machine before claiming real deployment capacity.

Start the cluster and deploy the intended images first. Use a fresh output path
for each run. Inspect `run.json`, `summary.json`, and `generator.log` together.
A zero runner exit means the k6 thresholds passed; it does not certify every M2
criterion. Skipped integration cases are not load-test evidence.

```bash
python3 scripts/m2/p2_load_test.py --config .m2-local/config.json \
  --output .m2-local/load-smoke-new --vus 20 \
  --ramp-seconds 5 --hold-seconds 15

python3 scripts/m2/p2_load_test.py --config .m2-local/config.json \
  --output .m2-local/load-10000-new --scale \
  --roll-from aat/aggregator:ACTUAL_PREVIOUS_TAG
```

Defaults are 10,000 VUs, 30-second ramp, 135-second hold, 15-second ramp-down,
one-second think time, 10% Field VUs, and a Field report every ten iterations.
Each VU logs in and refreshes its own session; all VUs share the configured demo
client identities. Per-client admission is therefore shared across those sessions.
Reports retain the same idempotency key and bytes after an ambiguous response.
Live Auth introspection, response validation, and normal admission remain active.

The full run refuses to start with source polling/replay or coastal acquisition
explicitly paused, or with source processors scaled to zero. Record whether
satellite acquisition is enabled; bounded rehearsal configurations may disable it
to preserve disk space.

With `--scale`, actions begin after the observer first sees the requested VUs:
scale to five after 15 seconds and to two after 80 seconds. `--roll-from` first
deploys the specified older Aggregator image, then requests the configured image
after 45 seconds at target VUs. Finally, the runner restores three Aggregators
and the configured image. Examine readiness/image observations to verify actions
completed; requesting an action alone does not prove convergence or continuity.

Metrics distinguish controlled 429 responses with Retry-After, unexpected errors,
transport failures, all unsuccessful outcomes, and successful reads. Hazard
responses are checked for the partial flag, count/data agreement and as-of shape;
complete and explicitly partial pages have separate counters. Earlier captures
without these checks establish HTTP success only. The primary
unexpected-error denominator excludes controlled rejections; an additional
all-operation metric retains the earlier denominator. Successful-only latency
avoids fast rejections masking slow useful requests. Role/operation breakdowns
show Field login, reads, reports, refresh, and rejection outcomes.

Do not add an arbitrary allowed-rejection percentage to M2. Establish sustainable
capacity from successful throughput and latency; report deliberate overload
separately. Preserve the independent requirements for Field service, three versus
one throughput of at least 2x, zero failed rolling requests, coordinated polling,
and recovery. CPU-saturated generators and missed VU observations must be reported.

## Fixed-dataset throughput comparison (P2-7)

From an operator machine with access to the target cluster and Docker running:

```bash
python3 scripts/m2/p2_throughput_compare.py --output .m2-local/throughput-RUN
```

The runner compares one and three Aggregators using the same dataset, 10,000
VUs and one total request rate during both measured holds. It disables Field
report submissions, pauses BMKG/PVMBG polling and replay and coastal acquisition,
and stops the three processors during the comparison. If satellite acquisition
is enabled, pause it with `manage.py satellite off` before the comparison and
restore it afterwards. The runner restores the standard three Aggregators and
two replicas per processor when it finishes.

The default `--target-rps auto` samples one Aggregator in a 30-second pilot and
sets the shared cap to three times its measured complete pages/s, with a floor
that gives every VU time to issue a measured request. Use `--target-rps N` to
supply the cap. A one-instance result at or above 90% of the cap is `INVALID`.
If three instances reach at least 95% of the cap, the measured ratio is a lower
bound.

`comparison.json` records hold-window complete pages/s, the ratio, replica
placement, dataset row counts and validity checks. Inspect it alongside the
per-run `summary.json` files. The comparison requires at least 2× throughput,
a fixed dataset and valid samples. Partial or invalid pages, transport failures,
unexpected errors or insufficient VU participation prevent a passing verdict.
