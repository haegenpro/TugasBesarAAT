# Bounded satellite contract probe

The role1 image includes `/usr/local/bin/satellite-probe`. It uses the production
SSE reader and normalizer, including nested detections, numeric acquisition time,
the `frp` alias and confidence values. It only reads the source; it does not
publish, advance ingestion progress or reset a session.

Run it later with the same private token and agreed namespace as ingestion:

```sh
satellite-probe --token-file /private/satellite-token \
  --namespace bnpb-satellite-practice-v1 --from 0 --records 32 \
  --duration 45s --output /private/evidence/satellite-RUN
```

The output directory must be new and its parent must already exist. The token
must be in a private regular file, not in arguments. Evidence uses mode 0600 and
contains session metadata, offsets, event kinds, hashes and phase durations;
neither bearer credentials nor raw payloads are recorded. Capture the actual
image digest alongside the result. The role1 image has an Aggregator entrypoint;
use an explicit `/usr/local/bin/satellite-probe` entrypoint when running its
container as this tool.

Three bounded connections check the initial range, replay starting at the last
received offset, and replay starting at its successor. Every range must be
contiguous with inclusive `from` semantics. Repeated identities must preserve
their type and payload hash. The check rejects changed sessions, changed playback
rates, gaps, unreadable payloads, truncated streams and deadline exhaustion.
An unnumbered keepalive heartbeat is ignored like the production connector.
Redirects are refused so bearer credentials cannot follow another origin.

`--disconnect-for 10s --duration 90s` adds a bounded deliberate disconnect before
resuming. Counts are limited to 1,024 records per leg, each event to 1 MiB, each
connection to 64 MiB, and the whole probe to five minutes. A failed run retains
partial evidence and exits nonzero. Choose a new output directory for a retry.

Local fake-stream tests exercise successful replay plus gaps, changed payloads,
changed groups, normalizer rejection, truncation, redirects and timeouts. No
extended live probe has been run yet. The earlier three-record practice sample
remains an interface observation only.

This tool does **not** prove full source completeness, the 60x burst/catch-up SLA,
canonical results, broker durability or node recovery. Run those scenarios on
the real deployment with original operation identities, agreed province/course
reference data, ingress/canonical timestamps and recipient receipts. Event kinds
absent from a bounded sample remain unverified. A session speed of 60 by itself
is not burst acceptance evidence.
