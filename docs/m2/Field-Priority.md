# Field priority under overload (P5-5)

P5-5 asks that, when load exceeds capacity, Tim Lapangan requests keep being served while Media
requests are rejected in a controlled way. Problem 3 adds that a Field report must not be lost
just because the system is busy. This note records how Field is prioritized, what the 10,000-VU
runs measured, and why Field is still rejected most of the time in that test.

## How Field is prioritized

| Layer | Mechanism (deployed values from `scripts/m2/tuning.py`) |
| --- | --- |
| Client API, token validation | Field access and refresh tokens carry an HMAC-signed priority marker, so Field is recognised without calling Auth. Field has its own 8 of 32 Auth-call slots per Client API instance and its own 8-connection pool to Auth. |
| Client API, request capacity | `reserveFieldCapacity`: 64 requests in flight per instance, 16 reserved for Field. Media can hold at most 48; Field can take any free slot. AI `/situation` calls count as general traffic. Over the limit: 429 with `Retry-After: 1`. |
| Client API, per client | `limitInFlightPerClient`: 16 in flight per client identity per instance, for every role. |
| Auth service | 32 in flight with 8 reserved for Field; 3 of its database connections reserved for Field. Saturated storage answers 429 with `Retry-After` (not 503). |
| Aggregator reads | Field reads go to `/internal/field/...` with a dedicated key and 3 dedicated database connections. |
| Field report acceptance | Durable ledger row, raw input and outbox entry in one transaction before the 202. Acceptance has its own deadline (`FIELD_ACCEPT_TIMEOUT_MS`, 2 s); past it the answer is 429 with `Retry-After`, and retrying the same `Idempotency-Key` returns the same receipt. |

## What the 10,000-VU runs measured (minikube, 2026-11-22)

The load script runs 9,000 Media VUs and 1,000 Field VUs (`M2_FIELD_FRACTION=0.1`) at one second
of think time. All Field VUs log in as the same client, `field-demo`; all Media VUs as `media-demo`.

P2-6 run (`.m2-local/p2/p2-6`, 701,137 responses, no 5xx, unexpected error rate 0.0000%):

| | Field | Media |
| --- | ---: | ---: |
| Requests | 68,417 (9.8%) | 632,720 (90.2%) |
| Successful | 18,207 (26.6%) | 47,589 (7.5%) |
| Share of all successes | 27.7% | 72.3% |
| Field reports accepted / rejected (429) | 6,039 / 21,208 | — |

Prioritization works relatively: Field succeeds about 3.5 times as often as Media and receives
almost three times its share of demand in successes. It does not make Field "always served":
about three of four Field requests were rejected.

Rejection reasons on the Client API (cumulative counters over two later runs, both instances):

| Reason | Count |
| --- | ---: |
| `authentication_capacity` (Auth-call slots full) | ~869,000 |
| `per_client` (16 in flight per client identity) | ~486,000 |
| `concurrency` (global in-flight limit) | ~13,000 |
| `authentication_upstream_capacity` (Auth answered 429) | ~3,400 |
| `role_capacity` (the Field reservation) | 0 |

The Field reservation never triggered: with every Field VU sharing one client identity, the
per-client limit of 16 per instance bounds Field before the 16 reserved slots matter.

## Why Field is still rejected, and what was tried

1. **One identity for all Field devices.** 1,000 Field VUs as `field-demo` share one per-client
   budget. Exempting Field from the per-client limit was tried and reverted: Field reports then
   queued instead of being rejected (1,472 requests exceeded the 5 s client timeout, the unexpected
   error rate rose to 3.03% and Field unsuccessful rose to 82%). The per-client limit was acting as
   Field's admission control, and removing it only moved the queue downstream.
2. **Field report acceptance serializes per submitter.** `AcceptReport` assigns each report's
   position with `UPDATE source_progress SET received = received + 1` for the submitter, inside the
   acceptance transaction. Every report from one identity therefore waits on one row lock through
   a synchronously replicated commit. Nothing downstream needs that position to be gapless
   (`ProcessReport` is idempotent per receipt and order-independent), so acceptance now takes it
   from the `field_report_position` sequence instead, which takes no row lock. Positions stay
   unique; the idempotency lock, ledger row, raw input and outbox entry are unchanged, so the
   202 guarantee is the same. Field reports no longer update `source_progress`, so the progress
   view no longer lists a Field received count (counts come from the `field_reports` ledger).
   Not yet measured under the 10,000-VU load.
3. **The host.** All nodes share one disk-bound, swapping 16-core host; Field acceptance is a
   durable write and its latency follows the disk.

With the per-client limit restored and the acceptance and Auth paths answering 429 instead of
waiting, the strict P2-8 run had no 5xx and no Field report timeouts, and Field was unsuccessful
80.2% of the time (Media 94.0%).

## Position for the report

- Field is prioritized at every layer and is served measurably better than Media under overload.
- In this test Field demand alone (1,000 devices behind one identity, one request per second each)
  exceeds what one host serves, so "always served" is not reachable on this hardware.
- No Field report acknowledged with 202 is lost; a rejected report gets 429 with `Retry-After` and
  can be resubmitted with the same `Idempotency-Key` (P3-6).
- Per-submitter serialization in acceptance (point 2) is removed in code; it still needs a load
  measurement, preferably on the two-VM deployment with the generator on a separate machine.
