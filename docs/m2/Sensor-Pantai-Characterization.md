# Sensor Pantai Characterization

Observed on 16 November 2026 against the unmodified image
`sustuna/sensor-pantai@sha256:51fbd90d3891ad6e9f5768ad330ffb65205644cb29e50da65c7538f2e0b4397d`,
run locally with `PANTAI_PROFILE=practice`, `PANTAI_SEED=42`, `PANTAI_SPEED=60`.
The exam profile is encrypted and was not observed. Nothing below is tuned to an
observation ID, seed or ordering of the practice profile.

Reproduce with:

```sh
docker run -d --name pantai -p 18080:8080 -e PANTAI_PROFILE=practice -e PANTAI_SEED=42 \
  -e PANTAI_SPEED=60 -e PANTAI_AUTH_TOKEN=lab-token \
  sustuna/sensor-pantai@sha256:51fbd90d3891ad6e9f5768ad330ffb65205644cb29e50da65c7538f2e0b4397d
PANTAI_AUTH_TOKEN=lab-token python3 scripts/probe-sensor-pantai.py --seconds 600
```

## Results

| Run | Requests | Status | p50 / p95 / max latency | Unique observations | Exact duplicates | Changed content under one ID | Critical |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: |
| 150 s | 498 | all 200 | 5 / 14 / 70 ms | 1,003 | 17 | 0 | 3 |
| 600 s | 2,035 | all 200 | 5 / 19 / 101 ms | 3,524 | 83 | 0 | 3 |

- **Authentication.** `/v1/stations` and `/v1/observations` return 401 without the bearer token. `/healthz` needs no token and returned 200 for every probe.
- **Stations.** There are four stations: Padang (Sumatera Barat), Banda Aceh (Aceh), Cilacap (Jawa Tengah) and Pelabuhan Ratu (Jawa Barat). The list did not change during either run.
- **Cursor.**
  - `next_cursor` is an opaque base64 value (it decodes to `{"o":<offset>}`; the adapter does not rely on this).
  - On the last page it is `null` and `items` is empty.
  - Resuming with the previous cursor continues correctly, so the adapter keeps its last non-null cursor instead of overwriting it with `null`.
- **Clock.** `generated_at` advances at the configured speed: 150 real seconds moved the sensor clock by about 2.5 hours. Freshness is therefore measured from `observed_at` against `generated_at`, never from the local wall clock or `/healthz`.
- **Duplicates.** About 2.3% of observations were delivered again with identical content. Warning identity is content-based, so a repeated observation does not create a second notification; repeated canonical writes return `no_change`.
- **Quality.** Only `ok` and `suspect` appeared. The adapter keeps `quality` as an attribute and does not reject unknown values, because the contract does not enumerate them.
- **Critical.** Three critical observations appeared, each with `quality: ok`.

## Consequences for the adapter

| Observation | Setting or behaviour |
| --- | --- |
| p95 latency is under 20 ms when healthy | `COASTAL_REQUEST_TIMEOUT=1s` leaves a wide margin and still fails fast. |
| A page arrives every 0.25–1 s at 60× | `COASTAL_POLL_INTERVAL=500ms` keeps the critical path well under the 5 s target. |
| Sensor clock moves 60× faster than real time | `COASTAL_STALE_AFTER=60s` compares sensor clock with the newest observation and also flags no new observation for 60 real seconds. |
| Healthy `/healthz` says nothing about data | `/healthz` is never used for freshness or breaker recovery. |
| Duplicates exist | Warnings use `warning-v1` IDs derived from observation ID and payload digest. |

The practice profile showed no outages, malformed bodies, contract violations or unknown stations. The validation, quarantine, circuit breaker and stale-data handling are covered by unit tests with synthetic failures; their behaviour against the exam profile still has to be recorded during game day.
