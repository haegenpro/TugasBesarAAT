# Integrated release preparation

The target is four physical hosts, each with one VM. Actual addresses, provider,
RAM and disk capacity remain operator inputs. Do not use the former single-host
7 GiB bootstrap allocation as a full release capacity claim.

`scripts/platform-release.py` assembles the canonical Citus formation, independent
ingestion PostgreSQL formation, service catalog, owner processing, isolation and
monitoring into one dependency-ordered bundle. It does not build images, start
VMs, rotate credentials, migrate databases or apply resources.

Copy `release.example.json` into a private directory and provide its referenced
JSON files. `images.json` combines the service/monitoring digest maps with actual
`storage` and `role1` image digests. Build the role1 image using
`deployment/role1/Dockerfile` with repository-root context. Never use a made-up
digest or a synthetic province table for an accepted release.

The role1 input must explicitly enable analytics and contain the R3 source
configuration. `wildfire_config` and `province_reference` reference UTF-8 files;
their exact table hash and satellite namespace/start must match. Use the actual
feed session and course reference inputs. Audit sessions can be enabled in the
same input; live Audit API is enabled independently by the owner renderer.

The private satellite session check confirmed that `from=0` yielded detection IDs
0, 1 and 2 on the practice feed. The session reported 60x speed. This is a bounded
interface observation, not a burst/recovery/zero-loss acceptance result. The
credential is deliberately absent from this file and release artifacts.

```sh
python3 scripts/platform-release.py --inventory /private/four-hosts.json \
  --config /private/release.json --output /private/releases/RUN
python3 scripts/platform-deploy.py plan --inventory /private/four-hosts.json \
  --bundle /private/releases/RUN/bundle.json
```

The bundle handles canonical and ingestion ownership separately, records their
Secret references and avoids duplicate Aggregator/Namespace resources. Missing
owner-database artifacts, captured PostgreSQL identities, offline migration/
cutover, or capacity remain visible blockers. Complete those prerequisites before
removing their corresponding unresolved entries. Review exact artifact hashes in
the plan; application apply uses the validated bytes rather than rereading files.

The broker catalog now reserves five 1 GiB raw/critical streams plus three
256 MiB legacy streams, 5.75 GiB per broker. Its 8 GiB file-store cap requires a
10 GiB local volume, leaving 2 GiB declared filesystem headroom. Every broker
needs that capacity because all streams use three copies. These are declared
reservations, not measured use; physical disk growth and retained WAL/database
data need separate accounting. Existing PVCs/data are never implicitly resized.
Keep raw/critical retention and acknowledgement rules unchanged.

Focused offline regression checks are authored in `bundle_integration_test.py`.
The full four-node, migration, failover and sustained-load validation remains a
later execution phase; rendered files alone do not establish acceptance.
