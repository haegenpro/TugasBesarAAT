# Role 1 M1 development lab

This independent Go module runs destructive **faults against its own disposable containers only**. It does not import application configuration, access the existing canonical database, or modify production migrations. It is not the four-node/separate-kernel deployment.

From the repository root:

```powershell
rtk proxy go -C experiments/m1 test -mod=readonly -count=1 -timeout=60s ./...
rtk proxy go -C experiments/m1 vet ./...
rtk proxy go -C experiments/m1 test -race -mod=readonly -count=1 -timeout=60s ./...
rtk proxy go -C experiments/m1 run . doctor
rtk proxy go -C experiments/m1 run . run --suite all
```

`run` defaults to `--suite all`; `core` and `ha` can also be selected independently. Requested failures or blocked execution return a nonzero exit. A counterexample is retained as a failed architectural proof even if reproducing it worked as intended. Other independent cases continue. Full deployment integration gates remain separate from the local result.

Prerequisites: Go 1.27.1 (automatic toolchain selection works), Docker Linux containers, Compose, network access for the first build/pull, and at least 3,712 MiB exposed to Docker for `all`/`ha` or 3,456 MiB for `core`/`m2`. The lab checks capacity without changing Docker settings. Native `python3`, `psql`, and application `.env` files are unnecessary.

The immutable Citus base contains PostgreSQL 16.14/Citus 14.2.0. The derived image installs Patroni 4.1.5 and the complete Python runtime dependency pins in `requirements.lock`. etcd 3.5.21 is pinned by Linux/amd64 manifest digest. Image identity and actual software versions are captured before accepting experiments. OS package versions resolved during image construction are not a promise of a byte-identical future rebuild; retain the recorded derived image ID when reproducing a specific run.

Profiles run sequentially:

| Suite | Components | What it can establish |
| --- | --- | --- |
| core | One coordinator, two single-copy workers, one etcd member; four logical shards | Candidate constraints/migration, distributed transaction recovery, partial reads, and retained-view capture. No replicated acknowledgement guarantee. |
| ha | One three-member Citus/Patroni group, three etcd members | PostgreSQL/Patroni quorum durability, group promotion, two-survivor writes, and DCS-loss behavior. No integrated Citus formation failover claim. |

Each database is limited to 1 GiB with a 512 MiB reservation, 32 MiB shared buffers and 32 connections. Each etcd member is limited to 128 MiB. Development DCS limits are not final Kubernetes resource specifications. Patroni uses a 15-second TTL, 2-second HA loop and 3-second retry timeout (`loop_wait + 2*retry_timeout <= ttl`); these are experiment budgets, not production coordination decisions. The core profile explicitly disables synchronous replication because it has one copy per group. The HA profile uses `quorum`, strict mode, one standby acknowledgement, `synchronous_commit=on`, `fsync=on`, and `full_page_writes=on`.

Every run creates an isolated project, generated credentials, random loopback ports and volumes. The printed run ID locates evidence beneath the repository's ignored `evidence/role1-m1/` directory. `compose.json` contains **generated lab credentials** and stays local/ignored. JSON results, bounded command logs, runtime versions, dependency resolutions, image identity and memory samples are stored alongside it. Do not publish the generated configuration as a report attachment.

The runner stops containers/networks on exit but retains volumes and evidence. Cleanup after an interrupted run is explicit:

```powershell
rtk proxy go -C experiments/m1 run . down --run <printed-run-id>
```

`down` validates the run ID and resource ownership labels. It does not remove volumes. No command resets other Compose projects or the application stack.

The snapshot gate is an experiment-only shared/exclusive PostgreSQL advisory lock, plus a persisted epoch for publication. All normal fixture mutations use the shared admission helper. Schema initialization, deliberately rejected negative probes, and snapshot materialization are distinct lab operations. Capture actually exports and retains independent primary views while admission is closed; those handles are not portable across database instances or recoverable after loss. A real child-process kill checks lock release, but frozen owners/authoritative lease expiry remain Role 2 integration work.

The correction experiment separates stable transaction recovery from read consistency. Its deterministic two-unit reader reads the loser before a committed correction and the survivor afterward. A mixed result blocks that unversioned read strategy. This is **not** evidence that a single coordinator SQL statement fractured; no production publication/read protocol is silently substituted to make the result pass.

For the observed results, blockers and downstream handoff, see [Role 1 M1 feasibility](../../docs/Role1-M1-Feasibility.md).

## M2 application proofs

`rtk proxy go -C experiments/m1 run . doctor --suite m2` checks the core profile.
`rtk proxy go -C experiments/m1 run . run --suite m2` starts its own core formation and runs the actual Aggregator migration/write/read tests plus both consumers' PostgreSQL retraction tests. Credentials travel through child environment variables. Fault requests use recorded handshakes; the controller validates resource ownership before pause/unpause/termination-related actions. Tests never use the application environment files or database.

`all` retains its original M1 meaning (`core`, then `ha`). Select `m2` explicitly. M1's original visibility counterexample remains failed historical evidence; M2 tests its application protocol separately. The same `down --run` command handles M2 projects and retains volumes. See [M2 write-core report](../../docs/Role1-M2-Write-Core.md) for the acceptance boundary and migration commands.
