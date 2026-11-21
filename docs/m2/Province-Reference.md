# Province reference

The selected reference is [deployment/m2/provinces.json](../../deployment/m2/provinces.json).
Its exact bytes were copied from `didnt-work:deployment/role1/provinces.json`,
whose documentation records a 38-province table supplied by the project owner
on 2026-11-18. The owner approved reusing it on 2026-11-21. Its SHA-256 is:

```text
a82363d09ff6f8e85d48db143e4e6a99d6be4283769574fd4a802f82f647cf20
```

Names and coordinates are preserved. IDs are stable project identifiers rather
than official administrative codes. The table's BNPB provenance and agreement
with the assistants' evaluation reference remain unverified. The `(0,0)` table
under `docs/m2/fixtures/` is synthetic and is not the deployment reference.

Install this table with writers stopped, as described in the deployment runbook:

```bash
python3 scripts/m2/p4_province_reference.py --file deployment/m2/provinces.json --sha256 a82363d09ff6f8e85d48db143e4e6a99d6be4283769574fd4a802f82f647cf20
python3 scripts/m2/p4_province_reference.py --file deployment/m2/provinces.json --sha256 a82363d09ff6f8e85d48db143e4e6a99d6be4283769574fd4a802f82f647cf20 --execute
```

The installer binds the rows to the configured satellite namespace and refuses
a different table once that namespace has processed observations. Reinstall it
after an application storage reset. Preserving the exact bytes and LF line
endings preserves the checksum across checkouts.
