# Consistent source checkpoints

`scripts/platform-source-backup.py` supplies the capture half of the existing
source/sensor restore workflow. It supports the owned BMKG, PVMBG and coastal
sensor StatefulSet volumes. It never scales a controller, starts a VM or restarts
a source. Capture requires an explicit maintenance window with selected source
controllers already scaled to zero and their pods fully terminated.

```sh
# Read-only plan from the reviewed release and its volume requirements.
python3 scripts/platform-source-backup.py plan --inventory /private/four-hosts.json \
  --bundle /private/releases/RUN/bundle.json \
  --requirements /private/releases/RUN/service-requirements.json

# Later, with the selected writers already stopped:
python3 scripts/platform-source-backup.py capture --writers-stopped \
  --inventory /private/four-hosts.json --bundle /private/releases/RUN/bundle.json \
  --requirements /private/releases/RUN/service-requirements.json \
  --claim aat-fourhost-bmkg-mock-a --output /private/checkpoints/CAPTURE
```

Omit `--claim` to select all three source volumes; repeat it for a subset. The
command checks every selected writer before accessing any data. It verifies
live controller ownership/UID, zero replicas, absence of pods referencing the
claim, the bound PVC/PV identity, node placement, guest ownership markers,
mounted data disk and file ownership. It checks controller/claim state again
after reading each volume. Keep other operators from rescaling during capture.
Output must be new and private; repository-local backups belong under ignored
`evidence/`. Existing checkpoints are never overwritten.

The guest script detects SQLite by its actual file header, including archives
whose filenames end in `.json`. SQLite's backup API incorporates committed WAL
into a standalone database snapshot and `quick_check` verifies it. Transient WAL,
SHM and journal sidecars are excluded from that snapshot's tar. Other regular
files are copied while writers are stopped. Links/special files, unexpected
owners, changed source contents, failed integrity checks and quota violations
are refused. Captures use the same 64 MiB/10,000-entry bounds as recovery.

Each volume produces an uncompressed tar and private JSON receipt containing its
SHA-256, deployment/volume/controller identity, capture time and SQLite checks.
For source history tables the receipt includes row count and highest sequence.
`checkpoints.json` records which captures finished; an interrupted set is marked
incomplete. Inspect incomplete output and use a new directory when retrying.

Add the exact archive and receipt to the corresponding replacement policy:

```json
{
  "kind": "restore",
  "archive": "checkpoints/CLAIM.tar",
  "sha256": "ACTUAL_ARCHIVE_SHA256",
  "receipt": "checkpoints/CLAIM.receipt.json"
}
```

Paths resolve relative to the replacement configuration. A receipt binds the
archive to its original deployment, node, volume and owning service; mismatches
are refused during planning. Owner-produced consistent archives without this
platform receipt remain supported by the original restore interface.

A checkpoint preserves state **only through its capture time**. If a source
restarts and writes more data, that older checkpoint does not protect the new
state. This command is not continuous replication, a live source HA solution,
or proof that all reference/accepted events survive a later failure. Select a
restore point and reconcile its history with ingestion/canonical data explicitly.
Live SQLite files must not be archived by a generic file copy while changing.

Focused checks use a real SQLite database with committed uncheckpointed WAL,
restore its snapshot through the production restore helper, and verify contents
and integrity. They also check unsafe-file/corrupt-database refusal, source-only
planning, refusal of live writers and receipt identity/hash binding. No VM or
live cluster was accessed by those checks.
