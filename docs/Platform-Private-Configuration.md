# Private release configuration

`platform-configure.py` generates a new, unused set of cross-service credentials,
six independent storage PKIs/password sets, application database DSNs and an
integrated practice configuration. It never contacts or changes a deployment.
The output must be outside the repository or under ignored `evidence/`; directories
are private and files use mode 0600. Existing output is refused to prevent silent
credential rotation. Keep this directory off Git and back it up privately.

```sh
python3 scripts/platform-configure.py \
  --inventory /private/four-hosts.json --output /private/release-inputs \
  --satellite-token-file /private/satellite-token
```

Optional file inputs are `--audit-token-file`, `--province-reference`,
`--images-file` and `--external-flows-file`. Token files must have private
permissions; token values never belong in shell arguments. The province table
defaults to the [38-row project reference](Province-Reference.md); use
`--province-reference` to override it. The instructor Audit token remains pending.
Missing image digests remain null, and missing
external credentials are absent Secret keys. `preparation.json` lists these
inputs without exposing their values. This scaffold is not activation approval.

The current local preparation is in
`evidence/platform-private/fourhost-release`, using the supplied private
satellite credential and example four-host placement. It has **not** been applied.
Replace inventory identities/capacity before deployment; regenerate into a new
directory if formation identities change before first use. Generated etcd leaf
certificates last 90 days; prepare a fresh unused set if deployment is delayed
past their validity. Existing deployed credentials require explicit rotation.

`secrets.json` contains TLS, administrator database passwords and application
Secrets. Application DSNs use restricted roles; APP and broker system recovery
accounts remain separate. `client-credentials.json` holds the three client
IDs/secrets for later login/load tests. It must remain private too. The source
namespaces/offsets are proposed identities: confirm them against the actual
session before first activation and preserve them across normal rollouts.

Use `platform-inputs.py` from the
[input-driven workflow](Platform-Input-Driven-Workflow.md) to fill missing files
and resolve their generated pending entries without rotating internal credentials.
Its maintenance runner resolves initialization gates from verified receipts.
Build/publish
real immutable images; never fill nulls with invented digests. Declare bounded
satellite egress CIDRs in `external-flows.json`; generic NetworkPolicy cannot
follow changing DNS addresses. Then use `platform-release.py` to assemble a new
reviewable bundle. Its optional `secrets` input adds a namespace/Secrets stage
before storage, and restricts repository-local output to ignored `evidence/`.
Offline review checks required keys even when the Secret is supplied in the
bundle. No report contains Secret values.

Storage bootstrap, administrator schema initialization, original identity capture,
canonical/ingestion migrations, BMKG cutover and real-node capacity/HA tests still
require execution. The full bundle retains these blockers. See
[release preparation](../deployment/platform/RELEASE.md) and the
[acceptance matrix](Platform-Acceptance-Plan.md).
