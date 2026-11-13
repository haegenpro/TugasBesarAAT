# Application isolation development draft

Implemented, unexecuted. `platform-isolation.py --output evidence/isolation.json`
renders namespace-scoped default-deny ingress/egress plus explicit DNS, service,
canonical formation, migration, broker, per-service database and monitoring
flows. It does not apply policies or claim CNI enforcement has been tested.

Application labels are `app.kubernetes.io/name` and `platform.aat/cluster`;
canonical PostgreSQL/routers/DCS use their formation/component labels. Canonical
database connections are limited to Aggregator, the offline migration job and
required formation peers. Consumers and Client API cannot directly connect.
Each external owner database permits only its owning application/init job and
its own replication peers. Database credentials still need distinct restricted
roles: NetworkPolicy is not SQL permission enforcement.

Owner contributions must use the catalog labels and provide additional scoped
flows for their processor database, recipient inboxes or actual external feeds.
An optional JSON list of `{app,cidr,ports}` adds bounded external egress; /0
rules and unspecified ports are rejected. Generic NetworkPolicy cannot target
DNS names. Do not silently add internet-wide access to make a missing owner flow
work. Private ClusterIP entry remains; explicit ingress/load-generator labels
are allowed to reach Client API. Service-account tokens remain unmounted on
application pods; monitoring receives a separate read-only discovery account.

Policies exist only in the required application namespace. The supplied Tester
Agent manifest/namespace/privileges are untouched. Its hostNetwork access and
the CNI's handling of host/node traffic must be verified during integration; do
not assume a policy render proves its fault controls remain unobstructed.

Deferred runtime matrix: allowed source->registry, Aggregator->canonical routing,
Citus/Patroni/DCS peer traffic, broker peers/clients, auth/consumer own databases,
DNS, metrics/traces and migration connectivity; forbidden consumer/client/source
connections to canonical and other services' databases; no unintended cross-
namespace access; Tester Agent still operates. Record actual CNI, credentials,
source/destination pod labels, ports and positive/negative results. Third-party
owner policies must be reviewed together because allow policies are additive.
