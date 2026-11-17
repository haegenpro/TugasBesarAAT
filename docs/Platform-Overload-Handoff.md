# Priority admission handoff

Status: implemented with focused local checks; physical load execution deferred. The feature is opt-in for existing
local environments and enabled in the Kubernetes Client API catalog.

| Setting | Kubernetes value | Meaning |
| --- | --- | --- |
| `CLIENT_API_PRIORITY_ENABLED` | `true` | Authenticate before protected-request admission |
| `CLIENT_API_MAX_INFLIGHT` | `128` | All protected requests per API instance |
| `CLIENT_API_FIELD_RESERVED` | `32` | Capacity unavailable to Media/internal |
| `CLIENT_API_MAX_INFLIGHT_PER_CLIENT` | `32` | Existing per-client bound, across sessions/routes |
| `CLIENT_API_AUTH_MAX_INFLIGHT` | `32` | Separate bounds for login/refresh/logout and token introspection |
| `CLIENT_API_AUTH_FIELD_RESERVED` | `8` | Reserved token-check/refresh/logout capacity for signed Field hints |
| `AUTH_MAX_INFLIGHT` / `AUTH_FIELD_RESERVED` | `32` / `8` | Auth itself separates general and Field admission |
| `DB_MAX_OPEN_CONNS` / `AUTH_DB_FIELD_RESERVED` | `16` / `4` | Auth splits its existing connection budget into 12 general and 4 Field connections |

Media and internal requests together can occupy at most 96 protected-request
slots; authenticated `field` requests can use any free slot up to 128 total.
Reject immediately with 429 and Retry-After, releasing counts on completion,
error or panic. Unknown roles fail closed. Only Auth's validated principal
selects the role; headers and query parameters cannot claim the field reserve.
Metrics distinguish role-capacity rejections and authentication-capacity
rejections. Limits apply to each process, so scaling changes total capacity.

Auth can now issue access and refresh tokens with an HMAC-signed scheduling hint.
Use one distinct `AUTH_PRIORITY_KEY` of at least 32 bytes in both Auth and Client
runtime Secrets, as shown in `deployment/services/auth-priority.env.example`.
The hint only selects reserved admission, HTTP transport and database connections:
every introspection still queries the live sessions table. Expiry, rotation and
logout remain authoritative database checks. Spoofed role headers, tampered hints,
refresh tokens presented as access tokens and expired hints cannot select the
reserved pre-authentication lane. Auth only prioritizes login after validating
the configured client credentials. Client login keeps a separate bounded pool.

Client introspection/refresh/logout and Auth each keep bounded general and Field
lanes. Auth's two SQL pools together retain the original total connection budget;
Media cannot occupy the reserved Field connections. Existing opaque tokens remain
valid but use general pre-authentication capacity until Auth issues/refreshed them
with a hint. Coordinate the shared key across replicas. Key rotation changes hint
classification without bypassing live session validation. There is no token cache.

These reservations protect admitted requests and connections. Physical database
CPU/I/O, table locks, network and canonical read capacity remain shared resources
requiring target-host overload measurements. A revoked Field token can retain a
valid scheduling hint until its signed expiry, but the live DB check still rejects
it. Initial login at the public Client API has no prevalidated role and uses the
general login bound.

During later validation, fill Media capacity while issuing authenticated field
requests; report both roles' successful throughput, latency, 429, unexpected
errors and authentication failures. Also saturate login/introspection and shared
upstreams, and confirm health remains responsive. The M2 evidence runner must
retain these limitations if field progress fails.

Critical coastal delivery must retain its own bounded workers, broker capacity
and recipient acknowledgements independent of the canonical audit gate.
Per-source faulty-sensor isolation and critical-operation delivery remain the
ingestion owner's integration, not effects of this HTTP admission policy.

Focused HTTP integration now verifies the actual Client API router against a
local Auth test server and a saturated general-read upstream: Media gets 429,
spoofed role headers do not claim the reserve, verified field reads continue,
and released slots are reusable. This supplements the middleware/per-client
regressions. `scripts/test-priority-integration.py` additionally uses actual
PostgreSQL to occupy the general Auth connection and verifies continuing Field
introspection/rotation/logout, revocation checks and unchanged total connection
budget. Its Client API HTTP check fills general introspection, rejects tampered
hints and confirms that a live Auth rejection is never cached away. Successful
run `evidence/platform-priority/0866eb4ad46d`: Auth 4 passed, Client API 15 passed,
no failures/skips. Physical database/network saturation remains deferred.
