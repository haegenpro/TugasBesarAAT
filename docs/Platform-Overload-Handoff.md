# Priority admission handoff

Status: implemented, execution deferred. The feature is opt-in for existing
local environments and enabled in the Kubernetes Client API catalog.

| Setting | Kubernetes value | Meaning |
| --- | --- | --- |
| `CLIENT_API_PRIORITY_ENABLED` | `true` | Authenticate before protected-request admission |
| `CLIENT_API_MAX_INFLIGHT` | `128` | All protected requests per API instance |
| `CLIENT_API_FIELD_RESERVED` | `32` | Capacity unavailable to Media/internal |
| `CLIENT_API_MAX_INFLIGHT_PER_CLIENT` | `32` | Existing per-client bound, across sessions/routes |
| `CLIENT_API_AUTH_MAX_INFLIGHT` | `32` | Separate bounds for login/refresh/logout and token introspection |

Media and internal requests together can occupy at most 96 protected-request
slots; authenticated `field` requests can use any free slot up to 128 total.
Reject immediately with 429 and Retry-After, releasing counts on completion,
error or panic. Unknown roles fail closed. Only Auth's validated principal
selects the role; headers and query parameters cannot claim the field reserve.
Metrics distinguish role-capacity rejections and authentication-capacity
rejections. Limits apply to each process, so scaling changes total capacity.

The reservation covers work **after authentication**. Login has its own pool;
introspection releases its slot before calling Aggregator. A Media flood can
still saturate the shared introspection path, Auth database, network or canonical
read pool. This change does not establish end-to-end Tim Lapangan progress under
every overload. Validate those bottlenecks and, if required, isolate field
ingress/Auth/read capacity with the relevant service owners. Do not introduce a
token cache that silently weakens logout/revocation semantics.

During later validation, fill Media capacity while issuing authenticated field
requests; report both roles' successful throughput, latency, 429, unexpected
errors and authentication failures. Also saturate login/introspection and shared
upstreams, and confirm health remains responsive. The M2 evidence runner must
retain these limitations if field progress fails.

Critical coastal delivery must retain its own bounded workers, broker capacity
and recipient acknowledgements independent of the canonical audit gate.
Per-source faulty-sensor isolation and critical-operation delivery remain the
ingestion owner's integration, not effects of this HTTP admission policy.
