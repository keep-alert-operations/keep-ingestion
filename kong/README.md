# Keep Kong policy plugin

`keep-policy/` is the complete Lua package for the Keep policy plugin, version
`1.0.0`. It complements open-source Kong Key Auth. Installation in a shared or
production Kong fleet is a separate, owner-authorized deployment; this repository
does not perform it.

## Package and migrate

The Kong team includes this directory at
`/usr/local/share/lua/5.1/kong/plugins/keep-policy` in its qualified image, or
mounts it read-only for isolated development. Set `KONG_PLUGINS=bundled,keep-policy`
on both migration and serving workloads. Run the installed Kong version's normal
`kong migrations up` / `kong migrations finish` workflow before starting the new
serving image. A fresh isolated Kong database also needs `kong migrations bootstrap`.
Do not bootstrap an existing shared database or replace its unrelated configuration.

The plugin migration adds Kong-owned `keep_policies` and a database trigger that
rejects lower policy generations and different documents at the same generation.
This is separate from Keep's database. Serving requests read Kong's policy
authority directly and fail closed if it is unavailable; they never query Keep
PostgreSQL. Policy documents contain no raw API keys. Key Auth retains its normal
credential storage. Retain denied identities/policy generations across recovery;
never restore an older policy authority into service without the recovery gate.

## Serving and proof routes

Use separate native-v2 and Grafana routes. Their pre-auth configuration rejects
ambiguous credential headers, strips client identity context, and adapts exactly
one Grafana Bearer credential to Key Auth. Native v2 accepts only `X-API-Key`.
Configure Key Auth with header-only credentials, `hide_credentials=true`, and no
anonymous fallback. The custom plugin runs afterward (priority `1200`); requalify
actual execution order against the installed Kong version.

The plugin replaces upstream identity headers and supplies operator, immutable
tenant, credential, policy generation, permitted routing names, and
`X-Keep-Authorized-At`. That timestamp is captured **before** policy evaluation.
Forward solely over the existing private pinned mTLS boundary. Ingress enforces
the bounded authorization lifetime used by the operator-deletion drain proof.

Provisioning uses two private, non-ingesting routes:

- `probe=true`: authenticate `X-Keep-Probe-Token` and evaluate the supplied
  operator/credential IDs with a nonce. Do not attach Key Auth to this route:
  revoked credentials have no retained plaintext and still require denial proof.
- `probe_active=true`: attach Key Auth and the custom plugin, and require the
  probe token. It verifies the actual key binding plus policy without forwarding
  an alert. Prepared keys must fail this check.

Restrict both routes to the provisioning proof service. Keep probe tokens in
restricted configuration and out of logs. Neither route admits customer alerts.
The operator binding names every serving member by `kong.node.get_id()`, not by
the legacy display-only `member_id` plugin setting. The proof service must contact
each member directly, with TLS validation, and aggregate exact nonce, generation,
policy digest, expiry, tenant, credential and decision results. A load-balanced
sample, control-plane readback, unavailable node or missing member is not proof.
Requalify membership when nodes restart or the serving pool changes.

## Clock evidence

By default every serving node reads `/run/keep/clock-health.json`; configure
`clock_health_file` when the owner mounts another path. The owning platform's
clock observer must refresh the file, using its actual measurement:

```json
{"measured_at":"2026-09-30T10:00:00.000000+00:00","synchronized":true,"offset_seconds":0.02}
```

Allow decisions require finite absolute offset at most one second and evidence
aged from minus one to sixty seconds. Missing, invalid, stale or unsynchronized
evidence fails closed. Expiring credentials compare their deadline with the
serving node's UTC clock; UTC does not remove the documented near-boundary clock
uncertainty. Revocation/disabled-policy denial remains effective without trusted
time. The example is schema documentation, not deployable current evidence.

`unqualified_local_clock=true` is an explicit isolated-development exception;
`scripts/local_v2.py` sets it only on its dedicated local routes. It is a plugin
configuration field, not an environment variable, and defaults to false. Do not
enable it in staging or production or use it as clock qualification evidence.

## Executed scope and remaining qualification

The local integration tests in `backend/tests/integration/test_kong_policy.py`
exercise the actual installed single-node Kong plugin: prepared denial, active
authentication, expiry, revocation without plaintext, monotonic generations, and
clock-health failure behavior. The local proof adapter targets only that one
isolated node. These checks do not qualify a company serving pool, node recovery,
real clock-observer feed, load capacity, networking or production deployment.
Record broader executed evidence in the delivery records before enabling those
capabilities.
