# Observability watchlist — Central EMS backend

Covers the structured events/endpoints that **actually exist** in the code today
(`src/middleware.py`, `src/ratelimit.py`, `src/main.py`). Logging uses `structlog`
(`src/logging.py`): console renderer when `env == "dev"`, JSON otherwise. **No metrics backend,
dashboard, or alert is implemented** — this is a list of what to watch, not an assertion that any
monitoring exists.

**Never logged (verified by the current code, keep it that way):** secrets, JWTs/tokens, raw
`Authorization` headers, request/response bodies, query strings, cookies, exception messages or
tracebacks. The rate-limit key is logged only as a **truncated SHA-256** (`key_hash`), never a raw
IP or gateway id.

## Request/response (middleware)
- **`access`** (`src/middleware.py`, info) — one per HTTP request. Fields: `request_id`,
  `method`, `path`, `status`, `duration_ms`. Watch: error-status rates, latency, and that
  `X-Request-ID` appears on responses (confirms `asgi_app` is the served entrypoint).
- **`unhandled_exception`** (error) — emitted once when the app raises, then re-raised so the
  framework still renders the 500. Fields: `request_id`, `method`, `path`, `exc_type` only.
  Watch: any occurrence is a real server error — correlate by `request_id`. Must stay free of
  message/args/traceback.

## Rate limiter (shadow-only today; OFF by default)
- **`ratelimit_shadow_block`** (`src/ratelimit.py`, info) — a request that **would** be denied if
  enforcement were on. Fields: `route`, `key_kind` (`ip` / `gateway` / `badauth_ip`), `key_hash`
  (truncated SHA-256), `retry_after`. **Expected in shadow mode:** these may appear, but **no 429
  is returned and no request is blocked**. Use the volume/shape to size real limits later. If you
  ever see a 429 while `rate_limit_enforce=False`, that is a defect.
- **`ratelimit_degraded`** (warning) — Redis was unreachable/slow for a check and the limiter fell
  back to the **per-process** bucket. Field: `reason`. Watch: sustained occurrences mean the
  shared/global limit is not in effect (degraded, per-worker only). Not security-sensitive, but a
  Redis-health signal.
- **`ratelimit_redis_init_failed`** (warning) — the shared Redis limiter could not be built at
  startup; the app continues fail-open. Field: `exc_type` only (no URL/credentials). Watch: should
  not appear while the limiter is disabled; if it does once enabled, limiting is degraded.

## Readiness / liveness
- **`/ready`** (`src/main.py`) — JSON `{status, postgres, redis, influx}`, `200` when all `ok`
  else `503`; each dependency check is bounded (`READY_PROBE_TIMEOUT_SECONDS = 3`) and reports
  `error: <ExcType>` on failure (no secrets). Watch: `503`, or any dependency flipping to `error`.
- **`/health`** — static `{status, service}`; liveness only, no dependency checks.

## Explicitly NOT present (do not assume)
No Prometheus/StatsD metrics, no tracing exporter, no pre-built dashboard or alert rules exist in
the repo. Any such wiring is future work and must not be implied as done.
