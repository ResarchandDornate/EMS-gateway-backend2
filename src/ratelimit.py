"""Rate limiting (shadow-first; enforcement OFF by default).

Implements the reviewed rate-limiting MECHANISM only. It changes no authentication,
scope-validation, idempotency, or ingestion semantics. It is inert unless explicitly
configured:

  * rate_limit_enabled = False  -> dependencies are a hard no-op (no Redis, no log).
  * rate_limit_enforce = False  -> SHADOW mode: evaluate + count + log would-be
                                   denials, but NEVER block and NEVER return 429.
  * a policy capacity/refill is None or capacity <= 0 -> that policy is inert even when
    enabled. NO numeric value here is approved operational policy; all are placeholders
    pending gateway/ops confirmation.

Keys:
  * /token  -> source IP only (direct peer). Request-body identity is untrusted and is
    never used as a key. X-Forwarded-For is NOT trusted (this repo establishes no
    trusted-proxy handling); behind a proxy the key is the proxy address - a documented
    limitation - with a safe constant fallback when the peer address is unavailable.
  * /ingest -> keyed on the AUTHENTICATED gateway id. The dependency runs AFTER
    authentication (it depends on get_current_gateway), so a missing/invalid/disabled
    credential returns the existing 401/403 BEFORE any limiting - unauthenticated
    requests are never rate-limited and auth semantics are unchanged.

Global-limit note: only the Redis token bucket (atomic single-EVAL Lua) enforces a
GLOBAL limit. The process-local fallback used when Redis is unavailable is PER-WORKER
and therefore DOES NOT enforce a global limit (see LocalTokenBucket) - it is a
degradation-mode circuit breaker only, with bounded memory.

Operational prerequisite (NOT assumed or provisioned here): a reachable, correctly
sized and - for multi-instance deployments - HIGHLY AVAILABLE Redis is required for the
global limit to hold. This module introduces no Redis HA assumption.

Privacy: logs carry only route, a coarse key-kind, and a truncated SHA-256 of the key.
Never raw IPs, client secrets, connection URLs, tokens, Authorization headers, or bodies.
"""
from __future__ import annotations

import asyncio
import hashlib
import time
from collections import OrderedDict
from dataclasses import dataclass

import structlog
from fastapi import Depends, HTTPException, Request, status

from src.config import get_settings
from src.gateway.dependencies import get_current_gateway

_log = structlog.get_logger("ratelimit")

# Atomic token bucket in a single EVAL -> race-free across workers/instances.
_LUA_TOKEN_BUCKET = """
local cap    = tonumber(ARGV[1])
local refill = tonumber(ARGV[2])
local now    = tonumber(ARGV[3])
local ttl    = tonumber(ARGV[4])
local d = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(d[1])
local ts = tonumber(d[2])
if tokens == nil then tokens = cap; ts = now end
local delta = now - ts
if delta < 0 then delta = 0 end
tokens = math.min(cap, tokens + delta * refill)
local allowed = 0
local retry = 0.0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  if refill > 0 then retry = (1 - tokens) / refill else retry = ttl end
end
redis.call('HMSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', KEYS[1], ttl)
return {allowed, tostring(tokens), tostring(retry)}
"""


@dataclass(frozen=True)
class Policy:
    capacity: float
    refill_per_sec: float

    @property
    def ttl(self) -> int:
        if self.refill_per_sec > 0:
            return max(1, int(self.capacity / self.refill_per_sec) + 1)
        return 60


@dataclass(frozen=True)
class Decision:
    allowed: bool
    remaining: float
    retry_after: float


class RedisTokenBucket:
    """Global, atomic token bucket backed by Redis (one EVAL per check)."""

    def __init__(self, client):
        self._c = client

    async def consume(self, key: str, policy: Policy, now: float) -> Decision:
        res = await self._c.eval(
            _LUA_TOKEN_BUCKET, 1, key,
            policy.capacity, policy.refill_per_sec, now, policy.ttl,
        )
        return Decision(bool(int(res[0])), float(res[1]), float(res[2]))

    async def aclose(self) -> None:
        try:
            await self._c.aclose()
        except Exception:
            pass


class LocalTokenBucket:
    """PER-PROCESS fallback bucket - DEGRADATION MODE ONLY, with bounded memory.

    Lets one worker bound its own load when Redis is unreachable. It is NOT shared
    across workers/instances, so with W processes the aggregate allowance is ~ W x
    capacity: it DOES NOT enforce a global limit and must never be presented as one.

    Memory is bounded two ways (concurrency-safe under a single asyncio.Lock):
      * idle expiry - entries untouched for >= idle_seconds are dropped (0 disables);
      * hard cap   - at most max_keys entries; the least-recently-used are evicted.
    """

    def __init__(self, max_keys: int = 10_000, idle_seconds: float = 300.0) -> None:
        self._state: "OrderedDict[str, tuple[float, float]]" = OrderedDict()
        self._lock = asyncio.Lock()
        self._max_keys = max(1, int(max_keys))
        self._idle_seconds = float(idle_seconds)

    def set_bounds(self, max_keys: int, idle_seconds: float) -> None:
        self._max_keys = max(1, int(max_keys))
        self._idle_seconds = float(idle_seconds)

    def _evict_idle(self, now: float) -> None:
        if self._idle_seconds <= 0:
            return
        cutoff = now - self._idle_seconds
        stale = []
        for k, (_tok, ts) in self._state.items():   # oldest-access first (LRU order)
            if ts < cutoff:
                stale.append(k)
            else:
                break
        for k in stale:
            del self._state[k]

    async def consume(self, key: str, policy: Policy, now: float) -> Decision:
        async with self._lock:
            self._evict_idle(now)
            tokens, ts = self._state.get(key, (policy.capacity, now))
            delta = max(0.0, now - ts)
            tokens = min(policy.capacity, tokens + delta * policy.refill_per_sec)
            if tokens >= 1:
                tokens -= 1
                allowed, retry = True, 0.0
            else:
                allowed = False
                retry = (1 - tokens) / policy.refill_per_sec if policy.refill_per_sec > 0 else float(policy.ttl)
            self._state[key] = (tokens, now)
            self._state.move_to_end(key)               # mark most-recently-used
            while len(self._state) > self._max_keys:
                self._state.popitem(last=False)        # evict least-recently-used
            return Decision(allowed, tokens, retry)


# One per-worker fallback instance (bounds refreshed from settings at use time).
_local_fallback = LocalTokenBucket()


async def init_rate_limiter(app) -> None:
    """Lifespan startup: build the shared Redis limiter iff enabled. When disabled
    (default) sets app.state.rate_limiter = None and touches no network. Never raises:
    a limiter-setup failure is logged (exc_type only - no URL/secret) and the app starts
    fail-open (per-process fallback only)."""
    s = get_settings()
    app.state.rate_limiter = None
    if not (s.rate_limit_enabled or s.otp_rate_limit_enabled):
        return
    try:
        from redis.asyncio import Redis
        client = Redis.from_url(
            s.redis_url,
            socket_connect_timeout=s.rate_limit_redis_connect_timeout_seconds,
            socket_timeout=s.rate_limit_redis_socket_timeout_seconds,
            encoding="utf-8",
            decode_responses=True,
        )
        app.state.rate_limiter = RedisTokenBucket(client)
    except Exception as exc:
        # Observable, but never leak the URL/credentials: exc_type only.
        _log.warning("ratelimit_redis_init_failed", exc_type=type(exc).__name__)
        app.state.rate_limiter = None


async def shutdown_rate_limiter(app) -> None:
    rl = getattr(app.state, "rate_limiter", None)
    if rl is not None:
        await rl.aclose()


def _hash_key(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8", "replace")).hexdigest()[:16]


def _client_ip(request: Request) -> str:
    # X-Forwarded-For is deliberately NOT trusted (no established trusted-proxy config);
    # trusting it would let clients spoof the key. Direct peer only; behind a proxy this
    # is the proxy IP (documented). Safe constant fallback when peer is unavailable.
    client = request.client
    if client and client.host:
        return client.host
    return "unknown"


def _policy_for(route: str, s) -> Policy | None:
    if route == "token":
        cap, refill = s.token_rate_capacity, s.token_rate_refill_per_sec
    else:
        cap, refill = s.ingest_rate_capacity, s.ingest_rate_refill_per_sec
    # None or non-positive capacity, or (defensively) negative refill -> inert no-op.
    if cap is None or refill is None or cap <= 0 or refill < 0:
        return None
    return Policy(float(cap), float(refill))


async def _limit(request: Request, policy: Policy, key: str) -> Decision:
    """Bounded check: a slow/failed Redis must NOT hang the request. On any Redis error
    or timeout, degrade to the per-process fallback (which is NOT a global limit)."""
    s = get_settings()
    rl = getattr(request.app.state, "rate_limiter", None)
    now = time.time()
    if rl is not None:
        try:
            return await asyncio.wait_for(
                rl.consume(key, policy, now),
                timeout=s.rate_limit_redis_timeout_seconds,
            )
        except Exception:
            _log.warning("ratelimit_degraded", reason="redis_unavailable")
    _local_fallback.set_bounds(
        s.rate_limit_local_fallback_max_keys, s.rate_limit_local_fallback_idle_seconds)
    return await _local_fallback.consume(key, policy, now)


async def _evaluate(request: Request, *, route: str, key: str, key_kind: str) -> None:
    s = get_settings()
    if not s.rate_limit_enabled:
        return                               # hard no-op (default)
    policy = _policy_for(route, s)
    if policy is None:
        return                               # unconfigured / inert policy -> no-op
    dec = await _limit(request, policy, key)
    if dec.allowed:
        return
    if s.rate_limit_enforce:                  # OFF by default; not enabled in this task
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            "rate limit exceeded",
            headers={"Retry-After": str(max(1, int(dec.retry_after + 0.999)))},
        )
    # SHADOW: never block. Privacy-conscious, low-cardinality event.
    _log.info("ratelimit_shadow_block", route=route, key_kind=key_kind,
              key_hash=_hash_key(key), retry_after=round(dec.retry_after, 3))


async def token_rate_limit(request: Request) -> None:
    """/token dependency: key on source IP (never on untrusted request-body identity)."""
    await _evaluate(request, route="token",
                    key=f"rl:token:ip:{_client_ip(request)}", key_kind="ip")


async def ingest_rate_limit(request: Request, gw=Depends(get_current_gateway)) -> None:
    """/ingest dependency: runs AFTER authentication (depends on get_current_gateway), so
    missing/invalid/disabled credentials return the existing 401/403 BEFORE any limiting;
    unauthenticated requests are never rate-limited and auth semantics are unchanged. Keyed
    on the authenticated gateway id (never a client-supplied value). Still runs before the
    ingest gate (503) and the ledger claim."""
    await _evaluate(request, route="ingest",
                    key=f"rl:ingest:gw:{gw.id}", key_kind="gateway")
