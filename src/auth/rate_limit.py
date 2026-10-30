"""OTP auth-scoped rate limiting (F1).

Reuses the shared token-bucket mechanism in src.ratelimit (Redis-backed, with a bounded
per-process fallback). Controlled ONLY by the otp_rate_limit_* settings; it never reads or
touches the gateway rate_limit_* flags. Inert unless otp_rate_limit_enabled AND a policy is
configured.

Keys carry only the authenticated user id (UUID) or the direct peer IP - never a phone
number, email, or OTP code. Logs carry a truncated hash of the key only.
"""
from __future__ import annotations

import structlog
from fastapi import HTTPException, Request, status

from src.config import get_settings
from src.ratelimit import Policy, _client_ip, _hash_key, _limit

_log = structlog.get_logger("otp_ratelimit")


def _policy(capacity, refill) -> Policy | None:
    if capacity is None or refill is None or capacity <= 0 or refill < 0:
        return None
    return Policy(float(capacity), float(refill))


async def _evaluate_otp(request: Request, *, policy: "Policy | None", key: str, key_kind: str) -> None:
    s = get_settings()
    if not s.otp_rate_limit_enabled or policy is None:
        return                       # inert unless enabled AND configured
    dec = await _limit(request, policy, key)
    if dec.allowed:
        return
    if s.otp_rate_limit_enforce:
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS, "rate limit exceeded",
            headers={"Retry-After": str(max(1, int(dec.retry_after + 0.999)))})
    _log.info("otp_ratelimit_shadow_block", key_kind=key_kind,
              key_hash=_hash_key(key), retry_after=round(dec.retry_after, 3))


# ---- /otp/request ----
async def otp_request_ip_rate_limit(request: Request) -> None:
    s = get_settings()
    await _evaluate_otp(
        request, policy=_policy(s.otp_request_ip_capacity, s.otp_request_ip_refill_per_sec),
        key=f"rl:otp:req:ip:{_client_ip(request)}", key_kind="ip")


async def otp_request_user_limit(request: Request, user_id) -> None:
    s = get_settings()
    await _evaluate_otp(
        request, policy=_policy(s.otp_request_user_capacity, s.otp_request_user_refill_per_sec),
        key=f"rl:otp:req:uid:{user_id}", key_kind="uid")


# ---- /otp/verify ----
async def otp_verify_ip_rate_limit(request: Request) -> None:
    s = get_settings()
    await _evaluate_otp(
        request, policy=_policy(s.otp_verify_ip_capacity, s.otp_verify_ip_refill_per_sec),
        key=f"rl:otp:vrf:ip:{_client_ip(request)}", key_kind="ip")


async def otp_verify_user_limit(request: Request, user_id) -> None:
    s = get_settings()
    await _evaluate_otp(
        request, policy=_policy(s.otp_verify_user_capacity, s.otp_verify_user_refill_per_sec),
        key=f"rl:otp:vrf:uid:{user_id}", key_kind="uid")
