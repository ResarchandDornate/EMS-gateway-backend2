import secrets
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.models import OtpChallengeModel
from src.auth.security import hash_password, verify_password

OTP_LENGTH = 6
OTP_TTL_MINUTES = 5
OTP_MAX_ATTEMPTS = 5


def generate_code(length: int = OTP_LENGTH) -> str:
    return "".join(secrets.choice("0123456789") for _ in range(length))


async def create_challenge(session: AsyncSession, *, user_id, channel: str, destination: str) -> str:
    code = generate_code()
    session.add(OtpChallengeModel(
        user_id=user_id, channel=channel, destination=destination,
        otp_hash=hash_password(code),
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=OTP_TTL_MINUTES)))
    await session.flush()
    return code


async def verify_code(session: AsyncSession, *, user_id, code: str) -> bool:
    now = datetime.now(timezone.utc)
    stmt = (select(OtpChallengeModel)
            .where(OtpChallengeModel.user_id == user_id,
                   OtpChallengeModel.consumed_at.is_(None),
                   OtpChallengeModel.expires_at > now)
            .order_by(OtpChallengeModel.created_at.desc())
            .with_for_update().limit(1))
    ch = (await session.execute(stmt)).scalar_one_or_none()
    if ch is None or ch.attempts >= OTP_MAX_ATTEMPTS:
        return False
    ch.attempts += 1
    if not verify_password(code, ch.otp_hash):
        await session.flush()
        return False
    ch.consumed_at = now          # single-use
    await session.flush()
    return True
async def lock_user(session, user_id) -> None:
    """Transaction-scoped per-user advisory lock. Serializes OTP request/verify for one
    user so the F2 cumulative-attempt count is exact under concurrency. Auto-released on
    commit/rollback."""
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:k, 0))"), {"k": str(user_id)})


async def recent_attempt_total(session, user_id, window_seconds: int) -> int:
    """Cumulative verification attempts for a user across ALL challenges created within the
    rolling window. Because attempts accrue per verify regardless of which challenge is
    newest, requesting a new challenge cannot reset this total (F2)."""
    since = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
    stmt = (select(func.coalesce(func.sum(OtpChallengeModel.attempts), 0))
            .where(OtpChallengeModel.user_id == user_id,
                   OtpChallengeModel.created_at > since))
    return int((await session.execute(stmt)).scalar_one())


async def supersede_prior_challenges(session, user_id) -> None:
    """Invalidate the user's outstanding unconsumed challenges so only the newest code is
    live. Attempts history is preserved (counted by recent_attempt_total)."""
    await session.execute(
        update(OtpChallengeModel)
        .where(OtpChallengeModel.user_id == user_id, OtpChallengeModel.consumed_at.is_(None))
        .values(consumed_at=datetime.now(timezone.utc)))
