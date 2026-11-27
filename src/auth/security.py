import asyncio
from datetime import datetime, timedelta, timezone

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import VerificationError, VerifyMismatchError

try:  # argon2-cffi >= 23.1.0
    from argon2.exceptions import InvalidHashError
except ImportError:  # older argon2-cffi
    from argon2.exceptions import InvalidHash as InvalidHashError

from src.config import get_settings

_ph = PasswordHasher()        # Argon2id
_ALGO = "HS256"
ACCESS_TOKEN_MINUTES = 60 * 24 * 12   # 12 days
GATEWAY_TOKEN_MINUTES = 30
USER_AUDIENCE = "user"
GATEWAY_AUDIENCE = "gateway"


def hash_password(password: str) -> str:
    return _ph.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return _ph.verify(password_hash, password)
    except (VerifyMismatchError, VerificationError, InvalidHashError):
        # wrong or malformed stored hash -> non-match. Unexpected (non-Argon2)
        # errors are NOT caught here; they propagate (generic 500, no internals leaked).
        return False


# --- Async wrappers (audit finding D-10): Argon2id hash/verify is deliberately CPU-heavy
# (~tens of ms). Called directly in an async handler it blocks the event loop, serialising
# ingest/reads on the same worker and amplifying a login/token flood into a DoS. These run
# the hash on a worker thread so the loop stays free. The sync versions are kept for tests
# and non-async callers (model setup, seed scripts). Behaviour is otherwise identical.
async def hash_password_async(password: str) -> str:
    return await asyncio.to_thread(hash_password, password)


async def verify_password_async(password: str, password_hash: str) -> bool:
    return await asyncio.to_thread(verify_password, password, password_hash)


def create_access_token(user_id) -> str:
    now = datetime.now(timezone.utc)
    payload = {"sub": str(user_id), "aud": USER_AUDIENCE, "iat": now,
               "exp": now + timedelta(minutes=ACCESS_TOKEN_MINUTES)}
    return jwt.encode(payload, get_settings().jwt_secret, algorithm=_ALGO)


def decode_access_token(token: str) -> str | None:
    try:
        payload = jwt.decode(token, get_settings().jwt_secret, algorithms=[_ALGO],
                             audience=USER_AUDIENCE)
    except jwt.PyJWTError:
        return None
    sub = payload.get("sub")
    return sub if sub else None


def create_gateway_token(gateway_id) -> str:
    now = datetime.now(timezone.utc)
    payload = {"sub": str(gateway_id), "aud": GATEWAY_AUDIENCE, "iat": now,
               "exp": now + timedelta(minutes=GATEWAY_TOKEN_MINUTES)}
    return jwt.encode(payload, get_settings().jwt_secret, algorithm=_ALGO)


def decode_gateway_token(token: str) -> str | None:
    try:
        payload = jwt.decode(token, get_settings().jwt_secret, algorithms=[_ALGO],
                             audience=GATEWAY_AUDIENCE)
    except jwt.PyJWTError:
        return None
    sub = payload.get("sub")
    return sub if sub else None
