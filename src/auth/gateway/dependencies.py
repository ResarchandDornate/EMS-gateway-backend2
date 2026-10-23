import uuid

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.security import decode_gateway_token
from src.database import get_session
from src.gateway import service

_bearer = HTTPBearer(auto_error=False)


async def get_current_gateway(
    creds: HTTPAuthorizationCredentials | None = Depends(_bearer),
    session: AsyncSession = Depends(get_session),
):
    if creds is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Missing bearer token")
    sub = decode_gateway_token(creds.credentials)      # requires aud="gateway" + not expired
    if sub is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or expired gateway token")
    try:
        gid = uuid.UUID(sub)
    except ValueError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid gateway token subject")
    gw = await service.get_gateway_by_id(session, gid)
    if gw is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Gateway not found")
    if not gw.enabled:                                 # DB check on every request
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Gateway disabled")
    return gw