from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.security import GATEWAY_TOKEN_MINUTES, create_gateway_token, verify_password
from src.database import get_session
from src.gateway import service
from src.gateway.dependencies import get_current_gateway
from src.gateway.schemas import GatewayIdentity, GatewayTokenRequest, GatewayTokenResponse

router = APIRouter(prefix="/api/v1/gateway", tags=["gateway"])


@router.post("/token", response_model=GatewayTokenResponse)
async def gateway_token(body: GatewayTokenRequest, session: AsyncSession = Depends(get_session)):
    gw = await service.get_gateway_by_client_id(session, body.client_id)
    # generic 401 for unknown client OR bad secret (do not reveal which)
    if gw is None or not verify_password(body.client_secret, gw.client_secret_hash):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid credentials")
    if not gw.enabled:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Gateway disabled")
    token = create_gateway_token(gw.id)
    return GatewayTokenResponse(access_token=token, expires_in=GATEWAY_TOKEN_MINUTES * 60)


@router.get("/me", response_model=GatewayIdentity)
async def gateway_me(gw=Depends(get_current_gateway), session: AsyncSession = Depends(get_session)):
    ident = await service.resolve_gateway_identity(session, gw)
    return GatewayIdentity(
        gateway_id=ident["gateway_id"], site_id=ident["site_id"],
        organization_id=ident["organization_id"], enabled=gw.enabled,
    )