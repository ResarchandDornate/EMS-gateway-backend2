import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.otp import create_challenge, verify_code
from src.auth.otp_provider import OtpDeliveryProvider, get_otp_provider
from src.auth.repository import UserRepository
from src.auth.schemas import (LoginRequest, OtpRequestIn, OtpRequestOut,
                              OtpVerifyIn, TokenResponse, UserMe)
from src.auth.security import create_access_token, decode_access_token
from src.auth.service import (OTP_ROLES, authenticate, get_role_names,
                             resolve_channel)
from src.database import get_session

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])
_bearer = HTTPBearer(auto_error=True)


# --- Super Admin: password ---
@router.post("/login", response_model=TokenResponse)
async def login(body: LoginRequest, session: AsyncSession = Depends(get_session)):
    user = await authenticate(session, body.identifier, body.password)
    if user is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid credentials")
    return TokenResponse(access_token=create_access_token(user.id))


# --- Org Admin / Operator: phone OTP ---
@router.post("/otp/request", response_model=OtpRequestOut)
async def otp_request(
    body: OtpRequestIn,
    session: AsyncSession = Depends(get_session),
    provider: OtpDeliveryProvider = Depends(get_otp_provider),
):
    user = await UserRepository(session).get_by_identifier(body.identifier)
    if user is None or not user.is_active:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not registered")
    if not (await get_role_names(session, user.id) & OTP_ROLES):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "OTP login is not allowed for this account")
    resolved = resolve_channel(body.identifier, user)
    if resolved is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "No phone number registered for this account")
    channel, destination = resolved
    if channel == "sms":                      # phone only for now; email later
        code = await create_challenge(
            session, user_id=user.id, channel=channel, destination=destination)
        await session.commit()                       # persist challenge BEFORE any SMS I/O
        await provider.send_sms(destination, code)   # network I/O runs with no open transaction
    return OtpRequestOut()


@router.post("/otp/verify", response_model=TokenResponse)
async def otp_verify(body: OtpVerifyIn, session: AsyncSession = Depends(get_session)):
    user = await UserRepository(session).get_by_identifier(body.identifier)
    if user is None or not user.is_active:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not registered")
    if not (await get_role_names(session, user.id) & OTP_ROLES):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "OTP login is not allowed for this account")
    ok = await verify_code(session, user_id=user.id, code=body.otp)
    await session.commit()                    # persist attempts (wrong code) or consumption (success)
    if not ok:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or expired OTP")
    return TokenResponse(access_token=create_access_token(user.id))


# --- shared: current user ---
async def get_current_user(
    creds: HTTPAuthorizationCredentials = Depends(_bearer),
    session: AsyncSession = Depends(get_session),
):
    sub = decode_access_token(creds.credentials)
    if sub is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or expired token")
    user = await UserRepository(session).get_by_id(uuid.UUID(sub))
    if user is None or not user.is_active:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "User not found or inactive")
    return user


@router.get("/me", response_model=UserMe)
async def me(user=Depends(get_current_user)):
    return UserMe(id=str(user.id), email=user.email, phone=user.phone,
                  organization_id=str(user.organization_id) if user.organization_id else None)