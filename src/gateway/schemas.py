import uuid

from pydantic import BaseModel


class GatewayTokenRequest(BaseModel):
    client_id: str
    client_secret: str


class GatewayTokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int          # seconds


class GatewayIdentity(BaseModel):
    gateway_id: uuid.UUID
    site_id: uuid.UUID
    organization_id: uuid.UUID | None
    enabled: bool
