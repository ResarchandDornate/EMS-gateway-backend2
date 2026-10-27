import uuid
from datetime import datetime

from pydantic import BaseModel, EmailStr, field_validator


class SiteIn(BaseModel):
    site_key: str
    name: str

    @field_validator("site_key")
    @classmethod
    def _norm_site_key(cls, v: str) -> str:
        return v.strip().lower()          # trim + lowercase (identity)

    @field_validator("name")
    @classmethod
    def _norm_site_name(cls, v: str) -> str:
        return v.strip()                  # display label: trim only


class OrganizationCreate(BaseModel):
    org_key: str
    name: str
    admin_phone: str
    admin_email: EmailStr | None = None
    sites: list[SiteIn] = []

    @field_validator("org_key")
    @classmethod
    def _norm_org_key(cls, v: str) -> str:
        return v.strip().lower()          # trim + lowercase (identity)

    @field_validator("name")
    @classmethod
    def _norm_org_name(cls, v: str) -> str:
        return v.strip()                  # display label: trim only


class SiteOut(BaseModel):
    id: uuid.UUID
    site_key: str
    name: str


class OperatorOut(BaseModel):
    id: uuid.UUID
    phone: str | None
    email: str | None


class OrganizationOut(BaseModel):
    id: uuid.UUID
    org_key: str
    name: str
    admin_user_id: uuid.UUID
    admin_phone: str
    sites: list[SiteOut]


class OrganizationDetail(BaseModel):
    id: uuid.UUID
    org_key: str
    name: str
    sites: list[SiteOut]
    operators: list[OperatorOut]


class OperatorCreate(BaseModel):
    phone: str
    email: EmailStr | None = None
    site_ids: list[uuid.UUID] = []


class OperatorDetail(BaseModel):
    id: uuid.UUID
    phone: str | None
    email: str | None
    is_active: bool
    site_ids: list[uuid.UUID]


class GatewayCreate(BaseModel):
    gateway_key: str
    software_version: str | None = None

    @field_validator("gateway_key")
    @classmethod
    def _norm_gateway_key(cls, v: str) -> str:
        return v.strip()                  # trim ONLY - case preserved (external id)


class GatewayOut(BaseModel):
    id: uuid.UUID
    site_id: uuid.UUID
    gateway_key: str
    client_id: str
    enabled: bool
    # Last SUCCESSFUL token issuance (interim liveness); NULL until the gateway first
    # obtains a token. Not telemetry time; not a live-connectivity guarantee.
    last_seen_at: datetime | None = None


class GatewayCreated(GatewayOut):
    client_secret: str   # returned ONCE, at registration
