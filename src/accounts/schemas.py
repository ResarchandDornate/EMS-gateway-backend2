import uuid

from pydantic import BaseModel, EmailStr


class SiteIn(BaseModel):
    site_key: str
    name: str


class OrganizationCreate(BaseModel):
    org_key: str
    name: str
    admin_phone: str
    admin_email: EmailStr | None = None
    sites: list[SiteIn] = []


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


class GatewayOut(BaseModel):
    id: uuid.UUID
    site_id: uuid.UUID
    gateway_key: str
    client_id: str
    enabled: bool


class GatewayCreated(GatewayOut):
    client_secret: str   # returned ONCE, at registration    