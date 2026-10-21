import uuid

from sqlalchemy import Boolean, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models import Base, BaseEntityModel


class RoleModel(BaseEntityModel):
    __tablename__ = "roles"
    name: Mapped[str] = mapped_column(String(32), unique=True)   # super_admin / org_admin / site_operator


class PermissionModel(BaseEntityModel):
    __tablename__ = "permissions"
    name: Mapped[str] = mapped_column(String(64), unique=True)   # telemetry:read, command:write, ...


class OrganizationPermissionModel(BaseEntityModel):
    __tablename__ = "organization_permissions"
    __table_args__ = (UniqueConstraint("organization_id", "permission"),)

    organization_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        index=True,
    )
    permission: Mapped[str] = mapped_column(String(64))
    allowed: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    updated_by: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))


# --- junction tables: inherit Base, composite primary key (no id column) ---

class UserRoleModel(Base):
    __tablename__ = "user_roles"
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    role_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("roles.id", ondelete="CASCADE"), primary_key=True
    )


class RolePermissionModel(Base):
    __tablename__ = "role_permissions"
    role_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("roles.id", ondelete="CASCADE"), primary_key=True
    )
    permission_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("permissions.id", ondelete="CASCADE"), primary_key=True
    )


class UserSiteAccessModel(Base):
    __tablename__ = "user_site_access"
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    site_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("sites.id", ondelete="CASCADE"), primary_key=True
    )