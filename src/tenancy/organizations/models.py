from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class OrganizationModel(BaseEntityModel):
    __tablename__ = "organizations"

    org_key: Mapped[str] = mapped_column(String(64), unique=True)
    name: Mapped[str] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(32), default="active", server_default="active")