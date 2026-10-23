from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.models import UserModel
from src.auth.repository import UserRepository
from src.auth.security import verify_password
from src.rbac.models import RoleModel, UserRoleModel

OTP_ROLES = {"org_admin", "site_operator"}


async def authenticate(session: AsyncSession, identifier: str, password: str) -> UserModel | None:
    user = await UserRepository(session).get_by_identifier(identifier)
    if user is None or not user.is_active or user.password_hash is None:
        return None
    if not verify_password(password, user.password_hash):
        return None
    return user


async def get_role_names(session: AsyncSession, user_id) -> set[str]:
    stmt = (select(RoleModel.name)
            .join(UserRoleModel, UserRoleModel.role_id == RoleModel.id)
            .where(UserRoleModel.user_id == user_id))
    return set((await session.execute(stmt)).scalars())


def resolve_channel(identifier: str, user):
    ident = identifier.strip()
    if "@" in ident:
        if user.email and user.email.lower() == ident.lower():
            return ("email", user.email)
        return None
    if user.phone and user.phone == ident:
        return ("sms", user.phone)
    return None
