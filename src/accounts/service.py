from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
import secrets

from src.auth.security import hash_password
from src.tenancy.gateways.models import GatewayModel

from src.auth.models import AdminAuditModel, UserModel
from src.rbac.models import RoleModel, UserRoleModel, UserSiteAccessModel
from src.rbac.service import seed_org_permissions
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel


class ConflictError(Exception):
    """A duplicate-key conflict that must surface as HTTP 409."""
    def __init__(self, detail: str):
        self.detail = detail
        super().__init__(detail)


def _is_unique_violation(exc: IntegrityError) -> bool:
    """True iff exc is a Postgres UNIQUE violation (SQLSTATE 23505). Uses only the
    SQLSTATE the asyncpg adapter copies onto exc.orig (.sqlstate/.pgcode)."""
    orig = getattr(exc, "orig", None)
    code = getattr(orig, "sqlstate", None) or getattr(orig, "pgcode", None)
    return code == "23505"


def _unique_constraint_name(exc: IntegrityError) -> str | None:
    """The exact violated UNIQUE constraint name (lowercased) for a 23505 error, or None.

    The SQLAlchemy asyncpg dialect raises its wrapper with `raise translated_error from
    error`, so the ORIGINAL asyncpg error - which carries `.constraint_name` - is
    reachable through the cause/context chain. Returns None if it is not a unique
    violation OR the name cannot be recovered; callers then degrade safely (generic 409
    or re-raise), never guessing which key collided."""
    if not _is_unique_violation(exc):
        return None
    orig = getattr(exc, "orig", None)
    for obj in (orig,
                getattr(orig, "__cause__", None),
                getattr(orig, "__context__", None),
                getattr(exc, "__cause__", None)):
        name = getattr(obj, "constraint_name", None)
        if name:
            return name.lower()
    return None


async def get_role(session, name):
    result = await session.execute(select(RoleModel).where(RoleModel.name == name))
    return result.scalar_one()


async def org_key_exists(session, org_key):
    result = await session.execute(
        select(OrganizationModel).where(OrganizationModel.org_key == org_key)
    )
    return result.scalar_one_or_none() is not None


async def phone_exists(session, phone):
    result = await session.execute(select(UserModel).where(UserModel.phone == phone))
    return result.scalar_one_or_none() is not None


async def create_client(session, actor_id, data):
    try:
        # 1. organization
        org = OrganizationModel(org_key=data.org_key, name=data.name)
        session.add(org)
        await session.flush()

        # 2. give the org its permission rows at safe defaults
        await seed_org_permissions(session, org.id)

        # 3. org admin user (phone login, no password)
        admin = UserModel(
            phone=data.admin_phone,
            email=data.admin_email,
            organization_id=org.id,
        )
        session.add(admin)
        await session.flush()

        # 4. assign org_admin role
        role = await get_role(session, "org_admin")
        session.add(UserRoleModel(user_id=admin.id, role_id=role.id))

        # 5. sites for this org (keys normalized by the schema; intra-request dups are
        #    rejected in the router before this runs)
        sites = []
        for s in data.sites:
            site = SiteModel(organization_id=org.id, site_key=s.site_key, name=s.name)
            session.add(site)
            sites.append(site)
        await session.flush()

        # 6. audit
        session.add(AdminAuditModel(
            actor_user_id=actor_id,
            action="client_onboarded",
            target_type="organization",
            target_id=org.id,
            new_value={
                "org_key": org.org_key,
                "name": org.name,
                "admin_phone": admin.phone,
                "sites": [s.site_key for s in sites],
            },
        ))

        await session.commit()
        return org, admin, sites
    except IntegrityError as e:
        await session.rollback()                     # clean the session BEFORE reuse/return
        if _is_unique_violation(e):
            name = _unique_constraint_name(e) or ""
            if "org_key" in name:
                raise ConflictError("Organization key already exists")
            if "site_key" in name:
                raise ConflictError("Site key already exists in this organization")
            if "phone" in name:
                raise ConflictError("Phone number already registered")
            if "email" in name:
                raise ConflictError("Email already registered")
            raise ConflictError("Duplicate key conflict")   # unique but unnamed -> safe generic 409
        raise                                                # not a unique violation -> surface


async def list_clients(session):
    result = await session.execute(select(OrganizationModel))
    return result.scalars().all()


async def get_sites(session, org_id):
    result = await session.execute(
        select(SiteModel).where(SiteModel.organization_id == org_id)
    )
    return result.scalars().all()


async def get_operators(session, org_id):
    result = await session.execute(
        select(UserModel)
        .join(UserRoleModel, UserRoleModel.user_id == UserModel.id)
        .join(RoleModel, RoleModel.id == UserRoleModel.role_id)
        .where(UserModel.organization_id == org_id)
        .where(RoleModel.name == "site_operator")
    )
    return result.scalars().all()



MAX_ACTIVE_OPERATORS = 2


async def org_site_ids(session, org_id):
    result = await session.execute(
        select(SiteModel.id).where(SiteModel.organization_id == org_id)
    )
    return set(result.scalars())


async def count_active_operators(session, org_id):
    result = await session.execute(
        select(func.count())
        .select_from(UserModel)
        .join(UserRoleModel, UserRoleModel.user_id == UserModel.id)
        .join(RoleModel, RoleModel.id == UserRoleModel.role_id)
        .where(UserModel.organization_id == org_id)
        .where(RoleModel.name == "site_operator")
        .where(UserModel.is_active.is_(True))
    )
    return result.scalar_one()


async def operator_site_ids(session, user_id):
    result = await session.execute(
        select(UserSiteAccessModel.site_id).where(UserSiteAccessModel.user_id == user_id)
    )
    return list(result.scalars())


async def get_operator_in_org(session, org_id, operator_id):
    result = await session.execute(
        select(UserModel)
        .join(UserRoleModel, UserRoleModel.user_id == UserModel.id)
        .join(RoleModel, RoleModel.id == UserRoleModel.role_id)
        .where(UserModel.id == operator_id)
        .where(UserModel.organization_id == org_id)
        .where(RoleModel.name == "site_operator")
    )
    return result.scalar_one_or_none()


async def create_operator(session, org_admin, data):
    org_id = org_admin.organization_id

    # lock the org row so two concurrent creates can't both pass the cap
    await session.execute(
        select(OrganizationModel).where(OrganizationModel.id == org_id).with_for_update()
    )

    if await count_active_operators(session, org_id) >= MAX_ACTIVE_OPERATORS:
        return None, "limit"

    if not set(data.site_ids).issubset(await org_site_ids(session, org_id)):
        return None, "bad_site"

    if data.phone and await phone_exists(session, data.phone):
        return None, "phone"

    try:
        operator = UserModel(
            phone=data.phone,
            email=data.email,
            organization_id=org_id,
            is_active=True,
        )
        session.add(operator)
        await session.flush()

        role = await get_role(session, "site_operator")
        session.add(UserRoleModel(user_id=operator.id, role_id=role.id))

        for site_id in data.site_ids:
            session.add(UserSiteAccessModel(user_id=operator.id, site_id=site_id))

        session.add(AdminAuditModel(
            actor_user_id=org_admin.id,
            action="operator_created",
            target_type="user",
            target_id=operator.id,
            new_value={"phone": data.phone, "sites": [str(s) for s in data.site_ids]},
        ))

        await session.commit()
        return operator, None
    except IntegrityError as e:
        await session.rollback()
        name = _unique_constraint_name(e) or ""
        if "email" in name:                              # exact: never mislabels email as phone
            return None, "email"
        if "phone" in name:
            return None, "phone"
        raise                                            # unnamed / unrelated -> surface


async def deactivate_operator(session, org_admin, operator_id):
    operator = await get_operator_in_org(session, org_admin.organization_id, operator_id)
    if operator is None:
        return "not_found"
    if not operator.is_active:
        return "already_inactive"

    operator.is_active = False
    session.add(AdminAuditModel(
        actor_user_id=org_admin.id,
        action="operator_deactivated",
        target_type="user",
        target_id=operator.id,
        old_value={"is_active": True},
        new_value={"is_active": False},
    ))
    await session.commit()
    return None



async def get_site(session, site_id):
    result = await session.execute(select(SiteModel).where(SiteModel.id == site_id))
    return result.scalar_one_or_none()


async def gateway_key_exists(session, gateway_key):
    result = await session.execute(
        select(GatewayModel).where(GatewayModel.gateway_key == gateway_key)
    )
    return result.scalar_one_or_none() is not None


async def _unique_client_id(session):
    while True:
        client_id = "gw_" + secrets.token_hex(16)
        exists = await session.execute(
            select(GatewayModel).where(GatewayModel.client_id == client_id)
        )
        if exists.scalar_one_or_none() is None:
            return client_id


async def register_gateway(session, actor, site, data):
    if await gateway_key_exists(session, data.gateway_key):
        return None, None, "duplicate"

    client_id = await _unique_client_id(session)         # UNCHANGED: server-generated
    client_secret = secrets.token_urlsafe(32)

    try:
        gateway = GatewayModel(
            site_id=site.id,
            gateway_key=data.gateway_key,
            client_id=client_id,
            client_secret_hash=hash_password(client_secret),
            software_version=data.software_version,
            enabled=True,
        )
        session.add(gateway)
        await session.flush()

        session.add(AdminAuditModel(
            actor_user_id=actor.id,
            action="gateway_registered",
            target_type="gateway",
            target_id=gateway.id,
            new_value={
                "gateway_key": gateway.gateway_key,
                "site_id": str(site.id),
                "client_id": client_id,
            },
        ))

        await session.commit()
        return gateway, client_secret, None
    except IntegrityError as e:
        await session.rollback()
        name = _unique_constraint_name(e) or ""
        if "gateway_key" in name:                        # ONLY the expected key -> duplicate
            return None, None, "duplicate"
        raise                                            # client_id collision / unnamed / other -> surface


async def list_gateways(session, site_id):
    result = await session.execute(
        select(GatewayModel).where(GatewayModel.site_id == site_id)
    )
    return result.scalars().all()    