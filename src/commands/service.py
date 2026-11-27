"""Command lifecycle: authorize + create (UI), poll (gateway), close (S9 result).

The command-type -> role rule is the one authority for what each role may send.
super_admin / org_admin may send anything on the list; site_operator is held to the
stop / automation-disable subset. A type not on the list is refused -- the UI downlink
is a closed vocabulary even though S9 keeps command_type open.
"""
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import and_, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.commands.models import PendingCommandModel
from src.tenancy.sites.models import SiteModel

# How long a raised command stays deliverable before it lapses (gateway polls ~2s).
COMMAND_TTL_SECONDS = 120

# Once served, a command is not served again until this long has passed without an S9 result.
# The gateway de-duplicates on command_id, so this is a retry-on-silence back-off, not the
# delivery guarantee: at a 2s poll it turns ~60 re-serves per command into ~4.
RESERVE_AFTER_SECONDS = 30

# The full downlink vocabulary. Elevated roles may send any of these.
COMMAND_TYPES = frozenset({
    "pair_precheck", "pair_automatic_start", "pair_set_power", "pair_zero_power",
    "pair_safe_stop", "pair_abort", "safe_stop_all", "automation_enable", "automation_disable",
    "automation_set_schedule",
})

# The site_operator-safe subset: stops and automation-disable only.
_OPERATOR_COMMANDS = frozenset({
    "pair_zero_power", "pair_safe_stop", "pair_abort", "safe_stop_all", "automation_disable",
})

_POLLABLE = ("pending", "served")                 # still eligible for delivery on the poll
_TERMINAL = frozenset({"completed", "failed", "timeout", "rejected"})


def effective_role(roles: set[str]) -> str | None:
    """The highest-authority command role the user holds, or None if they hold none."""
    for r in ("super_admin", "org_admin", "site_operator"):
        if r in roles:
            return r
    return None


def may_send(role: str, command_type: str) -> bool:
    if command_type not in COMMAND_TYPES:
        return False
    if role in ("super_admin", "org_admin"):
        return True
    if role == "site_operator":
        return command_type in _OPERATOR_COMMANDS
    return False


async def create_command(session: AsyncSession, *, gateway_id, organization_id, site_id,
                         command_type, arguments, requires_readback, requested_by,
                         requested_role) -> PendingCommandModel:
    now = datetime.now(timezone.utc)
    cid = uuid.uuid4()
    row = PendingCommandModel(
        command_id=cid, gateway_id=gateway_id, organization_id=organization_id, site_id=site_id,
        command_type=command_type, arguments=arguments, requires_readback=requires_readback,
        requested_by=requested_by, requested_role=requested_role, correlation_id=cid.hex,
        issued_at_utc=now, expires_at_utc=now + timedelta(seconds=COMMAND_TTL_SECONDS),
        status="pending",
    )
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return row


async def poll_pending(session: AsyncSession, *, gateway_id, limit: int) -> list[PendingCommandModel]:
    """Expire the lapsed, return the still-deliverable, mark them served. A row is served when
    pending, then held back until RESERVE_AFTER_SECONDS pass with no S9 result before it is
    re-served -- so a delivered command is not re-sent on every 2s poll, only retried on silence.
    An S9 result (apply_results) or expiry takes it off the poll for good."""
    now = datetime.now(timezone.utc)
    await session.execute(
        update(PendingCommandModel)
        .where(PendingCommandModel.gateway_id == gateway_id,
               PendingCommandModel.status.in_(_POLLABLE),
               PendingCommandModel.expires_at_utc <= now)
        .values(status="expired", closed_at_utc=now)
    )
    reserve_before = now - timedelta(seconds=RESERVE_AFTER_SECONDS)
    rows = (await session.execute(
        select(PendingCommandModel)
        .where(PendingCommandModel.gateway_id == gateway_id,
               PendingCommandModel.expires_at_utc > now,
               or_(PendingCommandModel.status == "pending",
                   and_(PendingCommandModel.status == "served",
                        PendingCommandModel.served_at_utc <= reserve_before)))
        .order_by(PendingCommandModel.issued_at_utc)
        .limit(limit)
    )).scalars().all()
    for r in rows:
        r.status = "served"
        r.served_at_utc = now           # resets the back-off clock on each delivery
    await session.commit()
    return rows


async def site_key_of(session: AsyncSession, site_id) -> str:
    """The contract site key the gateway expects in a command, not our site UUID."""
    return (await session.execute(
        select(SiteModel.site_key).where(SiteModel.id == site_id)
    )).scalar_one()


def to_pending(row: PendingCommandModel, gateway_key: str, site_key: str) -> dict:
    """A queued command in the gateway's contract shape: contract keys for gateway_id /
    site_id (not our UUIDs) so the gateway validator accepts it."""
    return {
        "command_id": row.command_id,
        "gateway_id": gateway_key,
        "site_id": site_key,
        "command_type": row.command_type,
        "requested_role": row.requested_role,
        "arguments": row.arguments,
        "requires_readback": row.requires_readback,
        "issued_at_utc": row.issued_at_utc,
        "expires_at_utc": row.expires_at_utc,
    }


async def apply_results(session: AsyncSession, *, gateway_id, records) -> None:
    """Close pending rows from a batch of S9 result records (CG-27). Any result takes the
    command off the poll; a terminal status also stamps closed_at. One UPDATE per command_id,
    guarded on closed_at IS NULL so a replayed result is a no-op; commits once."""
    now = datetime.now(timezone.utc)
    touched = False
    for rec in records:
        cid = getattr(rec, "command_id", None)
        if cid is None:
            continue
        fs = getattr(rec, "final_status", None)
        fs = fs.value if hasattr(fs, "value") else fs
        vals = {"status": fs, "final_status": fs,
                "validation_reason": getattr(rec, "validation_reason", None),
                "gateway_error_code": getattr(rec, "gateway_error_code", None)}
        if fs in _TERMINAL:
            vals["closed_at_utc"] = now
        await session.execute(
            update(PendingCommandModel)
            .where(PendingCommandModel.gateway_id == gateway_id,
                   PendingCommandModel.command_id == cid,
                   PendingCommandModel.closed_at_utc.is_(None))
            .values(**vals)
        )
        touched = True
    if touched:
        await session.commit()
