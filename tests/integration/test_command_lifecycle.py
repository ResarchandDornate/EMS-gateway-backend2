"""Pending-command lifecycle (CG-26/25/27): create -> poll -> close. gateway_id and the rest
are bare UUIDs — the queue carries them, it does not foreign-key the gateway here."""
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from sqlalchemy import select

from src.commands import service
from src.commands.models import PendingCommandModel


async def _create(session, gw, ctype="pair_set_power"):
    return await service.create_command(
        session, gateway_id=gw, organization_id=uuid.uuid4(), site_id=uuid.uuid4(),
        command_type=ctype, arguments={"pair_id": "pair_1", "power_kw": 50},
        requires_readback=True, requested_by=uuid.uuid4(), requested_role="org_admin",
    )


async def _get(session, command_id):
    return (await session.execute(
        select(PendingCommandModel).where(PendingCommandModel.command_id == command_id)
    )).scalar_one()


async def test_create_starts_pending_with_ttl(session):
    row = await _create(session, uuid.uuid4())
    assert row.status == "pending"
    assert row.command_id is not None
    assert (row.expires_at_utc - row.issued_at_utc).total_seconds() == service.COMMAND_TTL_SECONDS


async def test_poll_serves_until_a_result_arrives(session):
    gw = uuid.uuid4()
    row = await _create(session, gw)

    served = await service.poll_pending(session, gateway_id=gw, limit=10)
    assert [r.command_id for r in served] == [row.command_id]
    assert served[0].status == "served" and served[0].served_at_utc is not None

    # at-least-once: still delivered on the next poll, no result yet
    again = await service.poll_pending(session, gateway_id=gw, limit=10)
    assert [r.command_id for r in again] == [row.command_id]

    rec = SimpleNamespace(command_id=row.command_id, final_status="completed",
                          validation_reason=None, gateway_error_code=None)
    await service.apply_results(session, gateway_id=gw, records=[rec])
    assert await service.poll_pending(session, gateway_id=gw, limit=10) == []

    closed = await _get(session, row.command_id)
    assert closed.status == "completed" and closed.final_status == "completed"
    assert closed.closed_at_utc is not None


async def test_non_terminal_result_drops_from_poll_but_stays_open(session):
    gw = uuid.uuid4()
    row = await _create(session, gw)
    await service.poll_pending(session, gateway_id=gw, limit=10)

    rec = SimpleNamespace(command_id=row.command_id, final_status="executing",
                          validation_reason=None, gateway_error_code=None)
    await service.apply_results(session, gateway_id=gw, records=[rec])

    assert await service.poll_pending(session, gateway_id=gw, limit=10) == []
    r = await _get(session, row.command_id)
    assert r.status == "executing" and r.closed_at_utc is None


async def test_rejection_reason_and_error_code_recorded(session):
    gw = uuid.uuid4()
    row = await _create(session, gw, ctype="pair_abort")
    rec = SimpleNamespace(command_id=row.command_id, final_status="rejected",
                          validation_reason="pair not prepared", gateway_error_code="E_NOT_READY")
    await service.apply_results(session, gateway_id=gw, records=[rec])

    r = await _get(session, row.command_id)
    assert r.status == "rejected" and r.closed_at_utc is not None
    assert r.validation_reason == "pair not prepared" and r.gateway_error_code == "E_NOT_READY"


async def test_apply_results_ignores_a_replayed_result(session):
    gw = uuid.uuid4()
    row = await _create(session, gw)
    await service.apply_results(session, gateway_id=gw, records=[
        SimpleNamespace(command_id=row.command_id, final_status="completed",
                        validation_reason=None, gateway_error_code=None)])
    closed_at = (await _get(session, row.command_id)).closed_at_utc

    # a late/duplicate result must not reopen or restamp a closed command
    await service.apply_results(session, gateway_id=gw, records=[
        SimpleNamespace(command_id=row.command_id, final_status="failed",
                        validation_reason="late", gateway_error_code="X")])
    r = await _get(session, row.command_id)
    assert r.status == "completed" and r.closed_at_utc == closed_at


async def test_lapsed_command_expired_on_poll(session):
    gw = uuid.uuid4()
    row = await _create(session, gw)
    row.expires_at_utc = datetime.now(timezone.utc) - timedelta(seconds=1)
    await session.commit()

    assert await service.poll_pending(session, gateway_id=gw, limit=10) == []
    r = await _get(session, row.command_id)
    assert r.status == "expired" and r.closed_at_utc is not None


async def test_poll_is_scoped_to_the_asking_gateway(session):
    gw_a, gw_b = uuid.uuid4(), uuid.uuid4()
    a = await _create(session, gw_a)
    await _create(session, gw_b)
    served = await service.poll_pending(session, gateway_id=gw_a, limit=10)
    assert [r.command_id for r in served] == [a.command_id]
