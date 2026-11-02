"""Backfill tests: stream_records -> dedicated tables. Proves ledger-bypass, idempotent
resume, verbatim payload, source untouched, and recoverable per-record error reporting."""
import uuid
from datetime import datetime, timezone

from sqlalchemy import func, select

from src.auth.security import hash_password
from src.ingestion.backfill import backfill_stream
from src.ingestion.alarm_models import AlarmEventRecordModel
from src.ingestion.health_models import GatewayHealthRecordModel
from src.ingestion.models import IngestMessageModel
from src.ingestion.stream_models import StreamRecordModel
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel

TS = "2026-09-04T08:00:00.340928+00:00"
S3_OK = {"status": "ok", "timestamp_utc": TS, "gateway_mode": "ems_read_write",
         "source_count": 2, "asset_count": 18, "online_asset_count": 18,
         "total_signal_count": 2844, "bad_signal_count": 0, "commands_enabled": True,
         "control_enabled": True, "storage_can_write": True,
         "storage": {"enabled": True, "type": "sqlite", "reasons": []}}   # nested extra, verbatim
S3_BAD = {k: v for k, v in S3_OK.items() if k != "status"}               # missing required 'status'


def _s4(event_type="over_voltage", severity="Critical", state="ACTIVE"):
    return {"event_id": str(uuid.uuid4()), "timestamp_utc": TS, "gateway_id": "g", "site_id": "s",
            "source_id": "pcs1", "event_type": event_type, "severity": severity, "state": state,
            "message": "x", "dedupe_key": f"{event_type}:{state}"}


async def _mk(session):
    org = OrganizationModel(org_key="o1", name="o"); session.add(org); await session.flush()
    site = SiteModel(organization_id=org.id, site_key="s1", name="s"); session.add(site); await session.flush()
    gw = GatewayModel(site_id=site.id, gateway_key="g1", client_id="c1",
                      client_secret_hash=hash_password("x"), enabled=True)
    session.add(gw); await session.flush()
    return org, site, gw


async def _seed(session, *, gw, org, site, stream, substream, msg_id, payloads):
    for i, p in enumerate(payloads):
        session.add(StreamRecordModel(
            gateway_id=gw.id, organization_id=org.id, site_id=site.id, message_id=msg_id,
            stream=stream, substream=substream, record_index=i, source_id=None,
            recorded_at=datetime.now(timezone.utc), payload=p))
    await session.commit()


async def _count(session, model):
    return (await session.execute(select(func.count()).select_from(model))).scalar()


async def test_s3_backfill_migrates_verbatim_and_bypasses_ledger(client, session):
    _, _ = client
    org, site, gw = await _mk(session)
    good_msg = uuid.uuid4()
    await _seed(session, gw=gw, org=org, site=site, stream="S3", substream="gateway",
                msg_id=good_msg, payloads=[S3_OK])
    # ledger already says this message is 'stored' -> proves backfill bypasses the ledger
    session.add(IngestMessageModel(gateway_id=gw.id, message_id=good_msg, stream="S3",
                                   sequence=1, record_count=1, payload_hash="h", status="stored"))
    await session.commit()

    rep = await backfill_stream(session, stream="S3")
    assert rep.migrated_messages == 1 and rep.migrated_rows == 1 and rep.ok

    row = (await session.execute(select(GatewayHealthRecordModel))).scalars().one()
    assert row.gateway_id == gw.id and row.organization_id == org.id and row.site_id == site.id
    assert row.payload == S3_OK                       # verbatim, nested 'storage' preserved
    # source left completely intact
    assert await _count(session, StreamRecordModel) == 1


async def test_s3_backfill_idempotent_resume(client, session):
    _, _ = client
    org, site, gw = await _mk(session)
    await _seed(session, gw=gw, org=org, site=site, stream="S3", substream="gateway",
                msg_id=uuid.uuid4(), payloads=[S3_OK])
    r1 = await backfill_stream(session, stream="S3")
    assert r1.migrated_messages == 1
    r2 = await backfill_stream(session, stream="S3")          # re-run = resume
    assert r2.migrated_messages == 0 and r2.skipped_already_present == 1
    assert await _count(session, GatewayHealthRecordModel) == 1   # no duplicate


async def test_s3_malformed_record_reported_not_silently_skipped(client, session):
    _, _ = client
    org, site, gw = await _mk(session)
    bad_msg = uuid.uuid4()
    await _seed(session, gw=gw, org=org, site=site, stream="S3", substream="gateway",
                msg_id=bad_msg, payloads=[S3_BAD])
    rep = await backfill_stream(session, stream="S3")
    assert rep.migrated_rows == 0 and rep.failed_messages == 1
    assert any(e["message_id"] == str(bad_msg) and e["record_index"] == 0 for e in rep.errors)
    assert await _count(session, GatewayHealthRecordModel) == 0     # not written
    assert await _count(session, StreamRecordModel) == 1            # source intact


async def test_s4_event_keyed_backfill(client, session):
    _, _ = client
    org, site, gw = await _mk(session)
    await _seed(session, gw=gw, org=org, site=site, stream="S4", substream="alarms",
                msg_id=uuid.uuid4(), payloads=[_s4(), _s4(event_type="over_temp")])
    rep = await backfill_stream(session, stream="S4")
    assert rep.migrated_rows == 2 and rep.ok
    rows = (await session.execute(select(AlarmEventRecordModel))).scalars().all()
    assert len(rows) == 2 and {r.severity for r in rows} == {"Critical"}
    assert {r.event_type for r in rows} == {"over_voltage", "over_temp"}
