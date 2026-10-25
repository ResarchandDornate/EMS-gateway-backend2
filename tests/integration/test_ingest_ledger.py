import asyncio
import gzip
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.auth.security import hash_password
from src.ingestion.body import BodyError, decode_body, _gunzip_limited
from src.ingestion.models import IngestMessageModel
from src.ingestion.schemas import EnvelopeIn, MAX_RECORDS
from src.ingestion.service import (IngestDecision, canonical_hash, claim_or_resolve,
                                   route_stream, verify_scope)
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel


async def _mk_gateway(session, *, org_key, site_key, gateway_key, client_id, secret="x"):
    org = OrganizationModel(org_key=org_key, name="Org")
    session.add(org)
    await session.flush()
    site = SiteModel(organization_id=org.id, site_key=site_key, name="Site")
    session.add(site)
    await session.flush()
    gw = GatewayModel(site_id=site.id, gateway_key=gateway_key, client_id=client_id,
                      client_secret_hash=hash_password(secret), enabled=True)
    session.add(gw)
    await session.commit()
    return gw


# ---------------- unit: gzip (no DB) ----------------
def test_gunzip_empty():
    assert _gunzip_limited(gzip.compress(b""), 100) == b""


def test_gunzip_exactly_at_cap():
    data = b"x" * 100
    assert _gunzip_limited(gzip.compress(data), 100) == data


def test_gunzip_one_byte_over_cap():
    with pytest.raises(BodyError) as e:
        _gunzip_limited(gzip.compress(b"x" * 101), 100)
    assert e.value.status_code == 413


def test_gunzip_highly_compressible_oversized():
    raw = gzip.compress(b"\x00" * 1_000_000)
    with pytest.raises(BodyError) as e:
        _gunzip_limited(raw, 1000)
    assert e.value.status_code == 413


def test_gunzip_concatenated_trailing():
    raw = gzip.compress(b"first") + gzip.compress(b"second")
    with pytest.raises(BodyError) as e:
        _gunzip_limited(raw, 10_000)
    assert e.value.status_code == 400


def test_decode_body_truncated_gzip():
    raw = gzip.compress(b"hello world payload" * 20)
    with pytest.raises(BodyError) as e:
        decode_body(raw[:-8], "gzip")
    assert e.value.status_code == 400


def test_decode_body_unknown_encoding():
    with pytest.raises(BodyError) as e:
        decode_body(b"data", "br")
    assert e.value.status_code == 415


# ---------------- unit: schema + helpers (no DB) ----------------
def _env(**over):
    base = {"schema_version": "1.0", "message_id": str(uuid.uuid4()),
            "gateway_id": "g", "site_id": "s", "organization_id": "o",
            "stream": "S2", "substream": "bms", "sequence": 1,
            "created_at_utc": "2026-09-04T07:59:14+00:00", "record_count": 1,
            "records": [{"source_id": "x", "asset_id": "a", "signal_key": "k",
                         "value": 1.0, "quality": "good",
                         "timestamp": "2026-09-04T07:59:14+00:00"}]}
    base.update(over)
    return base


def test_route_stream():
    assert route_stream("S1") == "ingest" and route_stream("S2") == "ingest"
    assert route_stream("S5") == "deferred" and route_stream("D1") == "deferred"
    assert route_stream("S42") == "unknown"


def test_verify_scope():
    ident = {"gateway_id": uuid.uuid4(), "site_id": uuid.uuid4(), "organization_id": uuid.uuid4()}
    assert verify_scope(ident, gateway_id=str(ident["gateway_id"]),
                        site_id=str(ident["site_id"]), organization_id=str(ident["organization_id"]))
    assert not verify_scope(ident, gateway_id=str(uuid.uuid4()),
                            site_id=str(ident["site_id"]), organization_id=str(ident["organization_id"]))


def test_canonical_hash_deterministic_and_sensitive():
    h = canonical_hash(_env())
    assert h == canonical_hash(_env()) and len(h) == 64
    changed = _env()
    changed["records"][0]["value"] = 2.0
    assert canonical_hash(changed) != h


def test_envelope_ok():
    EnvelopeIn(**_env())


def test_envelope_accepts_deferred_stream():
    EnvelopeIn(**_env(stream="S5"))


def test_envelope_rejects_bad_schema_version():
    with pytest.raises(ValidationError):
        EnvelopeIn(**_env(schema_version="9.9"))


def test_envelope_rejects_unknown_stream():
    with pytest.raises(ValidationError):
        EnvelopeIn(**_env(stream="S42"))


def test_envelope_rejects_overlong_identifier():
    with pytest.raises(ValidationError):
        EnvelopeIn(**_env(gateway_id="g" * 100))


def test_envelope_rejects_negative_sequence():
    with pytest.raises(ValidationError):
        EnvelopeIn(**_env(sequence=-1))


def test_envelope_rejects_count_mismatch():
    with pytest.raises(ValidationError):
        EnvelopeIn(**_env(record_count=2))


def test_envelope_rejects_record_count_over_max():
    with pytest.raises(ValidationError):
        EnvelopeIn(**_env(record_count=MAX_RECORDS + 1))


# ---------------- ledger state machine (PostgreSQL) ----------------
async def test_claim_new(session):
    gw = await _mk_gateway(session, org_key="o1", site_key="s1", gateway_key="g1", client_id="c1")
    mid = uuid.uuid4()
    decision, token = await claim_or_resolve(
        session, gateway_id=gw.id, stream="S2", message_id=mid,
        sequence=1, record_count=1, payload_hash="h1")
    assert decision == IngestDecision.CLAIMED and token is not None
    assert not session.in_transaction()
    row = (await session.execute(
        select(IngestMessageModel).where(IngestMessageModel.message_id == mid))).scalar_one()
    assert row.status == "pending" and row.lease_owner == token and row.stored_at is None


async def test_claim_duplicate_stored(session):
    gw = await _mk_gateway(session, org_key="o1", site_key="s1", gateway_key="g1", client_id="c1")
    mid = uuid.uuid4()
    session.add(IngestMessageModel(gateway_id=gw.id, message_id=mid, stream="S2",
                                   sequence=1, record_count=1, payload_hash="h1", status="stored"))
    await session.commit()
    decision, token = await claim_or_resolve(
        session, gateway_id=gw.id, stream="S2", message_id=mid,
        sequence=1, record_count=1, payload_hash="h1")
    assert decision == IngestDecision.DUPLICATE_STORED and token is None
    assert not session.in_transaction()


async def test_claim_pending_active(session):
    gw = await _mk_gateway(session, org_key="o1", site_key="s1", gateway_key="g1", client_id="c1")
    mid = uuid.uuid4()
    session.add(IngestMessageModel(gateway_id=gw.id, message_id=mid, stream="S2",
                                   sequence=1, record_count=1, payload_hash="h1", status="pending",
                                   lease_owner=uuid.uuid4(),
                                   lease_expires_at=datetime.now(timezone.utc) + timedelta(minutes=5)))
    await session.commit()
    decision, _ = await claim_or_resolve(
        session, gateway_id=gw.id, stream="S2", message_id=mid,
        sequence=1, record_count=1, payload_hash="h1")
    assert decision == IngestDecision.IN_PROGRESS
    assert not session.in_transaction()


async def test_claim_pending_expired_reclaimed(session):
    gw = await _mk_gateway(session, org_key="o1", site_key="s1", gateway_key="g1", client_id="c1")
    mid = uuid.uuid4()
    old = uuid.uuid4()
    session.add(IngestMessageModel(gateway_id=gw.id, message_id=mid, stream="S2",
                                   sequence=1, record_count=1, payload_hash="h1", status="pending",
                                   lease_owner=old,
                                   lease_expires_at=datetime.now(timezone.utc) - timedelta(minutes=5)))
    await session.commit()
    decision, token = await claim_or_resolve(
        session, gateway_id=gw.id, stream="S2", message_id=mid,
        sequence=1, record_count=1, payload_hash="h1")
    assert decision == IngestDecision.RECLAIMED and token is not None and token != old


async def test_claim_hash_mismatch(session):
    gw = await _mk_gateway(session, org_key="o1", site_key="s1", gateway_key="g1", client_id="c1")
    mid = uuid.uuid4()
    session.add(IngestMessageModel(gateway_id=gw.id, message_id=mid, stream="S2",
                                   sequence=1, record_count=1, payload_hash="h1", status="pending",
                                   lease_owner=uuid.uuid4(),
                                   lease_expires_at=datetime.now(timezone.utc) + timedelta(minutes=5)))
    await session.commit()
    decision, _ = await claim_or_resolve(
        session, gateway_id=gw.id, stream="S2", message_id=mid,
        sequence=1, record_count=1, payload_hash="DIFFERENT")
    assert decision == IngestDecision.HASH_MISMATCH
    assert not session.in_transaction()


async def test_claim_cross_gateway_isolation(session):
    gw1 = await _mk_gateway(session, org_key="o1", site_key="s1", gateway_key="g1", client_id="c1")
    gw2 = await _mk_gateway(session, org_key="o2", site_key="s2", gateway_key="g2", client_id="c2")
    mid = uuid.uuid4()
    d1, _ = await claim_or_resolve(session, gateway_id=gw1.id, stream="S2", message_id=mid,
                                   sequence=1, record_count=1, payload_hash="h1")
    d2, _ = await claim_or_resolve(session, gateway_id=gw2.id, stream="S2", message_id=mid,
                                   sequence=1, record_count=1, payload_hash="h1")
    assert d1 == IngestDecision.CLAIMED and d2 == IngestDecision.CLAIMED


# ---------------- real concurrency: two independent sessions, same test DB ----------------
async def test_concurrent_reclaim_single_winner(session, test_db_url):
    gw = await _mk_gateway(session, org_key="o1", site_key="s1", gateway_key="g1", client_id="c1")
    mid = uuid.uuid4()
    session.add(IngestMessageModel(gateway_id=gw.id, message_id=mid, stream="S2",
                                   sequence=1, record_count=1, payload_hash="h1", status="pending",
                                   lease_owner=uuid.uuid4(),
                                   lease_expires_at=datetime.now(timezone.utc) - timedelta(minutes=5)))
    await session.commit()

    engine = create_async_engine(test_db_url)          # same DB as the session fixture
    maker = async_sessionmaker(engine, expire_on_commit=False)

    async def attempt():
        async with maker() as s:                        # independent connection per racer
            return await claim_or_resolve(
                s, gateway_id=gw.id, stream="S2", message_id=mid,
                sequence=1, record_count=1, payload_hash="h1")

    try:
        r1, r2 = await asyncio.gather(attempt(), attempt())
    finally:
        await engine.dispose()

    decisions = [r1[0], r2[0]]
    assert decisions.count(IngestDecision.RECLAIMED) == 1
    assert decisions.count(IngestDecision.IN_PROGRESS) == 1


# ---------------- endpoint stays disabled (503 after auth) ----------------
async def test_ingest_endpoint_requires_auth(client, session):
    c, _ = client
    assert (await c.post("/api/v1/gateway/ingest", json={})).status_code == 401


async def test_ingest_endpoint_503_after_auth(client, session):
    c, _ = client
    await _mk_gateway(session, org_key="oe", site_key="se", gateway_key="ge",
                      client_id="gw_ep", secret="sec")
    tok = (await c.post("/api/v1/gateway/token",
                        json={"client_id": "gw_ep", "client_secret": "sec"})).json()["access_token"]
    r = await c.post("/api/v1/gateway/ingest", json={}, headers={"Authorization": f"Bearer {tok}"})
    assert r.status_code == 503


# ---- 9b-ii-a item 3: canonical_hash excludes transport/identity metadata ----
def test_canonical_hash_ignores_transport_and_identity_metadata():
    base = _env()
    h = canonical_hash(base)
    assert canonical_hash(_env(compression="gzip")) == h                 # transport
    assert canonical_hash(_env(gateway_software_version="9.9.9")) == h    # transport
    assert canonical_hash(_env(gateway_id="DIFFERENT")) == h              # identity excluded
    assert canonical_hash(_env(site_id="OTHER")) == h
