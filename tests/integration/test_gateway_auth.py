import uuid
from datetime import datetime, timedelta, timezone

import jwt

from src.auth.security import create_access_token, create_gateway_token, hash_password
from src.config import get_settings
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel


async def make_gateway(session, client_id="gw_test", secret="s3cret-value", enabled=True):
    org = OrganizationModel(org_key="o1", name="Org 1")
    session.add(org)
    await session.flush()
    site = SiteModel(organization_id=org.id, site_key="s1", name="Site 1")
    session.add(site)
    await session.flush()
    gw = GatewayModel(site_id=site.id, gateway_key="gk-1", client_id=client_id,
                      client_secret_hash=hash_password(secret), enabled=enabled)
    session.add(gw)
    await session.commit()
    return gw, secret


def bearer(token):
    return {"Authorization": f"Bearer {token}"}


async def test_token_valid(client, session):
    c, _ = client
    _, secret = await make_gateway(session)
    r = await c.post("/api/v1/gateway/token", json={"client_id": "gw_test", "client_secret": secret})
    assert r.status_code == 200
    body = r.json()
    assert body["access_token"] and body["token_type"] == "bearer" and body["expires_in"] == 1800


async def test_token_wrong_secret(client, session):
    c, _ = client
    await make_gateway(session)
    r = await c.post("/api/v1/gateway/token", json={"client_id": "gw_test", "client_secret": "wrong"})
    assert r.status_code == 401


async def test_token_unknown_client(client, session):
    c, _ = client
    await make_gateway(session)
    r = await c.post("/api/v1/gateway/token", json={"client_id": "nope", "client_secret": "x"})
    assert r.status_code == 401


async def test_token_disabled(client, session):
    c, _ = client
    _, secret = await make_gateway(session, enabled=False)
    r = await c.post("/api/v1/gateway/token", json={"client_id": "gw_test", "client_secret": secret})
    assert r.status_code == 403


async def test_me_valid(client, session):
    c, _ = client
    gw, secret = await make_gateway(session)
    tok = (await c.post("/api/v1/gateway/token",
                        json={"client_id": "gw_test", "client_secret": secret})).json()["access_token"]
    r = await c.get("/api/v1/gateway/me", headers=bearer(tok))
    assert r.status_code == 200
    body = r.json()
    assert body["gateway_id"] == str(gw.id) and body["enabled"] is True and body["organization_id"]


async def test_me_disabled_after_token(client, session):
    c, _ = client
    gw, secret = await make_gateway(session)
    tok = (await c.post("/api/v1/gateway/token",
                        json={"client_id": "gw_test", "client_secret": secret})).json()["access_token"]
    gw.enabled = False
    await session.commit()
    r = await c.get("/api/v1/gateway/me", headers=bearer(tok))
    assert r.status_code == 403


async def test_me_rejects_user_token(client, session):
    c, _ = client
    await make_gateway(session)
    r = await c.get("/api/v1/gateway/me", headers=bearer(create_access_token(uuid.uuid4())))
    assert r.status_code == 401


async def test_me_rejects_expired_token(client, session):
    c, _ = client
    gw, _ = await make_gateway(session)
    past = datetime.now(timezone.utc) - timedelta(hours=1)
    expired = jwt.encode({"sub": str(gw.id), "aud": "gateway", "iat": past,
                          "exp": past + timedelta(minutes=1)},
                         get_settings().jwt_secret, algorithm="HS256")
    r = await c.get("/api/v1/gateway/me", headers=bearer(expired))
    assert r.status_code == 401


async def test_user_endpoint_rejects_gateway_token(client, session):
    c, _ = client
    gw, _ = await make_gateway(session)
    r = await c.get("/api/v1/auth/me", headers=bearer(create_gateway_token(gw.id)))
    assert r.status_code == 401


async def test_me_no_token(client, session):
    c, _ = client
    await make_gateway(session)
    r = await c.get("/api/v1/gateway/me")
    assert r.status_code == 401


async def test_me_malformed_subject(client, session):
    
    c, _ = client
    await make_gateway(session)
    now = datetime.now(timezone.utc)
    tok = jwt.encode({"sub": "not-a-uuid", "aud": "gateway", "iat": now,
                      "exp": now + timedelta(minutes=5)},
                     get_settings().jwt_secret, algorithm="HS256")
    r = await c.get("/api/v1/gateway/me", headers=bearer(tok))
    assert r.status_code == 401

# ---- O-hygiene: last_seen_at token-issuance heartbeat -------------------------------

async def test_token_success_sets_last_seen_at(client, session):
    c, _ = client
    gw, secret = await make_gateway(session)
    assert gw.last_seen_at is None
    before = datetime.now(timezone.utc)
    r = await c.post("/api/v1/gateway/token", json={"client_id": "gw_test", "client_secret": secret})
    assert r.status_code == 200
    await session.refresh(gw)
    assert gw.last_seen_at is not None
    assert gw.last_seen_at.tzinfo is not None                     # timezone-aware
    assert before - timedelta(seconds=5) <= gw.last_seen_at <= datetime.now(timezone.utc) + timedelta(seconds=5)


async def test_bad_secret_does_not_set_last_seen_at(client, session):
    c, _ = client
    gw, _ = await make_gateway(session)
    r = await c.post("/api/v1/gateway/token", json={"client_id": "gw_test", "client_secret": "wrong"})
    assert r.status_code == 401
    await session.refresh(gw)
    assert gw.last_seen_at is None


async def test_unknown_client_does_not_set_last_seen_at(client, session):
    c, _ = client
    gw, _ = await make_gateway(session)
    r = await c.post("/api/v1/gateway/token", json={"client_id": "nope", "client_secret": "x"})
    assert r.status_code == 401
    await session.refresh(gw)
    assert gw.last_seen_at is None


async def test_disabled_gateway_does_not_set_last_seen_at(client, session):
    c, _ = client
    gw, secret = await make_gateway(session, enabled=False)
    r = await c.post("/api/v1/gateway/token", json={"client_id": "gw_test", "client_secret": secret})
    assert r.status_code == 403
    await session.refresh(gw)
    assert gw.last_seen_at is None


async def test_last_seen_write_rolls_back_with_transaction(session):
    """Helper-level flush/rollback behavior ONLY: mark_gateway_seen stages the write
    (set attribute + flush) in the ambient transaction and performs no internal commit,
    so a rollback discards it. This does NOT independently test the endpoint's
    transaction (the endpoint's no-heartbeat-on-failure path is covered by the three
    negative token tests above)."""
    from src.gateway import service
    gw, _ = await make_gateway(session)          # committed with last_seen_at NULL
    assert gw.last_seen_at is None
    await service.mark_gateway_seen(session, gw)  # set + flush, NOT committed
    assert gw.last_seen_at is not None            # visible in-session
    await session.rollback()                      # abort the transaction
    await session.refresh(gw)                     # reload authoritative row
    assert gw.last_seen_at is None                # never persisted
