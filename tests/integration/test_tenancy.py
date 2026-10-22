from sqlalchemy import select

from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.bess_units.models import BessUnitModel


async def test_chain_and_base_fields(session):
    org = OrganizationModel(org_key="org1", name="Org One")
    session.add(org)
    await session.flush()

    # BaseEntityModel fields exist and default correctly
    assert org.id is not None
    assert org.is_active is True
    assert org.is_deleted is False
    assert org.created_at is not None

    site = SiteModel(organization_id=org.id, site_key="s1", name="Site 1")
    session.add(site)
    await session.flush()
    gw = GatewayModel(site_id=site.id, gateway_key="gw1",
                      client_id="c1", client_secret_hash="h", enabled=True)
    session.add(gw)
    await session.flush()
    bess = BessUnitModel(gateway_id=gw.id, bess_key="bess_1", name="BESS 1",
                         rated_kw=125, rated_kwh=261)
    session.add(bess)
    await session.flush()
    assert bess.gateway_id == gw.id


async def test_cascade_delete(session):
    org = OrganizationModel(org_key="org2", name="Org Two")
    session.add(org)
    await session.flush()
    site = SiteModel(organization_id=org.id, site_key="s1", name="S")
    session.add(site)
    await session.flush()

    # deleting the org cascades to the site (ON DELETE CASCADE)
    await session.delete(org)
    await session.flush()
    rows = (await session.execute(select(SiteModel))).scalars().all()
    assert rows == []