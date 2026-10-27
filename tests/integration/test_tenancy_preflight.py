"""Slice 3 preflight tests - fixtures write only to the isolated ems_test DB.

Isolation verified by inspecting tests/integration/conftest.py: the session engine is
pinned to ems_test (asserted) and every test runs create_all at setup / drop_all at
teardown, so committed fixtures cannot leak across tests. These tests call run_checks on
the test session and never invoke the script's _main() (which targets the dev DB).

All fixture pairs use DISTINCT raw keys that collide only AFTER normalization, so none
trips the current unconditional unique constraints before run_checks executes.
"""
import pytest
from sqlalchemy import select

from scripts.tenancy_duplicate_preflight import run_checks, exit_code
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.bess_units.models import BessUnitModel

ORG = "organizations.org_key (lower+trim)"
SITE = "sites.site_key per org (lower+trim)"
GW = "gateways.gateway_key (trim-only, case-sensitive)"
BESS = "bess_units.bess_key per gateway (lower+trim)"


async def _org(session, key, *, deleted=False):
    o = OrganizationModel(org_key=key, name=(key or "x"), is_deleted=deleted)
    session.add(o); await session.flush(); return o


async def _site(session, org, key, *, deleted=False):
    s = SiteModel(organization_id=org.id, site_key=key, name=(key or "x"), is_deleted=deleted)
    session.add(s); await session.flush(); return s


async def _gw(session, site, key, cid, *, deleted=False):
    g = GatewayModel(site_id=site.id, gateway_key=key, client_id=cid,
                     client_secret_hash="h", is_deleted=deleted)
    session.add(g); await session.flush(); return g


async def _bess(session, gw, key, *, deleted=False):
    b = BessUnitModel(gateway_id=gw.id, bess_key=key, name=(key or "x"), is_deleted=deleted)
    session.add(b); await session.flush(); return b


# --- organizations: all-rows + active/soft-deleted combinations -------------------

async def test_org_all_rows_collision_both_active(session):
    await _org(session, "dup"); await _org(session, "DUP")
    await session.commit()
    c = (await run_checks(session))["collisions"][ORG]
    assert c["all"] == 1 and c["active"] == 1


async def test_org_active_vs_deleted_blocks_unconditional_only(session):
    o_active = await _org(session, "x1")                 # active
    o_deleted = await _org(session, "X1", deleted=True)  # soft-deleted; same normalized key
    await session.commit()
    rep = await run_checks(session)
    c = rep["collisions"][ORG]
    assert c["all"] == 1 and c["active"] == 0            # 1 all-rows group; 0 active-only groups
    assert len(c["groups"]) == 1
    g = c["groups"][0]
    assert g["norm"] == "x1"
    assert g["n_total"] == 2 and g["n_active"] == 1      # 2 rows, 1 active (row count, not group count)
    assert set(g["ids"]) == {o_active.id, o_deleted.id}


async def test_org_both_deleted_findings_only_on_soft_deleted(session):
    a = await _org(session, "z1", deleted=True)
    b = await _org(session, "Z1", deleted=True)
    await session.commit()
    rep = await run_checks(session)
    c = rep["collisions"][ORG]
    assert c["all"] == 1 and c["active"] == 0
    assert len(c["groups"]) == 1
    g = c["groups"][0]
    assert g["norm"] == "z1"
    assert g["n_total"] == 2 and g["n_active"] == 0      # both soft-deleted
    assert set(g["ids"]) == {a.id, b.id}


# --- sites: same-org vs cross-org, plus active/deleted and both-deleted -----------

async def test_sites_same_org_collide_cross_org_distinct(session):
    o1 = await _org(session, "o1"); o2 = await _org(session, "o2")
    await _site(session, o1, "s1"); await _site(session, o1, "S1")   # same org -> collide
    await _site(session, o2, "s1")                                   # other org -> distinct
    await session.commit()
    c = (await run_checks(session))["collisions"][SITE]
    assert c["all"] == 1 and c["active"] == 1


async def test_sites_active_vs_deleted(session):
    o = await _org(session, "so")
    sa = await _site(session, o, "sa1")                      # active
    sd = await _site(session, o, "SA1", deleted=True)        # soft-deleted; same normalized
    await session.commit()
    c = (await run_checks(session))["collisions"][SITE]
    assert c["all"] == 1 and c["active"] == 0
    assert len(c["groups"]) == 1
    g = c["groups"][0]
    assert g["norm"] == "sa1"
    assert g["n_total"] == 2 and g["n_active"] == 1
    assert set(g["ids"]) == {sa.id, sd.id}


async def test_sites_both_deleted(session):
    o = await _org(session, "so2")
    a = await _site(session, o, "sd1", deleted=True)
    b = await _site(session, o, "SD1", deleted=True)
    await session.commit()
    c = (await run_checks(session))["collisions"][SITE]
    assert c["all"] == 1 and c["active"] == 0
    assert len(c["groups"]) == 1
    g = c["groups"][0]
    assert g["norm"] == "sd1"
    assert g["n_total"] == 2 and g["n_active"] == 0
    assert set(g["ids"]) == {a.id, b.id}


# --- bess: same-gateway vs cross-gateway, plus active/deleted and both-deleted -----

async def test_bess_same_gateway_collide_cross_gateway_distinct(session):
    o = await _org(session, "bo"); s = await _site(session, o, "bs")
    g1 = await _gw(session, s, "g1", "cid-g1"); g2 = await _gw(session, s, "g2", "cid-g2")
    await _bess(session, g1, "b1"); await _bess(session, g1, "B1")   # same gw -> collide
    await _bess(session, g2, "b1")                                   # other gw -> distinct
    await session.commit()
    c = (await run_checks(session))["collisions"][BESS]
    assert c["all"] == 1 and c["active"] == 1


async def test_bess_active_vs_deleted(session):
    o = await _org(session, "bbo"); s = await _site(session, o, "bbs")
    g = await _gw(session, s, "bbg", "cid-bb")
    ba = await _bess(session, g, "ba1")                     # active
    bd = await _bess(session, g, "BA1", deleted=True)       # soft-deleted; same normalized
    await session.commit()
    c = (await run_checks(session))["collisions"][BESS]
    assert c["all"] == 1 and c["active"] == 0
    assert len(c["groups"]) == 1
    grp = c["groups"][0]
    assert grp["norm"] == "ba1"
    assert grp["n_total"] == 2 and grp["n_active"] == 1
    assert set(grp["ids"]) == {ba.id, bd.id}


async def test_bess_both_deleted(session):
    o = await _org(session, "bbo2"); s = await _site(session, o, "bbs2")
    g = await _gw(session, s, "bbg2", "cid-bb2")
    a = await _bess(session, g, "bd1", deleted=True)
    b = await _bess(session, g, "BD1", deleted=True)
    await session.commit()
    c = (await run_checks(session))["collisions"][BESS]
    assert c["all"] == 1 and c["active"] == 0
    assert len(c["groups"]) == 1
    grp = c["groups"][0]
    assert grp["norm"] == "bd1"
    assert grp["n_total"] == 2 and grp["n_active"] == 0
    assert set(grp["ids"]) == {a.id, b.id}


# --- gateway_key: whitespace collides / case distinct; active/deleted + both-deleted

async def test_gateway_whitespace_collides_case_distinct(session):
    o = await _org(session, "go"); s = await _site(session, o, "gs")
    ga = await _gw(session, s, "GW3", "c1")         # collides (after trim) with the next,
    gb = await _gw(session, s, "  GW3  ", "c2")     # case preserved -> normalized "GW3"
    await _gw(session, s, "GW", "c3"); await _gw(session, s, "gw", "c4")  # case-only -> distinct
    await session.commit()
    rep = await run_checks(session)
    c = rep["collisions"][GW]
    assert c["all"] == 1 and c["active"] == 1       # both colliding rows active -> active-only group too
    assert len(c["groups"]) == 1                    # GW / gw produce NO collision group
    grp = c["groups"][0]
    assert grp["norm"] == "GW3"                     # whitespace trimmed, case preserved
    assert grp["n_total"] == 2 and grp["n_active"] == 2
    assert set(grp["ids"]) == {ga.id, gb.id}
    assert rep["whitespace"]["gateways"] >= 1       # "  GW3  " flagged


async def test_gateway_active_vs_deleted(session):
    o = await _org(session, "ggo"); s = await _site(session, o, "ggs")
    ga = await _gw(session, s, "gwa", "c1")                  # active
    gd = await _gw(session, s, " gwa ", "c2", deleted=True)  # soft-deleted; trims to same key
    await session.commit()
    c = (await run_checks(session))["collisions"][GW]
    assert c["all"] == 1 and c["active"] == 0
    assert len(c["groups"]) == 1
    grp = c["groups"][0]
    assert grp["norm"] == "gwa"
    assert grp["n_total"] == 2 and grp["n_active"] == 1
    assert set(grp["ids"]) == {ga.id, gd.id}


async def test_gateway_both_deleted(session):
    o = await _org(session, "ggo2"); s = await _site(session, o, "ggs2")
    a = await _gw(session, s, "gwd", "c3", deleted=True)
    b = await _gw(session, s, " gwd ", "c4", deleted=True)
    await session.commit()
    c = (await run_checks(session))["collisions"][GW]
    assert c["all"] == 1 and c["active"] == 0
    assert len(c["groups"]) == 1
    grp = c["groups"][0]
    assert grp["norm"] == "gwd"
    assert grp["n_total"] == 2 and grp["n_active"] == 0
    assert set(grp["ids"]) == {a.id, b.id}


# --- ASCII whitespace variant on org + informational whitespace report ------------

async def test_org_ascii_whitespace_variant_collides_and_reported(session):
    await _org(session, "ws"); await _org(session, "  ws  ")
    await session.commit()
    rep = await run_checks(session)
    assert rep["collisions"][ORG]["all"] == 1
    assert rep["whitespace"]["organizations"] >= 1
    assert exit_code(rep) == 1


# --- empty / whitespace-only keys: blank findings (split), NOT collisions ---------

async def test_blank_and_whitespace_only_keys_are_blank_findings_not_collisions(session):
    await _org(session, ""); await _org(session, "   ")            # both normalize to ""
    await session.commit()
    rep = await run_checks(session)
    assert rep["blanks"]["organizations"] == {"all": 2, "active": 2}
    assert rep["collisions"][ORG]["all"] == 0                       # not a collision group
    assert exit_code(rep) == 1


async def test_blank_key_on_soft_deleted_split_counts(session):
    await _org(session, "", deleted=True)
    await session.commit()
    rep = await run_checks(session)
    assert rep["blanks"]["organizations"] == {"all": 1, "active": 0}
    assert exit_code(rep) == 1


# --- NULL structurally absent; synthetic exit-flag -------------------------------

async def test_null_absent_and_exit_flag_on_synthetic_findings(session):
    await _org(session, "ok1"); await session.commit()
    rep = await run_checks(session)
    assert all(v == 0 for v in rep["nulls"].values())              # NOT NULL -> unreachable
    assert exit_code({"nulls": {"organizations": 1}, "blanks": {}, "collisions": {}}) == 1
    assert exit_code({"nulls": {}, "blanks": {"sites": {"all": 1, "active": 0}}, "collisions": {}}) == 1
