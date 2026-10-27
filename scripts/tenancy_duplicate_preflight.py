"""Slice 3 - READ-ONLY tenancy duplicate preflight (PRELIMINARY).

Detects rows that would collide under a FUTURE Slice 4 normalized unique index. Slice 4
is NOT implemented and its predicate is NOT finalized, so results are reported under BOTH
candidate designs and are never described as "safe to migrate":
  * all-rows    -> blocks an UNCONDITIONAL normalized unique index
  * active-only -> blocks a PARTIAL (WHERE is_deleted = false) normalized index

Blank normalized keys are reported SEPARATELY as data-quality findings that independently
force a non-zero exit. A blank (or any collision) on soft-deleted rows is NOT included in
an active-only (WHERE is_deleted = false) candidate index, so a finding is not an
automatic blocker for every candidate predicate.

The script itself is SELECT-only: no DDL/DML, no migration, not imported by the app.
Point it at the DEV database, never production.

    python -m scripts.tenancy_duplicate_preflight          # from repo root, project .venv

Exit: 0 = no findings (PRELIMINARY, not a migration go-ahead);
      1 = collisions and/or NULL/blank keys found; 2 = connect/config error.

Normalization fidelity (PostgreSQL 16): btrim(x, E' \x09\x0A\x0B\x0C\x0D') trims the six
ASCII whitespace chars {space,TAB,LF,VT,FF,CR}, matching Python str.strip() for ASCII
ONLY. It does NOT match str.strip() for \x1C-\x1F, NEL (\x85), or Unicode whitespace, and
PG lower() may differ from Python str.lower() for non-ASCII. Accurate for ASCII-slug keys;
exotic/Unicode keys are a documented limitation. Verified schema (models + migration
723d4c5ce073): is_deleted NOT NULL on all four tables; keys NOT NULL; all current unique
constraints UNCONDITIONAL. No validator rejects blank keys.
"""
import asyncio

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from src.config import get_settings

# Six ASCII whitespace chars via recognized hex escapes (avoids the \v -> 'v' pitfall).
_WS = r"E' \x09\x0A\x0B\x0C\x0D'"

_COL = {"organizations": "org_key", "sites": "site_key",
        "gateways": "gateway_key", "bess_units": "bess_key"}

# (label, table, parent_col_or_None, normalized-key SQL expression)
KEY_CHECKS = [
    ("organizations.org_key (lower+trim)", "organizations", None,
     f"lower(btrim(org_key, {_WS}))"),
    ("sites.site_key per org (lower+trim)", "sites", "organization_id",
     f"lower(btrim(site_key, {_WS}))"),
    ("gateways.gateway_key (trim-only, case-sensitive)", "gateways", None,
     f"btrim(gateway_key, {_WS})"),
    ("bess_units.bess_key per gateway (lower+trim)", "bess_units", "gateway_id",
     f"lower(btrim(bess_key, {_WS}))"),
]


def _group_sql(table, parent_col, norm_expr):
    # Collisions on NON-blank normalized keys only. Empty-normalized rows are reported
    # separately as blank-key data-quality findings so the two never conflate.
    group_cols = (f"{parent_col}, " if parent_col else "") + norm_expr
    select_parent = f"{parent_col}, " if parent_col else ""
    return f"""
        SELECT {select_parent}{norm_expr} AS norm,
               count(*) AS n_total,
               count(*) FILTER (WHERE is_deleted = false) AS n_active,
               array_agg(id ORDER BY id) AS ids
        FROM {table}
        GROUP BY {group_cols}
        HAVING count(*) > 1 AND {norm_expr} <> ''
        ORDER BY n_total DESC
    """


async def run_checks(db) -> dict:
    """Run all read-only checks on an open connection/session (async .execute). SELECT-only.
    Blank normalized keys are a data-quality finding; counted split into all-rows and
    active (is_deleted = false) so each candidate index's participating set is visible."""
    report = {"nulls": {}, "blanks": {}, "collisions": {}, "whitespace": {}}
    for label, table, parent, norm in KEY_CHECKS:
        col = _COL[table]
        report["nulls"][table] = (await db.execute(
            text(f"SELECT count(*) FROM {table} WHERE {col} IS NULL"))).scalar_one()
        b = (await db.execute(text(
            f"SELECT count(*) FILTER (WHERE {norm} = '') AS n_total, "
            f"       count(*) FILTER (WHERE {norm} = '' AND is_deleted = false) AS n_active "
            f"FROM {table}"))).one()
        report["blanks"][table] = {"all": b._mapping["n_total"], "active": b._mapping["n_active"]}
        rows = (await db.execute(text(_group_sql(table, parent, norm)))).fetchall()
        groups = [dict(r._mapping) for r in rows]
        report["collisions"][label] = {
            "all": sum(1 for g in groups if g["n_total"] > 1),
            "active": sum(1 for g in groups if g["n_active"] > 1),
            "groups": groups,
        }
        report["whitespace"][table] = (await db.execute(
            text(f"SELECT count(*) FROM {table} WHERE {col} <> btrim({col}, {_WS})"))).scalar_one()
    return report


def exit_code(report: dict) -> int:
    """Non-zero if ANY collision group (either candidate predicate), NULL key, or blank
    key (any row) exists. Whitespace is informational only."""
    bad = (any(v > 0 for v in report["nulls"].values())
           or any(b["all"] > 0 for b in report["blanks"].values())
           or any(c["all"] or c["active"] for c in report["collisions"].values()))
    return 1 if bad else 0


def _print_report(report: dict) -> None:
    print("== NULL keys (expected 0; columns are NOT NULL) ==")
    for t, n in report["nulls"].items():
        print(f"[{'NULL!' if n else 'ok'}] {t}.{_COL[t]}: {n}")

    print("\n== BLANK normalized keys (data-quality finding; forces non-zero exit) ==")
    for t, b in report["blanks"].items():
        flag = "BLANK!" if b["all"] else "ok"
        note = ""
        if b["all"] and b["active"] == 0:
            note = "  (all on soft-deleted rows: an active-only partial index would NOT include these)"
        print(f"[{flag}] {t}.{_COL[t]}: all={b['all']} active={b['active']}{note}")

    print("\n== NORMALIZED-KEY COLLISIONS (non-blank; whether each blocks depends on the "
          "candidate index - see n_total vs n_active) ==")
    for label, c in report["collisions"].items():
        if not c["groups"]:
            print(f"[ok] {label}: none")
            continue
        print(f"\n[{label}] all-rows groups={c['all']} active-only groups={c['active']}")
        for g in c["groups"]:
            parent = next((k for k in g if k not in ("norm", "n_total", "n_active", "ids")), None)
            ptxt = f"{parent}={g[parent]} " if parent else ""
            note = "" if g["n_active"] > 1 else "  (active-only index would NOT block: historical/soft-deleted)"
            print(f"   {ptxt}norm={g['norm']!r} n_total={g['n_total']} "
                  f"n_active={g['n_active']} ids={list(g['ids'])}{note}")

    print("\n== Leading/trailing ASCII whitespace (informational) ==")
    for t, n in report["whitespace"].items():
        print(f"[{'WS' if n else 'ok'}] {t}.{_COL[t]}: {n} row(s)")


async def _main() -> int:
    s = get_settings()
    try:
        u = make_url(s.database_url)
        print(f"Target: host={u.host} port={u.port} db={u.database} user={u.username}")
        print("NOTE: PRELIMINARY - Slice 4 index design is not finalized.\n")
    except Exception:
        print("Target: <unparsable database_url>\n")

    engine = create_async_engine(s.database_url)
    try:
        async with engine.connect() as conn:            # connect() + SELECT only = read-only
            report = await run_checks(conn)
    except Exception as e:
        print(f"FATAL: could not run preflight ({type(e).__name__})")
        return 2
    finally:
        await engine.dispose()

    _print_report(report)
    code = exit_code(report)
    if code == 0:
        print("\nPRELIMINARY CLEAN: no collisions/NULL/blank keys under either candidate "
              "predicate. NOT a migration go-ahead - Slice 4 design must be finalized first.")
    else:
        print("\nFINDINGS present (collisions and/or NULL/blank keys). Resolve the relevant "
              "set before any Slice 4 migration; which set depends on the final predicate.")
    return code


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
