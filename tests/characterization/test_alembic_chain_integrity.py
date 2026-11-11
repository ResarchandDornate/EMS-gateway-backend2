"""Characterization: Alembic migration-chain integrity (no database required).

Parses ``migrations/versions/*.py`` and asserts the revision graph is well formed:
exactly one head, every ``down_revision`` resolves to a known revision (or the base
``None``), no duplicate revision ids, and no cycles.

Purpose: the planned model refactor (Phase 2) moves model classes between modules. That
must not disturb the migration history. This test guards "Alembic migration integrity"
structurally and offline, so a broken or forked history is caught in CI before it ever
reaches ``alembic upgrade head`` on a real database.

Captured from tarun-DB @ d4eca47: 16 revision files, single linear head.
"""
import pathlib
import re

VERSIONS_DIR = pathlib.Path(__file__).resolve().parents[2] / "migrations" / "versions"

_REV = re.compile(r"^revision\s*(?::\s*str)?\s*=\s*['\"]([^'\"]+)['\"]", re.M)
_DOWN = re.compile(r"^down_revision\s*(?::[^=]*)?=\s*(None|['\"][^'\"]+['\"])", re.M)


def _parse():
    """Return {revision_id: down_revision_id_or_None} across every version file."""
    graph: dict[str, str | None] = {}
    files = sorted(VERSIONS_DIR.glob("*.py"))
    for f in files:
        if f.name == "__init__.py":
            continue
        text = f.read_text(encoding="utf-8")
        rev = _REV.search(text)
        down = _DOWN.search(text)
        assert rev, f"{f.name}: no `revision = ...` found"
        assert down, f"{f.name}: no `down_revision = ...` found"
        rev_id = rev.group(1)
        down_raw = down.group(1)
        down_id = None if down_raw == "None" else down_raw.strip("'\"")
        assert rev_id not in graph, f"duplicate revision id {rev_id!r} (in {f.name})"
        graph[rev_id] = down_id
    return graph, files


def test_version_files_present():
    graph, files = _parse()
    assert len(graph) >= 1, "no migration revisions found"


def test_every_down_revision_resolves():
    graph, _ = _parse()
    known = set(graph)
    for rev, down in graph.items():
        assert down is None or down in known, (
            f"revision {rev!r} has down_revision {down!r} which is not a known revision"
        )


def test_exactly_one_base_and_one_head():
    graph, _ = _parse()
    bases = [r for r, d in graph.items() if d is None]
    referenced = {d for d in graph.values() if d is not None}
    heads = [r for r in graph if r not in referenced]
    assert len(bases) == 1, f"expected exactly one base revision, found {sorted(bases)}"
    assert len(heads) == 1, f"expected exactly one head revision, found {sorted(heads)}"


def test_chain_is_acyclic_and_connected():
    graph, _ = _parse()
    # Walk from the single head back to the base; it must touch every revision exactly once.
    referenced = {d for d in graph.values() if d is not None}
    head = next(r for r in graph if r not in referenced)
    seen, cur, steps = set(), head, 0
    while cur is not None:
        assert cur not in seen, f"cycle detected at {cur!r}"
        seen.add(cur)
        cur = graph[cur]
        steps += 1
        assert steps <= len(graph) + 1, "walk exceeded revision count (cycle/fork)"
    assert seen == set(graph), (
        "migration history is forked/disconnected; unreachable from head: "
        f"{sorted(set(graph) - seen)}"
    )
