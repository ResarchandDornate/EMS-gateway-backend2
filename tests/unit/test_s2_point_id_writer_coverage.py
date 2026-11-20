"""Writer-coverage guard: EVERY production call to the S2 projection entry points -- the public
build_influx_points AND the private _s2_points where the point_id tag is actually written --
must pass the registry resolver (keyword resolve_point_id= or the 5th positional arg). Matching
BOTH names closes the bypass where a new writer could call _s2_points directly. Fails if any
uncovered S2 writer is added, so a legacy/alternate path can never emit an unvalidated point_id."""
import ast, pathlib

SRC = pathlib.Path(__file__).resolve().parents[2] / "src"

def _projection_calls(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            f = node.func
            name = f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", None)
            if name in ("build_influx_points", "_s2_points"):
                yield node

def test_every_production_build_influx_points_call_passes_resolver():
    missing = []
    for py in SRC.rglob("*.py"):
        tree = ast.parse(py.read_text(encoding="utf-8"))
        for call in _projection_calls(tree):
            has_kw = any(k.arg == "resolve_point_id" for k in call.keywords)
            has_pos = len(call.args) >= 5          # envelope, identity, settings, t3, resolve_point_id
            if not (has_kw or has_pos):
                missing.append(f"{py.relative_to(SRC)}:{call.lineno}")
    assert not missing, f"S2 projection called without a registry resolver: {missing}"

def test_coverage_guard_sees_the_two_known_call_sites():
    # sanity: the guard actually inspects production call sites (batch ingest + dead-letter replay)
    found = []
    for py in SRC.rglob("*.py"):
        tree = ast.parse(py.read_text(encoding="utf-8"))
        found += [f"{py.name}:{c.lineno}" for c in _projection_calls(tree)]
    assert any("router.py" in f for f in found)
    assert any("dead_letter_service.py" in f for f in found)
