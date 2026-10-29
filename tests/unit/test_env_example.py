"""Guards the committed .env.example against drift from Settings.

pydantic-settings uses extra="ignore", so an env var whose name does not match a
Settings field is silently dropped: a template using the wrong name (e.g.
POSTGRES_DSN instead of DATABASE_URL) lets a deployer set values that never load,
and the app then fails on the real required field. This keeps the template aligned
with the authoritative field names in src/config.py and ensures every required
(no-default) setting appears in it. Introspects Settings.model_fields only -- it
never constructs Settings(), so it reads no .env and needs no database.
"""
from pathlib import Path

from src.config import Settings

_ENV_EXAMPLE = Path(__file__).resolve().parents[2] / ".env.example"


def _template_keys():
    keys = set()
    for raw in _ENV_EXAMPLE.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        keys.add(line.split("=", 1)[0].strip())
    return keys


def test_env_example_has_no_unknown_keys():
    valid = {name.upper() for name in Settings.model_fields}
    unknown = _template_keys() - valid
    assert not unknown, f".env.example keys not read by Settings (silently ignored): {sorted(unknown)}"


def test_env_example_lists_all_required_settings():
    required = {name.upper() for name, f in Settings.model_fields.items() if f.is_required()}
    missing = required - _template_keys()
    assert not missing, f".env.example missing required settings: {sorted(missing)}"
