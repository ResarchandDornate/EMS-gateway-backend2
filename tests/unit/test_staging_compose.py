"""Config guards for the isolated staging stack (text-based; no YAML dependency). Protect the
isolation + safety assumptions in docker-compose.staging.yml against regressions."""
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
C = (_ROOT / "docker-compose.staging.yml").read_text()


def test_distinct_project_name():
    assert "name: ems-gateway-staging" in C
    assert "ems-gateway-backend2" not in C                 # never references the production project


def test_serves_asgi_app_not_bare_app():
    assert "src.main:asgi_app" in C
    assert "src.main:app " not in C and 'src.main:app"' not in C


def test_ingest_disabled_by_default():
    assert 'INGEST_ENABLED: "false"' in C


def test_no_cloudflare_tunnel():
    low = C.lower()
    assert "cloudflared" not in low and "tunnel" not in low


def test_staging_ports_do_not_collide_with_production():
    for staging_port in ("5437:5432", "8087:8086", "6381:6379", "8041:8000"):
        assert staging_port in C
    for prod_port in ("5436:5432", "8086:8086", "6380:6379", "8040:8000"):
        assert prod_port not in C                          # production host ports must not appear


def test_volumes_are_isolated():
    for v in ("pgdata_staging", "influxdata_staging", "redisdata_staging"):
        assert v in C
    assert "pgdata_staging:/var/lib/postgresql/data" in C


def test_has_dedicated_network():
    assert "ems-staging-net" in C


def test_cors_explicit_not_wildcard():
    assert "CORS_ALLOWED_ORIGINS" in C
    assert "*" not in C.split("CORS_ALLOWED_ORIGINS")[1].split("\n")[0]   # no wildcard on that line
