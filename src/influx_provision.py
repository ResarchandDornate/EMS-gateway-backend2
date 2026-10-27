"""Stage A-1: provision & verify the dedicated 3-year telemetry bucket.

One-time OPERATIONAL step, run explicitly (NOT wired into app startup):
    python -m src.influx_provision

Creates the configured bucket only if absent, then re-fetches and verifies its
enforced retention equals the configured seconds. Never updates, patches, or deletes
an existing bucket; fails visibly (non-zero exit) on any config/API/verification
error. The enforced duration (proposed 1095 d) is a configured value, not a
contract-confirmed production requirement.
"""
from influxdb_client import InfluxDBClient
from influxdb_client.domain.bucket_retention_rules import BucketRetentionRules

from src.config import get_settings


class BucketProvisionError(RuntimeError):
    """Provisioning / retention-verification failure (safe, self-authored message)."""


def ensure_t3_bucket(client, *, org: str, name: str, expect_seconds: int) -> None:
    """Create-if-absent then verify. Reads Bucket.retention_rules (list of
    BucketRetentionRules); the sole 'expire' rule's every_seconds must equal
    expect_seconds (unit: seconds). Never mutates an existing bucket."""
    api = client.buckets_api()
    bucket = api.find_bucket_by_name(name)              # raises on auth/network
    if bucket is None:
        api.create_bucket(
            bucket_name=name, org=org,
            retention_rules=[BucketRetentionRules(type="expire", every_seconds=expect_seconds)],
        )
        bucket = api.find_bucket_by_name(name)          # re-fetch; trust the store
        if bucket is None:
            raise BucketProvisionError(f"bucket {name!r} not found after creation")
    rules = [r for r in (bucket.retention_rules or []) if getattr(r, "type", None) == "expire"]
    if len(rules) != 1 or rules[0].every_seconds != expect_seconds:
        found = [getattr(r, "every_seconds", None) for r in rules]
        raise BucketProvisionError(
            f"bucket {name!r} retention {found}s != {expect_seconds}s; not modifying")


def main() -> int:
    s = get_settings()
    missing = [n for n in ("influx_url", "influx_token", "influx_org", "influx_bucket_3y")
               if not getattr(s, n, None)]
    if missing:
        print(f"FATAL: missing/empty configuration: {missing}")       # names only, never values
        return 2
    secs = getattr(s, "influx_bucket_3y_retention_seconds", None)
    if not isinstance(secs, int) or secs <= 0:
        print("FATAL: influx_bucket_3y_retention_seconds must be a positive integer")
        return 2
    client = InfluxDBClient(url=s.influx_url, token=s.influx_token, org=s.influx_org)
    try:
        ensure_t3_bucket(client, org=s.influx_org, name=s.influx_bucket_3y, expect_seconds=secs)
        print(f"OK: bucket '{s.influx_bucket_3y}' verified at {secs}s retention")
        return 0
    except BucketProvisionError as e:
        print(f"FATAL: {e}")                                          # safe message, no secrets
        return 1
    except Exception as e:
        print(f"FATAL: provisioning failed ({type(e).__name__})")     # type only; no token/body/url
        return 1
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
