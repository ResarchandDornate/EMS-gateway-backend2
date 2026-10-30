import pytest

from src.influx_provision import BucketProvisionError, ensure_t3_bucket, main

SECS = 94_608_000


class _Rule:
    def __init__(self, type="expire", every_seconds=SECS):
        self.type = type
        self.every_seconds = every_seconds


class _Bucket:
    def __init__(self, rules):
        self.retention_rules = rules


class _Api:
    def __init__(self, existing=None, create_result=None,
                 find_raises=False, create_raises=False):
        self._existing = existing
        self._create_result = create_result
        self._find_raises = find_raises
        self._create_raises = create_raises
        self.created = []
        self.updated = []
        self.deleted = []

    def find_bucket_by_name(self, name):
        if self._find_raises:
            raise RuntimeError("find boom")
        if self.created and self._create_result is not None:
            return self._create_result
        return self._existing

    def create_bucket(self, bucket_name=None, org=None, retention_rules=None):
        if self._create_raises:
            raise RuntimeError("create boom")
        self.created.append((bucket_name, org, retention_rules))
        return self._create_result

    def update_bucket(self, *a, **k):
        self.updated.append((a, k))          # must never be called

    def delete_bucket(self, *a, **k):
        self.deleted.append((a, k))          # must never be called


class _Client:
    def __init__(self, api):
        self._api = api

    def buckets_api(self):
        return self._api


def test_t3_correct_existing_retention_passes():
    api = _Api(existing=_Bucket([_Rule()]))
    ensure_t3_bucket(_Client(api), org="o", name="telemetry_3y", expect_seconds=SECS)
    assert api.created == [] and api.updated == [] and api.deleted == []


def test_t3_wrong_retention_fails_without_mutation():
    api = _Api(existing=_Bucket([_Rule(every_seconds=31_536_000)]))
    with pytest.raises(BucketProvisionError):
        ensure_t3_bucket(_Client(api), org="o", name="telemetry_3y", expect_seconds=SECS)
    assert api.created == [] and api.updated == [] and api.deleted == []


def test_t3_multiple_expire_rules_fail():
    api = _Api(existing=_Bucket([_Rule(), _Rule()]))
    with pytest.raises(BucketProvisionError):
        ensure_t3_bucket(_Client(api), org="o", name="telemetry_3y", expect_seconds=SECS)
    assert api.updated == [] and api.deleted == []


def test_t3_missing_bucket_created_with_configured_duration_then_verified():
    api = _Api(existing=None, create_result=_Bucket([_Rule()]))
    ensure_t3_bucket(_Client(api), org="o", name="telemetry_3y", expect_seconds=SECS)
    assert len(api.created) == 1
    name, org, rules = api.created[0]
    assert name == "telemetry_3y" and org == "o"
    assert len(rules) == 1 and rules[0].type == "expire" and rules[0].every_seconds == SECS


def test_t3_missing_after_create_fails():
    api = _Api(existing=None, create_result=None)   # re-fetch still None
    with pytest.raises(BucketProvisionError):
        ensure_t3_bucket(_Client(api), org="o", name="telemetry_3y", expect_seconds=SECS)


def test_t3_created_but_wrong_retention_fails():
    api = _Api(existing=None, create_result=_Bucket([_Rule(every_seconds=1)]))
    with pytest.raises(BucketProvisionError):
        ensure_t3_bucket(_Client(api), org="o", name="telemetry_3y", expect_seconds=SECS)
    assert api.updated == [] and api.deleted == []


def test_t3_find_api_error_propagates():
    api = _Api(find_raises=True)
    with pytest.raises(RuntimeError):
        ensure_t3_bucket(_Client(api), org="o", name="telemetry_3y", expect_seconds=SECS)


def test_t3_create_api_error_propagates():
    api = _Api(existing=None, create_raises=True)
    with pytest.raises(RuntimeError):
        ensure_t3_bucket(_Client(api), org="o", name="telemetry_3y", expect_seconds=SECS)


def _settings(**over):
    import types
    base = dict(influx_url="http://secret-host:8086", influx_token="SECRET-TOKEN",
                influx_org="o", influx_bucket_3y="telemetry_3y",
                influx_bucket_3y_retention_seconds=SECS)
    base.update(over)
    return types.SimpleNamespace(**base)


def _patch(monkeypatch, settings, api, closed):
    import src.influx_provision as mod

    class _FakeClient:
        def __init__(self, *a, **k):
            pass

        def buckets_api(self):
            return api

        def close(self):
            closed.append(1)

    monkeypatch.setattr(mod, "get_settings", lambda: settings)
    monkeypatch.setattr(mod, "InfluxDBClient", _FakeClient)
    return mod


def test_main_success_closes_client_and_hides_secrets(monkeypatch, capsys):
    closed = []
    mod = _patch(monkeypatch, _settings(), _Api(existing=_Bucket([_Rule()])), closed)
    rc = mod.main()
    out = capsys.readouterr().out
    assert rc == 0 and closed == [1]
    assert "SECRET-TOKEN" not in out and "secret-host" not in out


def test_main_verification_failure_closes_client_and_hides_secrets(monkeypatch, capsys):
    closed = []
    mod = _patch(monkeypatch, _settings(), _Api(existing=_Bucket([_Rule(every_seconds=1)])), closed)
    rc = mod.main()
    out = capsys.readouterr().out
    assert rc == 1 and closed == [1]
    assert "SECRET-TOKEN" not in out and "secret-host" not in out


def test_main_api_error_closes_client_and_hides_details(monkeypatch, capsys):
    closed = []
    mod = _patch(monkeypatch, _settings(), _Api(find_raises=True), closed)
    rc = mod.main()
    out = capsys.readouterr().out
    assert rc == 1 and closed == [1]
    assert "SECRET-TOKEN" not in out and "find boom" not in out


def test_main_rejects_invalid_config_without_connecting(monkeypatch, capsys):
    import src.influx_provision as mod
    monkeypatch.setattr(mod, "get_settings", lambda: _settings(influx_url=""))

    def _boom(*a, **k):
        raise AssertionError("must not connect on invalid config")

    monkeypatch.setattr(mod, "InfluxDBClient", _boom)
    assert mod.main() == 2


def test_main_rejects_nonpositive_retention(monkeypatch):
    import src.influx_provision as mod
    monkeypatch.setattr(mod, "get_settings",
                        lambda: _settings(influx_bucket_3y_retention_seconds=0))
    monkeypatch.setattr(mod, "InfluxDBClient",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not connect")))
    assert mod.main() == 2


def test_settings_reject_blank_3y_bucket_and_bad_retention():
    from pydantic import ValidationError
    from src.config import Settings, get_settings
    base = get_settings().model_dump()
    with pytest.raises(ValidationError):
        Settings.model_validate({**base, "influx_bucket_3y": "   "})
    with pytest.raises(ValidationError):
        Settings.model_validate({**base, "influx_bucket_3y_retention_seconds": 0})
