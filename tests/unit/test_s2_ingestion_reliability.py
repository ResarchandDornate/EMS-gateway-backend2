"""Regression tests for S2 (InfluxDB-only) ingestion reliability.

Covers the forward-looking fix for messages becoming permanently stuck in the
`ingest_messages` ledger as `pending`:
  * explicit Influx client timeout + shared-client lifecycle (src/influx.py),
  * no silent swallowing of S2 write failures (src/ingestion/stream_service.py),
  * retryable / idempotent semantics preserved (never ack 'stored' without a confirmed
    write), batch per-message accepted/rejected accuracy, and unchanged S1/S3-S10 routing.

Pure unit tests: InfluxDB and the ledger (claim_or_resolve / mark_stored) are faked /
monkeypatched. No database, no network, no production credentials.
"""
import asyncio
import json
import time
import uuid
from types import SimpleNamespace

import pytest

from src.ingestion import stream_service as ss
from src.ingestion.service import IngestDecision, LEASE_TTL_SECONDS
from src.ingestion.health_service import StoreNotConfirmed


# --------------------------------------------------------------------------- fakes


class _Gw:
    def __init__(self):
        self.id = uuid.uuid4()


class _Env:
    """Minimal S2 envelope stand-in (only the attributes persist_influx_only reads)."""
    def __init__(self, stream="S2", message_id=None):
        self.stream = stream
        self.substream = "pcs_extended"
        self.message_id = message_id or uuid.uuid4()
        self.sequence = 1
        self.record_count = 1

    def model_dump(self, mode="json"):
        return {"schema_version": "1", "stream": self.stream, "substream": self.substream,
                "sequence": self.sequence, "created_at_utc": "2026-11-02T06:00:00Z",
                "record_count": self.record_count, "records": [{"signal": "x", "value": 1.0}]}


class _Influx:
    def __init__(self, behavior="ok"):
        self.behavior = behavior
        self.writes = 0

    async def write_built(self, points):
        self.writes += 1
        if self.behavior == "conn":
            raise ConnectionError("influx connection refused")
        if self.behavior == "timeout":
            raise asyncio.TimeoutError("influx write timed out")
        if self.behavior == "slow":
            await asyncio.sleep(0.01)
        # "ok" -> return normally


def _patch_ledger(monkeypatch, *, decision=IngestDecision.CLAIMED, stored=True,
                  mark_calls=None):
    token = uuid.uuid4()

    async def fake_claim(session, **kw):
        return decision, token

    async def fake_mark(session, **kw):
        if mark_calls is not None:
            mark_calls.append(kw)
        return stored

    monkeypatch.setattr(ss, "claim_or_resolve", fake_claim)
    monkeypatch.setattr(ss, "mark_stored", fake_mark)
    return token


_POINTS = [("telemetry_1y", object())]


# --------------------------------------------------------------- 1. success -> stored


async def test_s2_success_marks_stored(monkeypatch):
    mark_calls = []
    _patch_ledger(monkeypatch, decision=IngestDecision.CLAIMED, stored=True,
                  mark_calls=mark_calls)
    infl = _Influx("ok")
    d = await ss.persist_influx_only(None, gw=_Gw(), identity={}, envelope=_Env(),
                                     influx=infl, influx_points=_POINTS)
    assert d == IngestDecision.CLAIMED
    assert infl.writes == 1
    assert len(mark_calls) == 1          # mark_stored only after a successful write


# ----------------------------------------- 2. connection failure -> retryable, not stored


async def test_s2_connection_failure_not_stored(monkeypatch):
    mark_calls = []
    _patch_ledger(monkeypatch, stored=True, mark_calls=mark_calls)
    with pytest.raises(ConnectionError):
        await ss.persist_influx_only(None, gw=_Gw(), identity={}, envelope=_Env(),
                                     influx=_Influx("conn"), influx_points=_POINTS)
    assert mark_calls == []               # never marked stored -> row stays pending (retryable)


# --------------------------------------------- 3. write timeout -> no false stored confirm


async def test_s2_write_timeout_not_stored(monkeypatch):
    mark_calls = []
    _patch_ledger(monkeypatch, stored=True, mark_calls=mark_calls)
    with pytest.raises(asyncio.TimeoutError):
        await ss.persist_influx_only(None, gw=_Gw(), identity={}, envelope=_Env(),
                                     influx=_Influx("timeout"), influx_points=_POINTS)
    assert mark_calls == []


# ---------------------------------- 4. slow write: lease stays compatible with the timeout


def test_influx_timeout_below_lease_invariant():
    s = SimpleNamespace(influx_url="http://x", influx_token="t", influx_org="o",
                        influx_timeout_ms=20_000)
    from src.influx import InfluxGateway
    g = InfluxGateway(s)
    assert g._timeout_ms < LEASE_TTL_SECONDS * 1000   # a single write cannot outlive its lease


def test_config_rejects_timeout_at_or_above_lease():
    import pydantic
    from src.config import Settings
    with pytest.raises(pydantic.ValidationError):
        Settings(database_url="x", influx_url="x", influx_token="x", influx_org="x",
                 influx_bucket_raw="r", influx_bucket_long="l", redis_url="x",
                 jwt_secret="x", influx_timeout_ms=60_000)


async def test_slow_write_completes_and_stores(monkeypatch):
    mark_calls = []
    _patch_ledger(monkeypatch, stored=True, mark_calls=mark_calls)
    d = await ss.persist_influx_only(None, gw=_Gw(), identity={}, envelope=_Env(),
                                     influx=_Influx("slow"), influx_points=_POINTS)
    assert d == IngestDecision.CLAIMED and len(mark_calls) == 1


# ------------------------------------------ 5. concurrent duplicate -> safe, idempotent


@pytest.mark.parametrize("dec", [IngestDecision.DUPLICATE_STORED,
                                 IngestDecision.HASH_MISMATCH,
                                 IngestDecision.IN_PROGRESS])
async def test_s2_resolved_decisions_skip_write(monkeypatch, dec):
    _patch_ledger(monkeypatch, decision=dec)
    infl = _Influx("ok")
    d = await ss.persist_influx_only(None, gw=_Gw(), identity={}, envelope=_Env(),
                                     influx=infl, influx_points=_POINTS)
    assert d == dec
    assert infl.writes == 0               # no duplicate telemetry written on a resolved claim


# -------------------------------------- 6. retry after a temporary failure -> eventual store


async def test_s2_retry_succeeds_after_transient_failure(monkeypatch):
    env = _Env()
    _patch_ledger(monkeypatch, decision=IngestDecision.CLAIMED, stored=True)
    with pytest.raises(ConnectionError):
        await ss.persist_influx_only(None, gw=_Gw(), identity={}, envelope=env,
                                     influx=_Influx("conn"), influx_points=_POINTS)
    # retry of the same message reclaims and writes identical (idempotent) points
    _patch_ledger(monkeypatch, decision=IngestDecision.RECLAIMED, stored=True)
    d = await ss.persist_influx_only(None, gw=_Gw(), identity={}, envelope=env,
                                     influx=_Influx("ok"), influx_points=_POINTS)
    assert d == IngestDecision.RECLAIMED


# ------------------------------------ 7. mark_stored failure -> StoreNotConfirmed (no ack)


async def test_s2_mark_stored_false_raises(monkeypatch):
    _patch_ledger(monkeypatch, decision=IngestDecision.CLAIMED, stored=False)
    with pytest.raises(StoreNotConfirmed):
        await ss.persist_influx_only(None, gw=_Gw(), identity={}, envelope=_Env(),
                                     influx=_Influx("ok"), influx_points=_POINTS)


# ------------------------------- 8. batch with mixed outcomes -> accurate accepted/rejected


async def test_batch_mixed_accepted_rejected(monkeypatch):
    from src.ingestion import router as r

    async def fake_identity(session, gw):
        return {"gateway_id": "g", "site_id": "s", "organization_id": "o"}

    def fake_adapt(msg, identity):
        return {"stream": "S2", "message_id": msg["message_id"]}

    class FakeEnv:
        def __init__(self, **kw):
            self.message_id = kw["message_id"]
            self.stream = "S2"
            self.gateway_id = "g"
            self.site_id = "s"
            self.organization_id = "o"
            self.substream = "x"
            self.sequence = 1
            self.record_count = 1
            self.records = []

    async def fake_persist(session, *, gw, identity, envelope, influx, influx_points):
        if envelope.message_id == "bad":
            raise Exception("influx down")
        return IngestDecision.CLAIMED

    monkeypatch.setattr(r, "resolve_gateway_identity", fake_identity)
    monkeypatch.setattr(r, "adapt_batch_message", fake_adapt)
    monkeypatch.setattr(r, "GenericStreamEnvelopeIn", FakeEnv)
    monkeypatch.setattr(r, "verify_scope", lambda *a, **k: True)
    monkeypatch.setattr(r, "build_influx_points", lambda *a, **k: _POINTS)
    monkeypatch.setattr(r, "persist_influx_only", fake_persist)

    body = {"request_id": "rq", "gateway_id": "g", "sent_at_utc": "2026-11-02T06:00:00Z",
            "messages": [{"message_id": "good"}, {"message_id": "bad"}]}
    influx = SimpleNamespace(settings=None)   # build_influx_points reads influx.settings
    resp = await r._ingest_batch(body, session=object(), gw=_Gw(), influx=influx)
    data = json.loads(resp.body)
    assert {a["message_id"] for a in data["accepted"]} == {"good"}
    assert {x["message_id"] for x in data["rejected"]} == {"bad"}
    assert all(x["retryable"] for x in data["rejected"])     # failures stay retryable


# ------------------------------------------------ 9. S1 and S3-S10 routing unchanged


def test_batch_routing_map_unchanged():
    from src.ingestion.router import _BATCH_DEDICATED
    assert set(_BATCH_DEDICATED) == {"S3", "S4", "S5", "S6", "S7", "S8", "S9"}


# -------------------- 10. no startup/recovery code mutates the historical ledger


def test_lifespan_does_not_touch_ledger():
    import inspect
    import src.main as m
    src = inspect.getsource(m._lifespan)
    for forbidden in ("ingest_messages", "IngestMessageModel", "mark_stored",
                      "claim_or_resolve", "reclaim", "backfill"):
        assert forbidden not in src, f"_lifespan must not reference {forbidden!r}"


# ------------------------- influx client lifecycle: timeout + shared-client reuse (area A)


async def test_acquire_reuses_shared_client_without_closing():
    from src.influx import InfluxGateway

    class FakeClient:
        def __init__(self):
            self.closed = False

        async def close(self):
            self.closed = True

    shared = FakeClient()
    s = SimpleNamespace(influx_url="u", influx_token="t", influx_org="o",
                        influx_timeout_ms=20_000)
    g = InfluxGateway(s, client=shared)
    async with g._acquire() as c:
        assert c is shared
    assert shared.closed is False            # shared client must NOT be closed per call


async def test_acquire_creates_and_closes_per_call_with_timeout(monkeypatch):
    import src.influx as inflmod
    from src.influx import InfluxGateway
    created = []

    class FakeClient:
        def __init__(self, **kw):
            self.kw = kw
            self.closed = False
            created.append(self)

        async def close(self):
            self.closed = True

    monkeypatch.setattr(inflmod, "InfluxDBClientAsync", FakeClient)
    s = SimpleNamespace(influx_url="u", influx_token="t", influx_org="o",
                        influx_timeout_ms=15_000)
    g = InfluxGateway(s, client=None)
    async with g._acquire() as c:
        assert c is created[-1]
    assert created[-1].closed is True                 # per-call client is closed
    assert created[-1].kw.get("timeout") == 15_000    # explicit timeout passed through


# ============ forward-only in-request recovery (bounded, idempotent retry) ============

from src.telemetry_errors import UnsafeQueryInput   # noqa: E402


class _InfluxSeq:
    """Fake whose write_built follows a per-attempt behaviour sequence."""
    def __init__(self, seq, retries=2, budget_ms=5_000):
        self.seq = seq
        self.calls = 0
        self.points_seen = []
        self.settings = SimpleNamespace(influx_write_retries=retries,
                                        influx_retry_budget_ms=budget_ms)

    async def write_built(self, points):
        self.points_seen.append(points)
        b = self.seq[min(self.calls, len(self.seq) - 1)]
        self.calls += 1
        if b == "conn":
            raise ConnectionError("influx connection reset")
        if b == "timeout":
            raise asyncio.TimeoutError("influx write timed out")
        if b == "unsafe":
            raise UnsafeQueryInput("refusing write to non-configured bucket")
        # "ok" -> return


async def test_retry_recovers_after_transient_failures():
    infl = _InfluxSeq(["conn", "conn", "ok"], retries=2)
    await ss._write_s2_with_retry(infl, _POINTS, envelope=_Env(), gw=_Gw())
    assert infl.calls == 3                       # 1 + 2 retries, last succeeded


async def test_retry_end_to_end_marks_stored(monkeypatch):
    mark_calls = []
    _patch_ledger(monkeypatch, decision=IngestDecision.CLAIMED, stored=True,
                  mark_calls=mark_calls)
    infl = _InfluxSeq(["timeout", "ok"], retries=2)
    d = await ss.persist_influx_only(None, gw=_Gw(), identity={}, envelope=_Env(),
                                     influx=infl, influx_points=_POINTS)
    assert d == IngestDecision.CLAIMED
    assert infl.calls == 2 and len(mark_calls) == 1   # recovered, then stored


async def test_permanent_failure_exhausts_retries_and_stays_retryable():
    infl = _InfluxSeq(["conn"] * 6, retries=2)
    with pytest.raises(ConnectionError):
        await ss._write_s2_with_retry(infl, _POINTS, envelope=_Env(), gw=_Gw())
    assert infl.calls == 3                        # bounded: exactly 1 + 2 retries


async def test_permanent_failure_not_marked_stored(monkeypatch):
    mark_calls = []
    _patch_ledger(monkeypatch, decision=IngestDecision.CLAIMED, stored=True,
                  mark_calls=mark_calls)
    with pytest.raises(ConnectionError):
        await ss.persist_influx_only(None, gw=_Gw(), identity={}, envelope=_Env(),
                                     influx=_InfluxSeq(["conn"] * 6, retries=2),
                                     influx_points=_POINTS)
    assert mark_calls == []                       # never acked without a confirmed write


async def test_nonretryable_config_error_not_retried():
    infl = _InfluxSeq(["unsafe", "ok"], retries=5)
    with pytest.raises(UnsafeQueryInput):
        await ss._write_s2_with_retry(infl, _POINTS, envelope=_Env(), gw=_Gw())
    assert infl.calls == 1                        # deterministic error -> no retry


async def test_retries_zero_disables_retry():
    infl = _InfluxSeq(["conn", "ok"], retries=0, budget_ms=5_000)
    with pytest.raises(ConnectionError):
        await ss._write_s2_with_retry(infl, _POINTS, envelope=_Env(), gw=_Gw())
    assert infl.calls == 1                        # retries=0 -> single attempt


# ---- error classification (influxdb-client 1.50.0 actual hierarchy) ------------------

from influxdb_client.client.exceptions import InfluxDBError   # noqa: E402


def _influx_http_error(status):
    """Fabricate an InfluxDBError with a given HTTP status (bypass __init__)."""
    e = InfluxDBError.__new__(InfluxDBError)
    e.response = SimpleNamespace(status=status, headers={})
    e.message = f"http {status}"
    e.retry_after = None
    return e


def test_classify_permanent_statuses():
    for st in (400, 401, 403, 404, 409, 413, 422):
        assert ss._classify_write_error(_influx_http_error(st)) == "permanent"


def test_classify_transient_statuses_and_network():
    for st in (429, 500, 502, 503, 504):
        assert ss._classify_write_error(_influx_http_error(st)) == "transient"
    assert ss._classify_write_error(_influx_http_error(None)) == "transient"   # no HTTP status
    assert ss._classify_write_error(ConnectionError()) == "transient"
    assert ss._classify_write_error(asyncio.TimeoutError()) == "transient"
    assert ss._classify_write_error(TimeoutError()) == "transient"
    assert ss._classify_write_error(OSError()) == "transient"


def test_classify_local_validation_permanent():
    assert ss._classify_write_error(UnsafeQueryInput("bad bucket")) == "permanent"


def test_classify_unknown_defaults_permanent():
    for exc in (RuntimeError("boom"), ValueError("x"), KeyError("k")):
        assert ss._classify_write_error(exc) == "permanent"   # fail fast, do not mask defects


class _InfluxRaise:
    def __init__(self, exc, retries=3, budget_ms=5_000):
        self.exc = exc
        self.calls = 0
        self.settings = SimpleNamespace(influx_write_retries=retries,
                                        influx_retry_budget_ms=budget_ms,
                                        influx_timeout_ms=20_000)

    async def write_built(self, points):
        self.calls += 1
        raise self.exc


async def test_auth_error_not_retried():
    infl = _InfluxRaise(_influx_http_error(401))
    with pytest.raises(InfluxDBError):
        await ss._write_s2_with_retry(infl, _POINTS, envelope=_Env(), gw=_Gw())
    assert infl.calls == 1                        # 401 -> permanent -> no retry


async def test_permanent_validation_not_retried():
    infl = _InfluxRaise(UnsafeQueryInput("refusing non-configured bucket"))
    with pytest.raises(UnsafeQueryInput):
        await ss._write_s2_with_retry(infl, _POINTS, envelope=_Env(), gw=_Gw())
    assert infl.calls == 1


async def test_unknown_error_not_retried():
    infl = _InfluxRaise(RuntimeError("unexpected"))
    with pytest.raises(RuntimeError):
        await ss._write_s2_with_retry(infl, _POINTS, envelope=_Env(), gw=_Gw())
    assert infl.calls == 1                        # unknown -> permanent -> single attempt


async def test_cancellation_propagates_without_retry():
    infl = _InfluxRaise(asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await ss._write_s2_with_retry(infl, _POINTS, envelope=_Env(), gw=_Gw())
    assert infl.calls == 1                        # cancellation never retried


class _InfluxSlow:
    def __init__(self, sleep_s, *, retries=3, budget_ms=100, timeout_ms=20_000):
        self.sleep_s = sleep_s
        self.calls = 0
        self.completed = 0
        self.settings = SimpleNamespace(influx_write_retries=retries,
                                        influx_retry_budget_ms=budget_ms,
                                        influx_timeout_ms=timeout_ms)

    async def write_built(self, points):
        self.calls += 1
        await asyncio.sleep(self.sleep_s)
        self.completed += 1


async def test_slow_attempt_cancelled_at_total_deadline():
    infl = _InfluxSlow(2.0, retries=3, budget_ms=100)   # 100 ms total deadline
    start = time.monotonic()
    with pytest.raises((asyncio.TimeoutError, TimeoutError)):
        await ss._write_s2_with_retry(infl, _POINTS, envelope=_Env(), gw=_Gw())
    elapsed = time.monotonic() - start
    assert infl.calls == 1 and infl.completed == 0   # cancelled mid-write, never completed
    assert elapsed < 1.0                             # cut at ~0.1 s deadline, not 2 s


async def test_budget_expires_before_another_attempt():
    infl = _InfluxSeq(["conn", "ok"], retries=5, budget_ms=1)   # 1 ms budget: no room to retry
    with pytest.raises((ConnectionError, TimeoutError)):
        await ss._write_s2_with_retry(infl, _POINTS, envelope=_Env(), gw=_Gw())
    assert infl.calls == 1


# ---- multi-bucket write safety: retry re-sends ALL buckets (idempotent, last-write-wins) ---

class _WApi:
    def __init__(self, fail_bucket, fail_times=1):
        self.writes = []
        self.fail_bucket = fail_bucket
        self.fail_times = fail_times
        self._f = 0

    async def write(self, bucket, record):
        self.writes.append(bucket)
        if bucket == self.fail_bucket and self._f < self.fail_times:
            self._f += 1
            raise ConnectionError(f"{bucket} transient down")


class _Cli:
    def __init__(self, wapi):
        self._w = wapi

    def write_api(self):
        return self._w

    async def close(self):
        pass


async def test_multibucket_partial_success_then_retry_resends_all():
    from src.influx import InfluxGateway
    wapi = _WApi(fail_bucket="telemetry_3y", fail_times=1)
    s = SimpleNamespace(influx_url="u", influx_token="t", influx_org="o",
                        influx_timeout_ms=20_000, influx_write_retries=2,
                        influx_retry_budget_ms=5_000,
                        influx_bucket_raw="telemetry_raw_90d",
                        influx_bucket_long="telemetry_1y",
                        influx_bucket_3y="telemetry_3y")
    g = InfluxGateway(s, client=_Cli(wapi))
    built = [("telemetry_1y", object()), ("telemetry_3y", object())]
    await ss._write_s2_with_retry(g, built, envelope=_Env(), gw=_Gw())
    # attempt 1: 1y ok, 3y fails -> whole write_built raises; attempt 2: 1y + 3y both ok.
    # The already-succeeded 1y bucket is re-sent on retry -> safe only via last-write-wins.
    assert wapi.writes.count("telemetry_1y") == 2
    assert wapi.writes.count("telemetry_3y") == 2


async def test_retry_is_idempotent_same_points_each_attempt():
    infl = _InfluxSeq(["conn", "conn", "ok"], retries=2)
    await ss._write_s2_with_retry(infl, _POINTS, envelope=_Env(), gw=_Gw())
    assert all(p is _POINTS for p in infl.points_seen)   # identical deterministic payload


def test_config_rejects_retry_budget_at_or_above_lease():
    import pydantic
    from src.config import Settings
    with pytest.raises(pydantic.ValidationError):
        Settings(database_url="x", influx_url="x", influx_token="x", influx_org="x",
                 influx_bucket_raw="r", influx_bucket_long="l", redis_url="x",
                 jwt_secret="x", influx_retry_budget_ms=60_000)


def test_recovery_is_in_request_only_no_worker_no_ledger_scan():
    """The recovery mechanism must not scan or mutate the ledger and must not run a
    background worker -> the 95 historical pending rows cannot be touched by it."""
    import inspect
    import src.ingestion.stream_service as m
    import src.main as mn
    # Strip the docstring; assert on CODE only (prose legitimately discusses the ledger).
    helper = inspect.getsource(m._write_s2_with_retry)
    code = helper.split('"""')[-1] if '"""' in helper else helper
    for forbidden in ("IngestMessageModel", "ingest_messages", "select(", "update(",
                      "create_task", "claim_or_resolve", "mark_stored"):
        assert forbidden not in code, f"recovery helper must not reference {forbidden!r}"
    assert "create_task" not in inspect.getsource(m)          # no background worker
    assert "create_task" not in inspect.getsource(mn._lifespan)
