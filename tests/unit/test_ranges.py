"""Unit tests for the shared time-range grammar + bucket math (no DB, no FastAPI)."""
from datetime import datetime, timedelta, timezone

import pytest

from src.structured_reads import ranges as R

UTC = timezone.utc
NOW = datetime(2026, 11, 1, 15, 30, tzinfo=UTC)   # a Sunday


def test_rolling_days_and_weeks():
    assert R.resolve_range("-7d", None, None, NOW)[0] == NOW - timedelta(days=7)
    assert R.resolve_range("-4w", None, None, NOW)[0] == NOW - timedelta(weeks=4)


def test_rolling_months_calendar():
    assert R.resolve_range("-6mo", None, None, NOW)[0] == datetime(2026, 5, 1, 15, 30, tzinfo=UTC)


def test_rolling_year_leap_clamp():
    now = datetime(2024, 2, 29, 12, 0, tzinfo=UTC)
    assert R.resolve_range("-1y", None, None, now)[0] == datetime(2023, 2, 28, 12, 0, tzinfo=UTC)


def test_calendar_week_month_year():
    assert R.resolve_range(None, None, "this_week", NOW)[0] == datetime(2026, 10, 26, tzinfo=UTC)  # Monday
    s, e = R.resolve_range(None, None, "this_month", NOW)
    assert s == datetime(2026, 11, 1, tzinfo=UTC) and e == NOW                                      # period-to-date
    assert R.resolve_range(None, None, "this_year", NOW)[0] == datetime(2026, 1, 1, tzinfo=UTC)


def test_absolute_iso_preserved():
    s, e = R.resolve_range("2026-09-01T00:00:00Z", "2026-09-02T00:00:00Z", None, NOW)
    assert s == datetime(2026, 9, 1, tzinfo=UTC) and e == datetime(2026, 9, 2, tzinfo=UTC)


def test_unbounded_when_nothing_given():
    assert R.resolve_range(None, None, None, NOW) == (None, None)


def test_conflicts_and_invalids():
    with pytest.raises(R.RangeError): R.resolve_range("-7d", None, "this_month", NOW)   # conflict
    with pytest.raises(R.RangeError): R.resolve_range("banana", None, None, NOW)         # bad token
    with pytest.raises(R.RangeError): R.resolve_range(None, None, "this_decade", NOW)    # bad keyword
    with pytest.raises(R.RangeError):
        R.resolve_range("2026-09-02T00:00:00Z", "2026-09-01T00:00:00Z", None, NOW)       # start>=stop


def test_bounded_for_summaries():
    with pytest.raises(R.RangeError): R.resolve_bounded(None, None, None, NOW)           # needs lower bound
    assert R.resolve_bounded("-1d", None, None, NOW)[1] == NOW                            # stop defaults now


def test_unit_validation():
    assert R.validate_unit("month") == "month"
    with pytest.raises(R.RangeError): R.validate_unit("fortnight")


def test_trunc_and_step_calendar():
    assert R.trunc(datetime(2026, 11, 1, 15, tzinfo=UTC), "week") == datetime(2026, 10, 26, tzinfo=UTC)
    assert R.trunc(NOW, "month") == datetime(2026, 11, 1, tzinfo=UTC)
    assert R.trunc(NOW, "year") == datetime(2026, 1, 1, tzinfo=UTC)
    assert R.step(datetime(2026, 1, 31, tzinfo=UTC), "month") == datetime(2026, 2, 28, tzinfo=UTC)
    assert R.step(datetime(2026, 1, 1, tzinfo=UTC), "year") == datetime(2027, 1, 1, tzinfo=UTC)


def test_enumerate_buckets_stop_exclusive_and_cap():
    a = R.enumerate_buckets(datetime(2026, 9, 1, 0, tzinfo=UTC),
                            datetime(2026, 9, 1, 2, tzinfo=UTC), "hour")
    assert a == [datetime(2026, 9, 1, 0, tzinfo=UTC), datetime(2026, 9, 1, 1, tzinfo=UTC)]  # not 02:00
    assert len(R.enumerate_buckets(datetime(2026, 9, 1, tzinfo=UTC),
                                   datetime(2026, 9, 4, tzinfo=UTC), "day")) == 3
    with pytest.raises(R.RangeError):
        R.enumerate_buckets(datetime(2000, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC),
                            "hour", max_buckets=1000)
