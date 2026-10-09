"""
tests/test_pipeline_health.py

GET /health/pipeline (api/routes/pipeline_health.py) and the NYSE calendar
it relies on (pipeline/market_calendar.py). Time is frozen by patching
pipeline_health._now; Redis is a MagicMock serving a dict.

All instants are UTC. October 2026 is EDT (session 13:30-20:00 UTC);
late November 2026 is EST (session 14:30-21:00 UTC).
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

import api.routes.pipeline_health as ph
from pipeline import market_calendar as cal


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _reset_cache():
    ph._cache = None
    yield
    ph._cache = None


# ---------------------------------------------------------------------------
# Fixtures: Redis state builders
# ---------------------------------------------------------------------------


def _state(
    now: datetime,
    *,
    ages_min: dict[str, float | None] | None = None,
    success_at: dict[str, datetime | None] | None = None,
    tick: dict | None = None,
    counts: dict[str, tuple[int, int]] | None = None,
) -> dict[str, str]:
    """Redis key → value. Defaults describe a healthy pipeline at ``now``."""
    ages = {
        "scoring_tick": 10,
        "narrative": 30,
        "market": 10,
        "macro_intraday": 40,
        "influencer": 180,
        "macro_daily": 600,
    }
    ages.update(ages_min or {})
    at: dict[str, datetime | None] = {
        job: (None if a is None else now - timedelta(minutes=a)) for job, a in ages.items()
    }
    # Once-per-session jobs: default = ran after the most recent close.
    close = cal.last_session_close(now)
    at["market_eod"] = close + timedelta(minutes=76)
    at["short_volume"] = close + timedelta(minutes=91)
    if at["market_eod"] > now:  # today's not run yet → the previous session's
        prev = cal.last_session_close(close - timedelta(seconds=1))
        at["market_eod"] = prev + timedelta(minutes=76)
        at["short_volume"] = prev + timedelta(minutes=91)
    at.update(success_at or {})

    store: dict[str, str] = {}
    for job, ts in at.items():
        if ts is not None:
            store[f"pipeline:last_run:{job}"] = ts.isoformat()
            store[f"pipeline:last_start:{job}"] = (ts - timedelta(minutes=5)).isoformat()
    for job, (ok, total) in (counts or {}).items():
        store[f"pipeline:last_run_counts:{job}"] = json.dumps(
            {"tickers_ok": ok, "tickers_total": total}
        )
    tick_summary = {
        "at": (now - timedelta(minutes=ages["scoring_tick"] or 0)).isoformat(),
        "tickers_scored": 586,
        "active_universe": 586,
        "missing_narrative": 6,
    }
    tick_summary.update(tick or {})
    store[ph.TICK_SUMMARY_KEY] = json.dumps(tick_summary)
    return store


def _redis(store: dict[str, str]) -> MagicMock:
    client = MagicMock()
    client.mget = AsyncMock(side_effect=lambda keys: [store.get(k) for k in keys])
    client.eval = AsyncMock(return_value=1)
    return client


def _get(now: datetime, client: MagicMock, headers: dict | None = None):
    from main import app

    with (
        patch.object(ph, "_now", return_value=now),
        patch.object(ph, "get_redis", return_value=client),
    ):
        r = TestClient(app).get("/health/pipeline", headers=headers or {})
    return r.status_code, r.json()


# ---------------------------------------------------------------------------
# Healthy states across the calendar
# ---------------------------------------------------------------------------


def test_in_session_healthy():
    now = _utc(2026, 10, 8, 15, 0)  # Thu 11:00 ET
    code, body = _get(now, _redis(_state(now)))
    assert code == 200
    assert body["status"] == "ok"
    assert body["reasons"] == []
    assert body["market_open"] is True
    assert body["checked_at"] == "2026-10-08T15:00:00Z"
    jobs = body["jobs"]
    assert jobs["scoring_tick"]["limit_minutes"] == 45
    assert jobs["market"] == {
        "last_success": "2026-10-08T14:50:00Z",
        "last_start": "2026-10-08T14:45:00Z",
        "minutes_since": 10,
        "limit_minutes": 45,
        "stale": False,
    }
    assert jobs["macro_intraday"]["limit_minutes"] == 90
    assert jobs["influencer"]["limit_minutes"] == 420
    assert jobs["macro_daily"]["limit_minutes"] == 1560
    assert body["latest_tick"] == {
        "at": "2026-10-08T14:50:00Z",
        "tickers_scored": 586,
        "active_universe": 586,
        "share_missing_narrative": 0.01,
    }


def test_overnight_healthy():
    now = _utc(2026, 10, 9, 0, 55)  # Thu 20:55 ET
    code, body = _get(now, _redis(_state(now, ages_min={"scoring_tick": 24, "market": 250})))
    assert code == 200, body["reasons"]
    assert body["status"] == "ok"
    assert body["market_open"] is False
    assert body["jobs"]["scoring_tick"]["limit_minutes"] == 75
    assert body["jobs"]["market"] == {
        "last_success": "2026-10-08T20:45:00Z",
        "last_start": "2026-10-08T20:40:00Z",
        "minutes_since": None,
        "limit_minutes": 45,
        "stale": False,
        "note": "market closed",
    }
    assert body["jobs"]["market_eod"]["stale"] is False
    assert body["jobs"]["market_eod"]["last_success"] == "2026-10-08T21:16:00Z"


def test_weekend_market_jobs_not_expected():
    now = _utc(2026, 10, 11, 15, 0)  # Sunday
    # Market/macro-intraday last ran Friday afternoon (~42 h ago).
    store = _state(now, ages_min={"market": 42 * 60, "macro_intraday": 43 * 60})
    code, body = _get(now, _redis(store))
    assert code == 200, body["reasons"]
    assert body["market_open"] is False
    for job in ("market", "macro_intraday"):
        assert body["jobs"][job]["stale"] is False
        assert body["jobs"][job]["note"] == "market closed"
    # Friday's end-of-day run satisfies the weekend.
    assert body["jobs"]["market_eod"]["last_success"] == "2026-10-09T21:16:00Z"
    assert body["jobs"]["market_eod"]["stale"] is False


def test_holiday_market_jobs_not_expected():
    now = _utc(2026, 11, 26, 16, 0)  # Thanksgiving, 11:00 ET — would be in session
    store = _state(now, ages_min={"market": 19 * 60, "scoring_tick": 60})
    code, body = _get(now, _redis(store))
    assert code == 200, body["reasons"]
    assert body["market_open"] is False
    assert body["jobs"]["market"]["note"] == "market closed"
    assert body["jobs"]["scoring_tick"]["limit_minutes"] == 75  # outside-session limit
    # Wednesday 11-25 (EST close 21:00 UTC) is the session the EOD check uses.
    assert body["jobs"]["market_eod"]["last_success"] == "2026-11-25T22:16:00Z"


def test_eod_not_due_between_close_and_deadline():
    now = _utc(2026, 10, 8, 21, 30)  # EDT close 20:00; EOD due by 21:45
    store = _state(now, success_at={"market_eod": _utc(2026, 10, 7, 21, 16)})
    code, body = _get(now, _redis(store))
    assert body["jobs"]["market_eod"]["stale"] is False
    assert code == 200, body["reasons"]


def test_eod_stale_after_deadline():
    now = _utc(2026, 10, 8, 21, 50)
    store = _state(now, success_at={"market_eod": _utc(2026, 10, 7, 21, 16)})
    code, body = _get(now, _redis(store))
    assert code == 503
    assert body["status"] == "degraded"
    assert body["jobs"]["market_eod"]["stale"] is True
    assert body["reasons"] == [
        "end-of-day market job has not succeeded since the 2026-10-08 session close "
        "(due by 21:45 UTC)"
    ]


def test_short_volume_deadline_is_2230_utc():
    stale_sv = {"short_volume": _utc(2026, 10, 7, 21, 31)}
    before = _utc(2026, 10, 8, 22, 20)
    _, body = _get(before, _redis(_state(before, success_at=stale_sv)))
    assert body["jobs"]["short_volume"]["stale"] is False
    ph._cache = None
    after = _utc(2026, 10, 8, 22, 40)
    _, body = _get(after, _redis(_state(after, success_at=stale_sv)))
    assert body["jobs"]["short_volume"]["stale"] is True


# ---------------------------------------------------------------------------
# Failure states
# ---------------------------------------------------------------------------


def test_hung_scoring_tick_is_down():
    now = _utc(2026, 10, 8, 15, 0)
    code, body = _get(now, _redis(_state(now, ages_min={"scoring_tick": 180})))
    assert code == 503
    assert body["status"] == "down"
    assert body["jobs"]["scoring_tick"]["stale"] is True
    assert "scoring tick last succeeded 3h ago (limit 45m)" in body["reasons"]
    assert "newest score is 3h old (limit 2h)" in body["reasons"]


def test_scoring_tick_never_recorded_is_down():
    now = _utc(2026, 10, 8, 15, 0)
    store = _state(now)
    del store["pipeline:last_run:scoring_tick"]
    code, body = _get(now, _redis(store))
    assert (code, body["status"]) == (503, "down")
    assert "scoring tick has no recorded success" in body["reasons"]


def test_missing_tick_summary_is_down():
    now = _utc(2026, 10, 8, 15, 0)
    store = _state(now)
    del store[ph.TICK_SUMMARY_KEY]
    code, body = _get(now, _redis(store))
    assert (code, body["status"]) == (503, "down")
    assert body["latest_tick"] is None
    assert body["reasons"] == ["no scoring tick summary recorded"]


def test_scoring_tick_past_limit_but_under_2h_is_degraded():
    now = _utc(2026, 10, 9, 0, 55)  # outside session, limit 75
    code, body = _get(now, _redis(_state(now, ages_min={"scoring_tick": 90, "market": 250})))
    assert (code, body["status"]) == (503, "degraded")


def test_hung_narrative_is_degraded():
    now = _utc(2026, 10, 8, 15, 0)
    code, body = _get(now, _redis(_state(now, ages_min={"narrative": 190})))
    assert code == 503
    assert body["status"] == "degraded"
    assert body["jobs"]["narrative"]["stale"] is True
    assert body["reasons"] == ["narrative job last succeeded 3h 10m ago (limit 1h 30m)"]


def test_quarter_missing_narrative_is_degraded():
    now = _utc(2026, 10, 8, 15, 0)
    tick = {"tickers_scored": 584, "active_universe": 586, "missing_narrative": 146}
    code, body = _get(now, _redis(_state(now, tick=tick)))
    assert code == 503
    assert body["status"] == "degraded"
    assert body["latest_tick"]["share_missing_narrative"] == 0.25
    assert body["reasons"] == [
        "25% of the latest tick is missing the narrative channel (limit 20%)"
    ]


def test_low_coverage_is_degraded():
    now = _utc(2026, 10, 8, 15, 0)
    tick = {"tickers_scored": 500, "active_universe": 586, "missing_narrative": 0}
    code, body = _get(now, _redis(_state(now, tick=tick)))
    assert (code, body["status"]) == (503, "degraded")
    assert body["reasons"] == ["latest tick scored 500 of 586 active tickers (85%, minimum 95%)"]


def test_redis_unreachable_is_down_without_exception():
    now = _utc(2026, 10, 8, 15, 0)
    client = MagicMock()
    client.mget = AsyncMock(side_effect=ConnectionError("redis://user:pw@10.0.0.1:6379 refused"))
    client.eval = AsyncMock(side_effect=ConnectionError("down"))
    code, body = _get(now, client)
    assert code == 503
    assert body["status"] == "down"
    assert body["reasons"] == ["state store unreachable"]
    assert "redis://" not in json.dumps(body)


def test_get_redis_not_initialised_is_down():
    now = _utc(2026, 10, 8, 15, 0)
    from main import app

    with (
        patch.object(ph, "_now", return_value=now),
        patch.object(ph, "get_redis", side_effect=RuntimeError("not initialised")),
    ):
        r = TestClient(app).get("/health/pipeline")
    assert r.status_code == 503
    assert r.json()["reasons"] == ["state store unreachable"]


# ---------------------------------------------------------------------------
# Response hygiene
# ---------------------------------------------------------------------------

_ALLOWED_TOP = {"status", "checked_at", "market_open", "jobs", "latest_tick", "reasons"}
_ALLOWED_JOB = {
    "last_success",
    "last_start",
    "minutes_since",
    "limit_minutes",
    "stale",
    "note",
    "tickers_ok",
    "tickers_total",
}
_ALLOWED_TICK = {"at", "tickers_scored", "active_universe", "share_missing_narrative"}
_SECRET_RE = re.compile(
    r"sk-|redis://|rediss://|postgres|password|secret|token|traceback|\.railway\.|"
    r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b",
    re.IGNORECASE,
)


def test_response_contains_no_secret_like_fields():
    now = _utc(2026, 10, 8, 15, 0)
    store = _state(now, ages_min={"narrative": 200}, counts={"market": (581, 586)})
    # Junk planted in Redis must not be echoed back.
    store["pipeline:last_run_counts:narrative"] = json.dumps(
        {"tickers_ok": 1, "tickers_total": 2, "error": "redis://u:secret@10.1.2.3"}
    )
    tick = json.loads(store[ph.TICK_SUMMARY_KEY])
    tick["api_key"] = "sk-sm-prod-abcdef"
    store[ph.TICK_SUMMARY_KEY] = json.dumps(tick)
    store["pipeline:last_run:influencer"] = "Traceback (most recent call last)"

    _, body = _get(now, _redis(store))
    assert set(body) == _ALLOWED_TOP
    for entry in body["jobs"].values():
        assert set(entry) <= _ALLOWED_JOB
    assert set(body["latest_tick"]) == _ALLOWED_TICK
    assert not _SECRET_RE.search(json.dumps(body)), json.dumps(body)
    assert body["jobs"]["influencer"]["last_success"] is None  # unparseable → missing


def test_counts_reported_per_job():
    now = _utc(2026, 10, 8, 15, 0)
    store = _state(now, counts={"market": (581, 586), "short_volume": (580, 586)})
    _, body = _get(now, _redis(store))
    assert body["jobs"]["market"]["tickers_ok"] == 581
    assert body["jobs"]["market"]["tickers_total"] == 586
    assert body["jobs"]["short_volume"]["tickers_ok"] == 580
    assert "tickers_ok" not in body["jobs"]["influencer"]  # none stored
    assert "tickers_ok" not in body["jobs"]["scoring_tick"]  # not a counted job
    assert body["status"] == "ok"  # report only — no status rule on counts


# ---------------------------------------------------------------------------
# Auth, rate limit, caching
# ---------------------------------------------------------------------------


def test_route_is_public_and_v1_still_requires_key():
    from main import app

    now = _utc(2026, 10, 8, 15, 0)
    code, _ = _get(now, _redis(_state(now)))  # no Authorization header
    assert code == 200

    client = TestClient(app)
    for path in (
        "/v1/sentiment/AAPL",
        "/v1/sentiment/AAPL/history",
        "/v1/tickers",
        "/v1/market/overview",
        "/v1/status",
    ):
        assert client.get(path).status_code == 401, path


def test_ip_rate_limit_429_over_30_per_minute():
    now = _utc(2026, 10, 8, 15, 0)
    client = _redis(_state(now))
    client.eval = AsyncMock(return_value=31)
    code, body = _get(now, client, headers={"X-Forwarded-For": "1.2.3.4, 203.0.113.9"})
    assert code == 429
    key = client.eval.call_args.args[2]
    assert key == "rate:ip:health_pipeline:203.0.113.9"  # rightmost hop, own namespace
    assert client.eval.call_args.args[3] == 60


def test_ip_rate_limit_allows_30th_request():
    now = _utc(2026, 10, 8, 15, 0)
    client = _redis(_state(now))
    client.eval = AsyncMock(return_value=30)
    code, _ = _get(now, client)
    assert code == 200


def test_ip_rate_limit_fails_open_when_redis_eval_errors():
    now = _utc(2026, 10, 8, 15, 0)
    client = _redis(_state(now))
    client.eval = AsyncMock(side_effect=ConnectionError("down"))
    code, _ = _get(now, client)
    assert code == 200


def test_response_cached_for_60_seconds():
    now = _utc(2026, 10, 8, 15, 0)
    client = _redis(_state(now))
    _get(now, client)
    _get(now, client)
    assert client.mget.await_count == 1
    ph._cache = (0.0, *ph._cache[1:])  # expire it
    _get(now, client)
    assert client.mget.await_count == 2


def test_tick_summary_key_matches_scheduler():
    import pipeline.scheduler as sched

    assert ph.TICK_SUMMARY_KEY == sched.TICK_SUMMARY_KEY


# ---------------------------------------------------------------------------
# Market calendar
# ---------------------------------------------------------------------------


def test_calendar_session_days():
    from datetime import date

    assert cal.is_session_day(date(2026, 10, 8))
    assert not cal.is_session_day(date(2026, 10, 10))  # Saturday
    assert not cal.is_session_day(date(2026, 11, 26))  # Thanksgiving
    assert not cal.is_session_day(date(2027, 7, 5))  # Independence Day observed
    assert cal.is_session_day(date(2026, 11, 27))  # early close, still a session


def test_calendar_in_session_dst_and_early_close():
    assert cal.in_session(_utc(2026, 10, 8, 13, 30))  # EDT open
    assert not cal.in_session(_utc(2026, 10, 8, 20, 0))  # EDT close
    assert not cal.in_session(_utc(2026, 11, 25, 14, 0))  # EST, before 14:30 open
    assert cal.in_session(_utc(2026, 11, 25, 20, 59))
    assert cal.in_session(_utc(2026, 11, 27, 17, 59))  # early close 13:00 ET = 18:00 UTC
    assert not cal.in_session(_utc(2026, 11, 27, 18, 0))


def test_calendar_last_session_close_skips_holidays_and_early_close():
    # Day after Thanksgiving, evening → that day's 13:00 ET early close.
    assert cal.last_session_close(_utc(2026, 11, 27, 22, 0)) == _utc(2026, 11, 27, 18, 0)
    # Thanksgiving itself → Wednesday's close.
    assert cal.last_session_close(_utc(2026, 11, 26, 23, 0)) == _utc(2026, 11, 25, 21, 0)
    # Monday morning before the open → Friday's close.
    assert cal.last_session_close(_utc(2026, 10, 12, 12, 0)) == _utc(2026, 10, 9, 20, 0)


def test_early_close_eod_deadline_uses_2145_floor():
    now = _utc(2026, 11, 27, 21, 0)  # 3 h after the 18:00 UTC early close
    store = _state(now, success_at={"market_eod": _utc(2026, 11, 25, 21, 16)})
    _, body = _get(now, _redis(store))
    assert body["jobs"]["market_eod"]["stale"] is False  # due by 21:45, not 19:00
