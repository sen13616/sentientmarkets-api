"""
tests/test_scheduler_config.py — Verify scheduler cron triggers fire within
the correct UTC windows, and that the scoring tick job is correctly configured.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from apscheduler.triggers.cron import CronTrigger


def _market_trigger() -> CronTrigger:
    """Reconstruct the market_job trigger from scheduler.py configuration.
    hour 13-20 UTC covers both DST regimes (EDT open 13:30 UTC, EST 14:30)."""
    return CronTrigger(day_of_week="mon-fri", hour="13-20", minute="*/15", timezone="UTC")


def test_market_job_fires_within_utc_market_window():
    """Next fire time after Mon 12:00 UTC must be the 13:00 UTC EDT-open slot."""
    trigger = _market_trigger()
    # Monday 2026-05-04 at 12:00 UTC (before the earliest, EDT, open)
    ref = datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc)
    nxt = trigger.get_next_fire_time(None, ref)
    assert nxt is not None
    # First eligible slot is now 13:00 UTC (covers the 13:30 EDT open)
    assert nxt.hour == 13
    assert nxt.minute == 0


def test_market_job_does_not_fire_before_13_utc():
    """Trigger must not fire before 13 UTC (ahead of even the EDT open)."""
    trigger = _market_trigger()
    # Monday at 09:00 UTC
    ref = datetime(2026, 5, 4, 9, 0, tzinfo=timezone.utc)
    nxt = trigger.get_next_fire_time(None, ref)
    assert nxt is not None
    # Should jump to 13:00, not fire at 09:00 or 09:15
    assert nxt.hour >= 13


def test_market_job_last_fire_is_2045_utc():
    """Last fire in a session should be 20:45 UTC, not 21:00 or later."""
    trigger = _market_trigger()
    # Monday at 20:46 UTC — just past last fire
    ref = datetime(2026, 5, 4, 20, 46, tzinfo=timezone.utc)
    nxt = trigger.get_next_fire_time(None, ref)
    assert nxt is not None
    # Should be next day (Tuesday) at 13:00
    assert nxt.day == 5
    assert nxt.hour == 13


# ---------------------------------------------------------------------------
# Sprint 3 — scoring tick tests
# ---------------------------------------------------------------------------


def _scoring_tick_trigger():
    """Reconstruct the scoring_tick trigger with explicit UTC (mirrors
    `_market_trigger`).  Production omits `timezone=` and relies on the
    Railway server running in UTC."""
    from apscheduler.triggers.combining import OrTrigger
    return OrTrigger([
        CronTrigger(minute="0,30", timezone="UTC"),
        CronTrigger(day_of_week="mon-fri", hour="13-20", minute="15,45", timezone="UTC"),
    ])


def test_scoring_tick_job_registered():
    """scoring_tick: 15 min during market hours (mon-fri 14:30-21:00 UTC),
    30 min off-hours.  Implemented as an OrTrigger of three CronTriggers."""
    from datetime import timedelta

    from apscheduler.triggers.combining import OrTrigger

    from pipeline.scheduler import scheduler

    job = scheduler.get_job("scoring_tick")
    assert job is not None, "scoring_tick job not found in scheduler"
    assert isinstance(job.trigger, OrTrigger)
    # Two sub-triggers: base 30-min cadence + one market-hours :15/:45 fill
    # (hour 13-20 UTC, covering both DST regimes).
    assert len(job.trigger.triggers) == 2

    # Spot-check the fire schedule against a UTC-anchored equivalent trigger.
    trig = _scoring_tick_trigger()

    # Monday at 14:25 UTC (just before open): next 6 fires should be the
    # 15-min market-hours cadence starting at 14:30.
    pre_open = datetime(2026, 5, 18, 14, 25, tzinfo=timezone.utc)
    fires = []
    cur = pre_open
    for _ in range(6):
        nxt = trig.get_next_fire_time(None, cur)
        fires.append(nxt)
        cur = nxt + timedelta(seconds=1)
    assert fires == [
        datetime(2026, 5, 18, 14, 30, tzinfo=timezone.utc),
        datetime(2026, 5, 18, 14, 45, tzinfo=timezone.utc),
        datetime(2026, 5, 18, 15,  0, tzinfo=timezone.utc),
        datetime(2026, 5, 18, 15, 15, tzinfo=timezone.utc),
        datetime(2026, 5, 18, 15, 30, tzinfo=timezone.utc),
        datetime(2026, 5, 18, 15, 45, tzinfo=timezone.utc),
    ]

    # Just after close: 30-min cadence resumes (21:30, 22:00, 22:30).
    post_close = datetime(2026, 5, 18, 21, 5, tzinfo=timezone.utc)
    fires = []
    cur = post_close
    for _ in range(3):
        nxt = trig.get_next_fire_time(None, cur)
        fires.append(nxt)
        cur = nxt + timedelta(seconds=1)
    assert fires == [
        datetime(2026, 5, 18, 21, 30, tzinfo=timezone.utc),
        datetime(2026, 5, 18, 22,  0, tzinfo=timezone.utc),
        datetime(2026, 5, 18, 22, 30, tzinfo=timezone.utc),
    ]


# ---------------------------------------------------------------------------
# Sprint P4.3 — macro scheduler split
# ---------------------------------------------------------------------------

def test_macro_daily_job_registered():
    """macro_daily job (FRED Treasury) must run once daily at 02:00 UTC."""
    from pipeline.scheduler import scheduler

    job = scheduler.get_job("macro_daily")
    assert job is not None, "macro_daily job not found in scheduler"
    # CronTrigger fields are CronTriggerField objects; stringify to check
    fields = {f.name: str(f) for f in job.trigger.fields}
    assert fields["hour"]   == "2"
    assert fields["minute"] == "0"


def test_macro_intraday_job_registered():
    """macro_intraday job (VIX + ETFs) must run hourly weekdays 13:00–20:00 UTC
    (hour 13 covers the EDT session open)."""
    from pipeline.scheduler import scheduler

    job = scheduler.get_job("macro_intraday")
    assert job is not None, "macro_intraday job not found in scheduler"
    fields = {f.name: str(f) for f in job.trigger.fields}
    assert fields["day_of_week"] == "mon-fri"
    assert fields["hour"]        == "13-20"
    assert fields["minute"]      == "0"


def test_legacy_macro_job_no_longer_registered():
    """The pre-P4.3 single `macro` job id is gone — replaced by daily + intraday."""
    from pipeline.scheduler import scheduler
    assert scheduler.get_job("macro") is None


async def test_scoring_tick_calls_score_all_for_active_tickers():
    """scoring_tick_job must call _score_all with the full ticker list + preloaded sector map (Sprint P4.2)."""
    mock_tickers = ["AAPL", "MSFT", "GOOGL"]
    mock_sector_map = {
        "AAPL":  "Information Technology",
        "MSFT":  "Information Technology",
        "GOOGL": "Communication Services",
    }

    with (
        patch("pipeline.scheduler.get_active_tickers", new_callable=AsyncMock, return_value=mock_tickers),
        patch("pipeline.scheduler.get_ticker_sector_map", new_callable=AsyncMock, return_value=mock_sector_map),
        patch("pipeline.scheduler._score_all", new_callable=AsyncMock, return_value=(3, 12)) as mock_score_all,
        patch("pipeline.scheduler._record_run", new_callable=AsyncMock),
    ):
        from pipeline.scheduler import scoring_tick_job
        await scoring_tick_job()

    mock_score_all.assert_called_once_with(mock_tickers, "SCORING_TICK", sector_map=mock_sector_map)


async def test_score_and_write_recomputes_all_four_layers():
    """_score_and_write must call all 4 layer scorers — none skipped."""
    now = datetime.now(timezone.utc)
    from pipeline.scoring.subindices import SubIndexResult

    si = SubIndexResult(50.0, 1, ["test"])

    with (
        patch("pipeline.orchestrator.read_scored_state", new_callable=AsyncMock, return_value=None),
        patch("pipeline.orchestrator.get_latest_close", new_callable=AsyncMock, return_value=100.0),
        patch("pipeline.orchestrator._score_market", new_callable=AsyncMock, return_value=(si, [], now)) as mock_market,
        patch("pipeline.orchestrator._score_narrative", new_callable=AsyncMock, return_value=(si, [], now)) as mock_narrative,
        patch("pipeline.orchestrator._score_influencer", new_callable=AsyncMock, return_value=(si, [], now, now, now)) as mock_influencer,
        patch("pipeline.orchestrator._score_macro", new_callable=AsyncMock, return_value=(si, [], now)) as mock_macro,
        patch("pipeline.orchestrator.compute_confidence", return_value=__import__("pipeline.confidence.scorer", fromlist=["ConfidenceResult"]).ConfidenceResult(score=80, flags=[])),
        patch("pipeline.orchestrator.write_scored_state", new_callable=AsyncMock),
        patch("pipeline.orchestrator.persist_scored_state", new_callable=AsyncMock),
    ):
        from pipeline.orchestrator import _score_and_write
        await _score_and_write("AAPL")

    mock_market.assert_called_once()
    mock_narrative.assert_called_once()
    mock_influencer.assert_called_once()
    mock_macro.assert_called_once()
