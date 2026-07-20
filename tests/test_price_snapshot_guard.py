"""
tests/test_price_snapshot_guard.py — price_snapshots writes are market-hours only.

persist_scored_state() must skip the price_snapshots insert outside US market
hours (weekdays 14:30–21:00 UTC) while still writing sentiment_history.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from pipeline.persistence.pg_writer import persist_scored_state

# 2026-07-15 is a Wednesday.
IN_HOURS      = datetime(2026, 7, 15, 15, 0, tzinfo=timezone.utc)
OPEN_BOUNDARY = datetime(2026, 7, 15, 14, 30, tzinfo=timezone.utc)   # inclusive
CLOSE_BOUNDARY = datetime(2026, 7, 15, 21, 0, tzinfo=timezone.utc)   # exclusive
WEEKDAY_NIGHT = datetime(2026, 7, 15, 23, 0, tzinfo=timezone.utc)
SUNDAY        = datetime(2026, 7, 19, 15, 0, tzinfo=timezone.utc)


def _mock_pool():
    """Build an asyncpg-like mock pool with acquire() → conn context manager."""
    mock_conn = MagicMock()
    mock_conn.transaction.return_value.__aenter__ = AsyncMock()
    mock_conn.transaction.return_value.__aexit__ = AsyncMock(return_value=False)

    mock_ctx = MagicMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_ctx.__aexit__ = AsyncMock(return_value=False)

    mock_pool = MagicMock()
    mock_pool.acquire.return_value = mock_ctx
    return mock_pool, mock_conn


async def _run(timestamp, close=101.5):
    state = {
        "ticker": "TEST",
        "timestamp": timestamp,
        "composite_score": 50.0,
        "confidence": {"score": 80, "flags": []},
        "freshness": {},
    }
    if close is not None:
        state["close_price"] = close

    pool, _ = _mock_pool()
    mock_sh_insert = AsyncMock()
    mock_ps_insert = AsyncMock()

    with (
        patch("pipeline.persistence.pg_writer.get_pool", new_callable=AsyncMock, return_value=pool),
        patch("pipeline.persistence.pg_writer.sh_queries") as mock_sh,
        patch("pipeline.persistence.pg_writer.ps_queries") as mock_ps,
    ):
        mock_sh.insert_row = mock_sh_insert
        mock_ps.insert_row = mock_ps_insert
        await persist_scored_state(state)

    return mock_sh_insert, mock_ps_insert


async def test_snapshot_written_during_market_hours():
    sh, ps = await _run(IN_HOURS)
    sh.assert_awaited_once()
    ps.assert_awaited_once()
    assert ps.call_args.kwargs["close"] == 101.5


async def test_snapshot_written_at_open_boundary():
    _, ps = await _run(OPEN_BOUNDARY)
    ps.assert_awaited_once()


async def test_snapshot_skipped_at_close_boundary():
    sh, ps = await _run(CLOSE_BOUNDARY)
    sh.assert_awaited_once()
    ps.assert_not_awaited()


async def test_snapshot_skipped_weekday_night():
    sh, ps = await _run(WEEKDAY_NIGHT)
    sh.assert_awaited_once()
    ps.assert_not_awaited()


async def test_snapshot_skipped_on_weekend():
    sh, ps = await _run(SUNDAY)
    sh.assert_awaited_once()
    ps.assert_not_awaited()


async def test_no_close_no_snapshot_even_in_hours():
    sh, ps = await _run(IN_HOURS, close=None)
    sh.assert_awaited_once()
    ps.assert_not_awaited()
