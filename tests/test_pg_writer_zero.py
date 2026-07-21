"""
tests/test_pg_writer_zero.py — Verify that 0.0 values are persisted, not replaced by fallbacks.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipeline.persistence.pg_writer import _first_not_none


# ---------------------------------------------------------------------------
# Unit tests for _first_not_none helper
# ---------------------------------------------------------------------------

def test_first_not_none_returns_zero():
    assert _first_not_none(0.0, 99.0) == 0.0

def test_first_not_none_skips_none():
    assert _first_not_none(None, 42.0) == 42.0

def test_first_not_none_all_none():
    assert _first_not_none(None, None) is None

def test_first_not_none_zero_int():
    assert _first_not_none(0, 99) == 0


# ---------------------------------------------------------------------------
# Integration-level tests: full persist_scored_state with 0.0 values
# ---------------------------------------------------------------------------

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


async def test_zero_market_index_persisted():
    """market_index=0.0 must be persisted as 0.0, not replaced by sub_indices fallback."""
    from pipeline.persistence.pg_writer import persist_scored_state

    state = {
        "ticker": "TEST",
        "timestamp": datetime.now(timezone.utc),
        "composite_score": 50.0,
        "market_index": 0.0,
        "sub_indices": {"market": {"value": 99.0}},
        "confidence": {"score": 80, "flags": []},
        "freshness": {},
    }

    pool, conn = _mock_pool()
    mock_sh_insert = AsyncMock()

    with (
        patch("pipeline.persistence.pg_writer.get_pool", new_callable=AsyncMock, return_value=pool),
        patch("pipeline.persistence.pg_writer.sh_queries") as mock_sh,
        patch("pipeline.persistence.pg_writer.ps_queries"),
    ):
        mock_sh.insert_row = mock_sh_insert
        await persist_scored_state(state)

    _, kwargs = mock_sh_insert.call_args
    assert kwargs["market_index"] == 0.0


async def test_zero_confidence_score_persisted():
    """confidence_score=0 must be persisted as 0, not replaced by fallback."""
    from pipeline.persistence.pg_writer import persist_scored_state

    state = {
        "ticker": "TEST",
        "timestamp": datetime.now(timezone.utc),
        "composite_score": 50.0,
        "confidence_score": 0,
        "confidence": {"score": 99, "flags": []},
        "freshness": {},
    }

    pool, conn = _mock_pool()
    mock_sh_insert = AsyncMock()

    with (
        patch("pipeline.persistence.pg_writer.get_pool", new_callable=AsyncMock, return_value=pool),
        patch("pipeline.persistence.pg_writer.sh_queries") as mock_sh,
        patch("pipeline.persistence.pg_writer.ps_queries"),
    ):
        mock_sh.insert_row = mock_sh_insert
        await persist_scored_state(state)

    _, kwargs = mock_sh_insert.call_args
    assert kwargs["confidence_score"] == 0


async def test_zero_close_price_persisted():
    """close_price=0.0 must be persisted as 0.0, not replaced by fallback."""
    from pipeline.persistence.pg_writer import persist_scored_state

    state = {
        "ticker": "TEST",
        # Fixed in-market-hours timestamp: snapshots are skipped off-hours.
        "timestamp": datetime(2026, 7, 15, 15, 0, tzinfo=timezone.utc),
        "composite_score": 50.0,
        "close_price": 0.0,
        "price": {"close": 999.0},
        "confidence": {"score": 80, "flags": []},
        "freshness": {},
    }

    pool, conn = _mock_pool()
    mock_ps_insert = AsyncMock()

    with (
        patch("pipeline.persistence.pg_writer.get_pool", new_callable=AsyncMock, return_value=pool),
        patch("pipeline.persistence.pg_writer.sh_queries") as mock_sh,
        patch("pipeline.persistence.pg_writer.ps_queries") as mock_ps,
    ):
        mock_sh.insert_row = AsyncMock()
        mock_ps.insert_row = mock_ps_insert
        await persist_scored_state(state)

    _, kwargs = mock_ps_insert.call_args
    assert kwargs["close"] == 0.0


async def test_score_exo_persisted_and_zero_safe():
    """score_exo flows to composite_score_exo; 0.0 must survive, absent → None."""
    from pipeline.persistence.pg_writer import persist_scored_state

    base = {
        "ticker": "TEST",
        "timestamp": datetime.now(timezone.utc),
        "composite_score": 50.0,
        "confidence": {"score": 80, "flags": []},
        "freshness": {},
    }

    for exo_in, exo_out in [(0.0, 0.0), (57.42, 57.42), (None, None)]:
        state = dict(base)
        if exo_in is not None:
            state["score_exo"] = exo_in
        pool, conn = _mock_pool()
        mock_sh_insert = AsyncMock()
        with (
            patch("pipeline.persistence.pg_writer.get_pool", new_callable=AsyncMock, return_value=pool),
            patch("pipeline.persistence.pg_writer.sh_queries") as mock_sh,
            patch("pipeline.persistence.pg_writer.ps_queries"),
        ):
            mock_sh.insert_row = mock_sh_insert
            await persist_scored_state(state)
        _, kwargs = mock_sh_insert.call_args
        assert kwargs["composite_score_exo"] == exo_out
