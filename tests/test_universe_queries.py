"""
tests/test_universe_queries.py — Sprint P4.1

Unit tests for the new sector accessors on `db/queries/universe.py`:
  - `get_ticker_sector(ticker)`            — per-ticker lookup
  - `get_ticker_sector_map()`              — bulk preload for scoring_tick_job
  - `get_all_tickers()` schema additive    — includes `sector` field

Uses a mocked asyncpg pool (same shape as `tests/test_pg_writer_zero.py`)
so the tests don't depend on a live Postgres connection.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Mock-pool helpers (mirrors test_pg_writer_zero.py)
# ---------------------------------------------------------------------------


def _mock_pool_with(conn):
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    pool = MagicMock()
    pool.acquire.return_value = ctx
    return pool


# ---------------------------------------------------------------------------
# get_ticker_sector
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_ticker_sector_returns_seeded_value():
    conn = MagicMock()
    conn.fetchval = AsyncMock(return_value="Information Technology")
    pool = _mock_pool_with(conn)

    with patch("scripts.db.queries.universe.get_pool", new=AsyncMock(return_value=pool)):
        from scripts.db.queries.universe import get_ticker_sector

        sector = await get_ticker_sector("AAPL")

    assert sector == "Information Technology"
    conn.fetchval.assert_awaited_once()
    args = conn.fetchval.await_args.args
    assert "SELECT sector FROM ticker_universe" in args[0]
    assert args[1] == "AAPL"


@pytest.mark.asyncio
async def test_get_ticker_sector_returns_none_for_unseeded():
    """A row exists for the ticker but the sector column is NULL."""
    conn = MagicMock()
    conn.fetchval = AsyncMock(return_value=None)
    pool = _mock_pool_with(conn)

    with patch("scripts.db.queries.universe.get_pool", new=AsyncMock(return_value=pool)):
        from scripts.db.queries.universe import get_ticker_sector

        sector = await get_ticker_sector("XYZ")

    assert sector is None


@pytest.mark.asyncio
async def test_get_ticker_sector_returns_none_for_missing_ticker():
    """No row for the ticker in ticker_universe → fetchval returns None."""
    conn = MagicMock()
    conn.fetchval = AsyncMock(return_value=None)
    pool = _mock_pool_with(conn)

    with patch("scripts.db.queries.universe.get_pool", new=AsyncMock(return_value=pool)):
        from scripts.db.queries.universe import get_ticker_sector

        sector = await get_ticker_sector("NOT_A_TICKER")

    assert sector is None


@pytest.mark.asyncio
async def test_get_ticker_sector_uppercases_input():
    """Mirror existing convention (is_supported_ticker uppercases); aapl → AAPL."""
    conn = MagicMock()
    conn.fetchval = AsyncMock(return_value="Information Technology")
    pool = _mock_pool_with(conn)

    with patch("scripts.db.queries.universe.get_pool", new=AsyncMock(return_value=pool)):
        from scripts.db.queries.universe import get_ticker_sector

        await get_ticker_sector("aapl")

    args = conn.fetchval.await_args.args
    assert args[1] == "AAPL"


# ---------------------------------------------------------------------------
# get_ticker_sector_map
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_ticker_sector_map_returns_dict():
    rows = [
        {"ticker": "AAPL", "sector": "Information Technology"},
        {"ticker": "XOM", "sector": "Energy"},
        {"ticker": "JNJ", "sector": "Health Care"},
    ]
    conn = MagicMock()
    conn.fetch = AsyncMock(return_value=rows)
    pool = _mock_pool_with(conn)

    with patch("scripts.db.queries.universe.get_pool", new=AsyncMock(return_value=pool)):
        from scripts.db.queries.universe import get_ticker_sector_map

        result = await get_ticker_sector_map()

    assert result == {
        "AAPL": "Information Technology",
        "XOM": "Energy",
        "JNJ": "Health Care",
    }


@pytest.mark.asyncio
async def test_get_ticker_sector_map_empty_when_no_rows():
    """Pre-seed state: no rows have a sector yet."""
    conn = MagicMock()
    conn.fetch = AsyncMock(return_value=[])
    pool = _mock_pool_with(conn)

    with patch("scripts.db.queries.universe.get_pool", new=AsyncMock(return_value=pool)):
        from scripts.db.queries.universe import get_ticker_sector_map

        result = await get_ticker_sector_map()

    assert result == {}


@pytest.mark.asyncio
async def test_get_ticker_sector_map_filters_nulls_at_sql_level():
    """The SQL should include `WHERE sector IS NOT NULL` — NULLs never reach Python."""
    conn = MagicMock()
    conn.fetch = AsyncMock(return_value=[])
    pool = _mock_pool_with(conn)

    with patch("scripts.db.queries.universe.get_pool", new=AsyncMock(return_value=pool)):
        from scripts.db.queries.universe import get_ticker_sector_map

        await get_ticker_sector_map()

    sql = conn.fetch.await_args.args[0]
    assert "WHERE sector IS NOT NULL" in sql


# ---------------------------------------------------------------------------
# get_all_tickers — additive schema change
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_all_tickers_includes_sector_field():
    rows = [
        {
            "ticker": "AAPL",
            "company_name": "Apple Inc.",
            "sector": "Information Technology",
            "in_sp500": True,
        },
        {"ticker": "XOM", "company_name": "Exxon Mobil", "sector": "Energy", "in_sp500": True},
    ]
    conn = MagicMock()
    conn.fetch = AsyncMock(return_value=rows)
    pool = _mock_pool_with(conn)

    with patch("scripts.db.queries.universe.get_pool", new=AsyncMock(return_value=pool)):
        from scripts.db.queries.universe import get_all_tickers

        items = await get_all_tickers()

    assert len(items) == 2
    assert items[0] == {
        "ticker": "AAPL",
        "company_name": "Apple Inc.",
        "sector": "Information Technology",
        "in_sp500": True,
    }
    assert items[1]["sector"] == "Energy"


@pytest.mark.asyncio
async def test_get_all_tickers_sector_can_be_null():
    """Backwards-compat: pre-seed rows have NULL sector; must not raise."""
    rows = [
        {"ticker": "AAPL", "company_name": "Apple Inc.", "sector": None, "in_sp500": False},
    ]
    conn = MagicMock()
    conn.fetch = AsyncMock(return_value=rows)
    pool = _mock_pool_with(conn)

    with patch("scripts.db.queries.universe.get_pool", new=AsyncMock(return_value=pool)):
        from scripts.db.queries.universe import get_all_tickers

        items = await get_all_tickers()

    assert items[0]["sector"] is None


# ---------------------------------------------------------------------------
# Static sector_map.py validation (no DB / network)
# ---------------------------------------------------------------------------


def test_sector_map_only_uses_canonical_gics_strings():
    """Every value in TICKER_SECTORS must be a key in SECTOR_ETFS."""
    from pipeline.sources.macro import SECTOR_ETFS
    from scripts.tools.sector_map import TICKER_SECTORS

    valid = set(SECTOR_ETFS.keys())
    invalid = {t: s for t, s in TICKER_SECTORS.items() if s not in valid}
    assert not invalid, f"sector_map contains non-GICS sectors: {invalid}"


def test_sector_map_covers_full_universe():
    """All 502 active tickers from tools/company_names.py must have a sector."""
    from scripts.tools.company_names import COMPANY_NAMES
    from scripts.tools.sector_map import TICKER_SECTORS

    missing = set(COMPANY_NAMES) - set(TICKER_SECTORS)
    # P4.1 sprint gate: ≥497 of 502 (plan tolerance for renamed/stale tickers).
    assert len(missing) <= 5, f"sector_map missing {len(missing)} tickers: {sorted(missing)[:10]}"
