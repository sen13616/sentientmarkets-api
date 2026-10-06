"""
db/queries/universe.py

All ticker_universe table operations.
"""

from __future__ import annotations

from datetime import datetime

from scripts.db.connection import get_pool


async def get_active_tickers(as_of: datetime | None = None) -> list[str]:
    """
    Return the tier1_supported tickers to fetch/score, sorted alphabetically.

    Live (``as_of`` None): every ticker not retired (``delisted_at IS NULL``,
    migration 015). With ``as_of`` — for offline backfills/replays of a past
    window — the tickers that existed at that time: already added, and not yet
    retired (``delisted_at > as_of``). E.g. a June replay keeps symbols that
    stopped trading later in the year and skips tickers added afterwards.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        if as_of is None:
            rows = await conn.fetch(
                """
                SELECT ticker
                FROM ticker_universe
                WHERE tier = 'tier1_supported'
                  AND delisted_at IS NULL
                ORDER BY ticker
                """
            )
        else:
            rows = await conn.fetch(
                """
                SELECT ticker
                FROM ticker_universe
                WHERE tier = 'tier1_supported'
                  AND added_at <= $1
                  AND (delisted_at IS NULL OR delisted_at > $1)
                ORDER BY ticker
                """,
                as_of,
            )
    return [r["ticker"] for r in rows]


async def get_ticker_status(ticker: str) -> dict | None:
    """
    Lifecycle info for ``ticker``: ``{"ticker", "delisted_at",
    "successor_ticker", "delisted_reason"}``, or None if it was never in the
    tier1_supported universe.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT ticker, delisted_at, successor_ticker, delisted_reason
            FROM ticker_universe
            WHERE ticker = $1 AND tier = 'tier1_supported'
            """,
            ticker.upper(),
        )
    return dict(row) if row else None


async def is_supported_ticker(ticker: str) -> bool:
    """Return True if ticker is in the tier1_supported universe and not retired."""
    status = await get_ticker_status(ticker)
    return status is not None and status["delisted_at"] is None


async def get_all_tickers(include_delisted: bool = False) -> list[dict]:
    """
    Return the universe sorted alphabetically (active tickers only unless
    ``include_delisted``).

    Each dict has:
        ticker       : str  — the ticker symbol
        company_name : str | None — full company name (None if not yet seeded)
        sector       : str | None — GICS sector (None if not yet seeded; P4.1)
        in_sp500     : bool — current S&P 500 member (migration 015 snapshot)
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT ticker, company_name, sector, in_sp500
            FROM ticker_universe
            {"" if include_delisted else "WHERE delisted_at IS NULL"}
            ORDER BY ticker
            """
        )
    return [
        {
            "ticker": r["ticker"],
            "company_name": r["company_name"],
            "sector": r["sector"],
            "in_sp500": r["in_sp500"],
        }
        for r in rows
    ]


async def get_ticker_sector(ticker: str) -> str | None:
    """
    Return the GICS sector name for `ticker`, or None if not seeded.

    Sprint P4.1 — added for use by the per-ticker macro sub-index in P4.2.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT sector FROM ticker_universe WHERE ticker = $1",
            ticker.upper(),
        )


async def get_ticker_sector_map() -> dict[str, str]:
    """
    Return ``{ticker: sector}`` for every ticker with a non-NULL sector.

    Designed for one-shot preload at the start of ``scoring_tick_job`` so
    per-ticker macro scoring (P4.2) doesn't issue 502 separate DB calls.
    Tickers with a NULL sector are omitted; the macro scorer must handle
    that case (likely by skipping the sector-ETF component and letting
    weight redistribution absorb it).
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT ticker, sector FROM ticker_universe WHERE sector IS NOT NULL"
        )
    return {r["ticker"]: r["sector"] for r in rows}


async def get_universe_as_of(as_of: datetime) -> list[str]:
    """
    ``get_active_tickers(as_of)`` for offline scripts, refusing an empty
    result (e.g. an ``as_of`` before the universe was first seeded on
    2026-04-24 — pass the date of the window being scored, not the start of
    a history-warmup range).
    """
    tickers = await get_active_tickers(as_of)
    if not tickers:
        raise ValueError(
            f"no tickers in the universe as of {as_of.isoformat()} "
            "(before the 2026-04-24 seed?) — pass --universe-as-of"
        )
    return tickers
