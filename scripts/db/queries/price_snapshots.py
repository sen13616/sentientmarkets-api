"""
db/queries/price_snapshots.py

All price_snapshots table operations.

Functions accept an explicit asyncpg Connection so that callers can wrap
multiple inserts in a single transaction (see pipeline/persistence/pg_writer.py).
"""
from __future__ import annotations

from datetime import datetime

import asyncpg

from scripts.db.connection import get_pool


async def estimate_row_count() -> int:
    """Fast O(1) planner estimate of price_snapshots row count.

    Uses pg_class.reltuples (updated by autovacuum/analyze) rather than a
    full COUNT(*) — the table is millions of rows and is never purged (it is
    write-only research data), so an exact count each run would be wasteful.
    Returns 0 if the estimate is unavailable.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        val = await conn.fetchval(
            "SELECT reltuples::bigint FROM pg_class WHERE relname = 'price_snapshots'"
        )
    return int(val) if val and val > 0 else 0


async def insert_row(
    conn: asyncpg.Connection,
    *,
    ticker: str,
    close: float,
    volume: int | None,
    timestamp: datetime,
) -> None:
    """Insert one price snapshot row into price_snapshots."""
    await conn.execute(
        """
        INSERT INTO price_snapshots (ticker, close, volume, timestamp)
        VALUES ($1, $2, $3, $4)
        """,
        ticker,
        close,
        volume,
        timestamp,
    )
