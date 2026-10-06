"""
db/queries/price_snapshots.py

All price_snapshots table operations.

Functions accept an explicit asyncpg Connection so that callers can wrap
multiple inserts in a single transaction (see pipeline/persistence/pg_writer.py).
"""

from __future__ import annotations

from datetime import datetime

import asyncpg


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
