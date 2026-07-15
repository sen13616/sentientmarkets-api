"""
db/queries/sentiment_history.py

All sentiment_history table operations.

Functions accept an explicit asyncpg Connection so that callers can wrap
multiple inserts in a single transaction (see pipeline/persistence/pg_writer.py).
Read-only functions (get_latest, get_history) acquire their own connection
from the pool.
"""
from __future__ import annotations

import json
from datetime import datetime

import asyncpg

from scripts.db.connection import get_pool


async def insert_row(
    conn: asyncpg.Connection,
    *,
    ticker: str,
    composite_score: float,
    market_index: float | None,
    narrative_index: float | None,
    influencer_index: float | None,
    macro_index: float | None,
    confidence_score: int,
    confidence_flags: list[str],
    top_drivers: list[dict],
    divergence: str | None,
    market_as_of: datetime | None,
    narrative_as_of: datetime | None,
    influencer_as_of: datetime | None,
    macro_as_of: datetime | None,
    timestamp: datetime,
    composite_score_smoothed: float | None = None,
    ema_obs_count: int = 0,
) -> None:
    """Insert one scored row into sentiment_history."""
    await conn.execute(
        """
        INSERT INTO sentiment_history (
            ticker,
            composite_score,
            market_index,
            narrative_index,
            influencer_index,
            macro_index,
            confidence_score,
            confidence_flags,
            top_drivers,
            divergence,
            market_as_of,
            narrative_as_of,
            influencer_as_of,
            macro_as_of,
            timestamp,
            composite_score_smoothed,
            ema_obs_count
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17
        )
        """,
        ticker,
        composite_score,
        market_index,
        narrative_index,
        influencer_index,
        macro_index,
        confidence_score,
        json.dumps(confidence_flags),
        json.dumps(top_drivers),
        divergence,
        market_as_of,
        narrative_as_of,
        influencer_as_of,
        macro_as_of,
        timestamp,
        composite_score_smoothed,
        ema_obs_count,
    )


async def get_latest(ticker: str) -> dict | None:
    """
    Return the most recent sentiment_history row for ticker as a dict,
    or None if no rows exist.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT *
              FROM sentiment_history
             WHERE ticker = $1
             ORDER BY timestamp DESC
             LIMIT 1
            """,
            ticker.upper(),
        )
    if row is None:
        return None
    return dict(row)


async def get_history(
    ticker: str,
    days: int = 30,
    interval: str = "daily",
) -> list[dict]:
    """
    Return historical sentiment rows for ticker.

    Parameters
    ----------
    ticker   : Ticker symbol (case-insensitive).
    days     : Lookback window. Default 30, max 365.
    interval : 'daily'  (one row per day, latest tick per day),
               'hourly' (one row per hour, latest tick per hour),
               'raw'    (every scoring tick).
    """
    days = min(max(1, days), 365)
    pool  = await get_pool()
    async with pool.acquire() as conn:
        if interval == "daily":
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (timestamp::date)
                       timestamp,
                       composite_score,
                       composite_score_smoothed,
                       ema_obs_count,
                       market_index,
                       narrative_index,
                       influencer_index,
                       macro_index,
                       confidence_score
                  FROM sentiment_history
                 WHERE ticker    = $1
                   AND timestamp >= NOW() - ($2 || ' days')::interval
                 ORDER BY timestamp::date DESC, timestamp DESC
                """,
                ticker.upper(),
                str(days),
            )
        elif interval == "hourly":
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (DATE_TRUNC('hour', timestamp))
                       timestamp,
                       composite_score,
                       composite_score_smoothed,
                       ema_obs_count,
                       market_index,
                       narrative_index,
                       influencer_index,
                       macro_index,
                       confidence_score
                  FROM sentiment_history
                 WHERE ticker    = $1
                   AND timestamp >= NOW() - ($2 || ' days')::interval
                 ORDER BY DATE_TRUNC('hour', timestamp) DESC, timestamp DESC
                """,
                ticker.upper(),
                str(days),
            )
        else:
            rows = await conn.fetch(
                """
                SELECT timestamp,
                       composite_score,
                       composite_score_smoothed,
                       ema_obs_count,
                       market_index,
                       narrative_index,
                       influencer_index,
                       macro_index,
                       confidence_score
                  FROM sentiment_history
                 WHERE ticker    = $1
                   AND timestamp >= NOW() - ($2 || ' days')::interval
                 ORDER BY timestamp DESC
                """,
                ticker.upper(),
                str(days),
            )
    return [dict(r) for r in rows]


async def get_baseline_scores(
    min_age_hours: int = 24,
    max_age_hours: int = 48,
) -> dict[str, float]:
    """
    Batched 1-day-change baseline: for every ticker, the smoothed composite
    score of its most recent tick aged between `min_age_hours` and
    `max_age_hours` (default 24-48h old).

    The upper bound is what makes day-over-day change gap-safe: a ticker
    whose newest >=24h-old tick is older than 48h (new ticker, or a data
    gap such as the 2026-06-23 -> 07-03 outage) is simply absent from the
    returned map, so callers report the change as null rather than
    comparing across the gap. Falls back to the raw composite for pre-EMA
    rows, mirroring the API's display-score convention.

    One query for the whole universe; called once per scoring tick.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT ON (ticker)
                   ticker,
                   COALESCE(composite_score_smoothed, composite_score) AS baseline
              FROM sentiment_history
             WHERE timestamp <= NOW() - ($1 || ' hours')::interval
               AND timestamp >= NOW() - ($2 || ' hours')::interval
             ORDER BY ticker, timestamp DESC
            """,
            str(min_age_hours),
            str(max_age_hours),
        )
    return {r["ticker"]: float(r["baseline"]) for r in rows if r["baseline"] is not None}
