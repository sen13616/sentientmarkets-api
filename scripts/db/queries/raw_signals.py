"""
db/queries/raw_signals.py

All raw_signals table operations. No raw SQL anywhere else in the codebase.
"""
from __future__ import annotations

from datetime import datetime

from scripts.db.connection import get_pool


async def get_signals_since(
    ticker: str,
    since: datetime,
    signal_types: list[str] | None = None,
) -> list[dict]:
    """
    Return raw_signals rows for `ticker` with timestamp >= `since`.

    Parameters
    ----------
    ticker       : Ticker symbol (or '_MACRO_' for global signals).
    since        : Earliest timestamp to include.
    signal_types : Optional allowlist of signal_type values.

    Returns
    -------
    list of dicts with keys: signal_type, value, source, timestamp.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        if signal_types:
            rows = await conn.fetch(
                """
                SELECT signal_type, value, source, timestamp
                FROM raw_signals
                WHERE ticker      = $1
                  AND timestamp  >= $2
                  AND signal_type = ANY($3::text[])
                ORDER BY timestamp DESC
                """,
                ticker,
                since,
                signal_types,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT signal_type, value, source, timestamp
                FROM raw_signals
                WHERE ticker     = $1
                  AND timestamp >= $2
                ORDER BY timestamp DESC
                """,
                ticker,
                since,
            )
    return [dict(r) for r in rows]


async def insert_signals(rows: list[tuple]) -> None:
    """Bulk-insert (ticker, signal_type, value, source, upload_type, timestamp) tuples.

    Idempotent on the natural key (ticker, signal_type, timestamp, value,
    source): a row whose exact key already exists is skipped, so jobs that
    re-fetch an overlapping window (influencer Form 4s, FRED latest-obs,
    sector-ETF closes, intraday OHLC bars) no longer accrue duplicates.
    upload_type is deliberately NOT part of the key — a live re-fetch of a
    value that a backfill already stored is still a duplicate.

    The NOT EXISTS probe is served by idx_raw_signals_lookup
    (ticker, signal_type, timestamp DESC). executemany runs the batch in
    one transaction, so within-batch duplicates are also collapsed.
    Concurrent writers could still race past the probe; the hard guarantee
    is a unique index, deferred until the historical duplicates are
    cleaned up.
    """
    if not rows:
        return
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO raw_signals
                (ticker, signal_type, value, source, upload_type, timestamp)
            SELECT $1::varchar, $2::varchar, $3::float8, $4::varchar,
                   $5::varchar, $6::timestamptz
            WHERE NOT EXISTS (
                SELECT 1 FROM raw_signals
                WHERE ticker      = $1::varchar
                  AND signal_type = $2::varchar
                  AND timestamp   = $6::timestamptz
                  AND value       = $3::float8
                  AND source      = $4::varchar
            )
            """,
            rows,
        )


async def get_close_history(
    ticker: str,
    limit: int = 25,
) -> list[tuple[datetime, float]]:
    """
    Return the most recent `limit` close-price rows sorted ascending.
    Accepts both legacy ohlcv_close (backfill/Polygon) and yf_close (yfinance).
    Includes both manual_backfill and live rows so returns can be computed
    against the most recent available close.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT ON (DATE(timestamp))
                   timestamp, value
            FROM raw_signals
            WHERE ticker      = $1
              AND signal_type IN ('ohlcv_close', 'yf_close')
            ORDER BY DATE(timestamp) DESC, timestamp DESC
            LIMIT $2
            """,
            ticker,
            limit,
        )
    return [(r["timestamp"], float(r["value"])) for r in reversed(rows)]


async def get_volume_history(
    ticker: str,
    limit: int = 25,
) -> list[float]:
    """
    Return the most recent `limit` volume values sorted ascending.
    Accepts both legacy ohlcv_volume (backfill/Polygon) and yf_volume (yfinance).
    Used for computing volume_ratio = current_vol / avg_vol.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT value
            FROM raw_signals
            WHERE ticker      = $1
              AND signal_type IN ('ohlcv_volume', 'yf_volume')
            ORDER BY timestamp DESC
            LIMIT $2
            """,
            ticker,
            limit,
        )
    return [float(r["value"]) for r in reversed(rows)]


async def get_signal_history(
    ticker: str,
    signal_type: str,
    limit: int = 20,
) -> list[float]:
    """
    Return the most recent `limit` values for a given signal_type, oldest first.

    Used for rolling z-score normalizers (e.g. short_volume_ratio_otc) that
    need a lookback window of historical daily values.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT value
            FROM raw_signals
            WHERE ticker      = $1
              AND signal_type = $2
            ORDER BY timestamp DESC
            LIMIT $3
            """,
            ticker,
            signal_type,
            limit,
        )
    return [float(r["value"]) for r in reversed(rows)]


async def get_latest_close(ticker: str) -> float | None:
    """Return the most recent close price (yf_close or ohlcv_close), or None."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        val = await conn.fetchval(
            """
            SELECT value
            FROM raw_signals
            WHERE ticker      = $1
              AND signal_type IN ('yf_close', 'ohlcv_close')
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            ticker,
        )
    return float(val) if val is not None else None


OHLCV_SIGNAL_TYPES: list[str] = [
    "yf_open", "yf_high", "yf_low", "yf_close", "yf_volume",
    "ohlcv_open", "ohlcv_high", "ohlcv_low", "ohlcv_close",
    "ohlcv_adjusted_close", "ohlcv_volume",
]


async def purge_signals_before(
    cutoff: datetime,
    signal_types: list[str] | None = None,
    *,
    exclude: bool = False,
) -> int:
    """Delete raw_signals rows with timestamp < cutoff.

    When `signal_types` is provided and `exclude` is False, only rows whose
    signal_type is in the list are deleted. When `exclude` is True, only rows
    whose signal_type is NOT in the list are deleted. When `signal_types` is
    None, all rows older than the cutoff are deleted.

    Returns the number of rows deleted.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        if signal_types is None:
            tag = await conn.execute(
                "DELETE FROM raw_signals WHERE timestamp < $1",
                cutoff,
            )
        elif exclude:
            tag = await conn.execute(
                """
                DELETE FROM raw_signals
                WHERE timestamp   < $1
                  AND signal_type <> ALL($2::text[])
                """,
                cutoff,
                signal_types,
            )
        else:
            tag = await conn.execute(
                """
                DELETE FROM raw_signals
                WHERE timestamp   < $1
                  AND signal_type = ANY($2::text[])
                """,
                cutoff,
                signal_types,
            )
    return int(tag.split()[-1]) if tag else 0
