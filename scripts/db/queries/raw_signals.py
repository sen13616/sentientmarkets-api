"""
db/queries/raw_signals.py

All raw_signals table operations. No raw SQL anywhere else in the codebase.
"""
from __future__ import annotations

from datetime import date, datetime, timezone

from scripts.db.connection import get_pool
from scripts.db.queries.as_of import cutoff


async def get_signals_since(
    ticker: str,
    since: datetime,
    signal_types: list[str] | None = None,
) -> list[dict]:
    """
    Return raw_signals rows for `ticker` with timestamp >= `since`
    (and <= the as-of cutoff when an offline replay has set one).

    Parameters
    ----------
    ticker       : Ticker symbol (or '_MACRO_' for global signals).
    since        : Earliest timestamp to include.
    signal_types : Optional allowlist of signal_type values.

    Returns
    -------
    list of dicts with keys: signal_type, value, source, timestamp.
    """
    params: list = [ticker, since]
    filters = ""
    if signal_types:
        params.append(signal_types)
        filters += f" AND signal_type = ANY(${len(params)}::text[])"
    as_of = cutoff()
    if as_of is not None:
        params.append(as_of)
        filters += f" AND timestamp <= ${len(params)}"

    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT signal_type, value, source, timestamp
            FROM raw_signals
            WHERE ticker     = $1
              AND timestamp >= $2{filters}
            ORDER BY timestamp DESC
            """,
            *params,
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


def _before_ts(before: date | None) -> datetime | None:
    return datetime(before.year, before.month, before.day, tzinfo=timezone.utc) if before else None


async def get_close_history(
    ticker: str,
    limit: int = 25,
    before: date | None = None,
) -> list[tuple[datetime, float]]:
    """
    Return one close per prior session — the most recent `limit` sessions,
    sorted ascending. Accepts both legacy ohlcv_close (backfill/Polygon) and
    yf_close (yfinance).

    Intraday market_job runs re-write today's partial bar every 15 min under
    the same timestamp (the bar date), so each date can hold many rows that
    tie on timestamp. The latest-WRITTEN row per date wins (created_at): for
    past sessions that is the market_eod_job final close. ``before`` (the
    current bar's date) excludes that session and later, so callers get
    prior sessions only. Before 2026-10, ties were broken arbitrarily and
    today's partial row was included (memory: market-history bug).
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
              AND ($3::timestamptz IS NULL OR timestamp < $3::timestamptz)
            ORDER BY DATE(timestamp) DESC, timestamp DESC, created_at DESC
            LIMIT $2
            """,
            ticker,
            limit,
            _before_ts(before),
        )
    return [(r["timestamp"], float(r["value"])) for r in reversed(rows)]


async def get_volume_history(
    ticker: str,
    limit: int = 20,
    before: date | None = None,
) -> list[float]:
    """
    Return one volume per prior session — the most recent `limit` sessions,
    sorted ascending. Accepts both legacy ohlcv_volume (backfill/Polygon) and
    yf_volume (yfinance). Used for volume_ratio = current_vol / avg_vol.

    Same per-date, latest-written selection as get_close_history: before
    2026-10 this read the last `limit` ROWS, i.e. mostly today's partial
    cumulative volumes, inflating volume_ratio ~2×.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT ON (DATE(timestamp))
                   timestamp, value
            FROM raw_signals
            WHERE ticker      = $1
              AND signal_type IN ('ohlcv_volume', 'yf_volume')
              AND ($3::timestamptz IS NULL OR timestamp < $3::timestamptz)
            ORDER BY DATE(timestamp) DESC, timestamp DESC, created_at DESC
            LIMIT $2
            """,
            ticker,
            limit,
            _before_ts(before),
        )
    return [float(r["value"]) for r in reversed(rows)]


async def get_signal_history(
    ticker: str,
    signal_type: str,
    limit: int = 20,
) -> list[float]:
    """
    Return the most recent `limit` values for a given signal_type, oldest first
    (most recent at or before the as-of cutoff when a replay has set one).

    Used for rolling z-score normalizers (e.g. short_volume_ratio_otc) that
    need a lookback window of historical daily values.
    """
    as_of = cutoff()
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT value
            FROM raw_signals
            WHERE ticker      = $1
              AND signal_type = $2
              {"AND timestamp <= $4" if as_of else ""}
            ORDER BY timestamp DESC
            LIMIT $3
            """,
            ticker,
            signal_type,
            limit,
            *([as_of] if as_of else []),
        )
    return [float(r["value"]) for r in reversed(rows)]


async def get_latest_close(ticker: str) -> float | None:
    """Return the most recent close price (yf_close or ohlcv_close), or None.

    Today's intraday rows tie on timestamp (the bar date); created_at picks
    the latest written. Bounded by the as-of cutoff when an offline replay
    has set one.
    """
    as_of = cutoff()
    pool = await get_pool()
    async with pool.acquire() as conn:
        val = await conn.fetchval(
            f"""
            SELECT value
            FROM raw_signals
            WHERE ticker      = $1
              AND signal_type IN ('yf_close', 'ohlcv_close')
              {"AND timestamp <= $2" if as_of else ""}
            ORDER BY timestamp DESC, created_at DESC
            LIMIT 1
            """,
            ticker,
            *([as_of] if as_of else []),
        )
    return float(val) if val is not None else None


OHLCV_SIGNAL_TYPES: list[str] = [
    "yf_open", "yf_high", "yf_low", "yf_close", "yf_volume",
    "ohlcv_open", "ohlcv_high", "ohlcv_low", "ohlcv_close",
    "ohlcv_adjusted_close", "ohlcv_volume",
]

# Recomputed by market_job every 15 min from OHLCV/live bars. Deepest read is
# the RollingZScorer window of 500 observations (~20 trading days at ~25
# rows/ticker/day), so these get the short DERIVED_RETENTION_DAYS tier.
DERIVED_INTRADAY_SIGNAL_TYPES: list[str] = [
    "rsi_14", "return_1d", "return_5d", "return_20d",
    "volume_ratio",
    "order_flow_imbalance", "buy_pressure", "sell_pressure",
    "bid_ask_spread_bps",
]

# Write-only quote telemetry — never read back (only bid_ask_spread_bps is
# scored). No longer written since 2026-07-20; QUOTE_RETENTION_DAYS tier
# drains the remainder.
QUOTE_SIGNAL_TYPES: list[str] = ["bid", "ask", "bid_ask_spread"]

# Research raw material — NEVER purged (2026-07-22, research program). FINRA
# short volume and insider transactions are candidate leading-signal inputs;
# purging them at 90 days would permanently cap the feature-backfill window.
# ~100-byte numeric rows accruing ~1.5k/day universe-wide — retention is
# effectively free. Excluded from the retention_job catch-all purge.
RESEARCH_RETAIN_SIGNAL_TYPES: list[str] = [
    "short_volume_otc", "short_volume_total_otc", "short_volume_ratio_otc",
    "insider_net_shares",
    # Analyst channel added 2026-07-22 (Track B2): likely future research input
    # (earnings-revision derivation reads up to 120 rows of EPS history);
    # was purging at 90 days.
    "analyst_buy_pct", "analyst_target_price", "analyst_eps_estimate_mean",
    # Options snapshots (2026-07-22, pipeline/sources/options.py): daily
    # yfinance chain snapshots — UNBACKFILLABLE; every purged day is lost
    # evaluation data forever.
    "pcr_volume", "pcr_oi", "atm_iv_30d", "iv_skew_25d",
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


async def replace_signals(
    ticker: str,
    signal_types: list[str],
    start: datetime,
    end: datetime,
    rows: list[tuple],
) -> int:
    """
    Atomically replace ``ticker``'s rows of ``signal_types`` in [start, end)
    with ``rows`` (same tuple shape as insert_signals). Used by
    scripts/backfill/reconstruct_signals.py --replace to swap derived history
    computed from contaminated close/volume history for corrected values.
    Returns the number of rows deleted.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            tag = await conn.execute(
                """
                DELETE FROM raw_signals
                WHERE ticker      = $1
                  AND signal_type = ANY($2::text[])
                  AND timestamp  >= $3
                  AND timestamp  <  $4
                """,
                ticker, signal_types, start, end,
            )
            if rows:
                await conn.executemany(
                    """
                    INSERT INTO raw_signals
                        (ticker, signal_type, value, source, upload_type, timestamp)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    """,
                    rows,
                )
    return int(tag.split()[-1]) if tag else 0
