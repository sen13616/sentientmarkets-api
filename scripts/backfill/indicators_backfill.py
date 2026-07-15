"""
backfill/indicators_backfill.py

Compute RSI(14) for all tier1 tickers from the ohlcv_close data
already in raw_signals, and write rsi_14 rows back to raw_signals.

Run AFTER ohlcv_backfill.py — this script reads from the DB instead of
calling any external API, so there is no rate limit.

Why compute from DB data instead of calling Alpha Vantage:
  The RSI TECHNICAL_INDICATORS endpoint is premium on the free AV tier.
  Computing RSI from our own stored close prices is more reliable and
  ensures the RSI is consistent with the same data used by the pipeline.

Usage:
  python backfill/indicators_backfill.py
"""
import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts.db.connection import close_pool, get_pool, init_pool


# --------------------------------------------------------------------------- #
# RSI computation                                                               #
# --------------------------------------------------------------------------- #

def compute_rsi_14(closes: list[float]) -> list[float | None]:
    """
    Wilder's smoothed RSI(14).

    Returns a list aligned with `closes`.
    The first 14 indices are None (insufficient history).
    Index 14 is computed using the simple average of the first 14 deltas.
    Subsequent indices use Wilder's exponential smoothing.
    """
    period = 14
    n = len(closes)

    if n <= period:
        return [None] * n

    arr = np.array(closes, dtype=np.float64)
    deltas = np.diff(arr)                        # length n-1

    gains  = np.where(deltas > 0,  deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)

    result: list[float | None] = [None] * n

    # Bootstrap with simple average over first `period` deltas
    avg_gain = float(np.mean(gains[:period]))
    avg_loss = float(np.mean(losses[:period]))

    def _rsi(ag: float, al: float) -> float:
        if al == 0.0:
            return 100.0
        return 100.0 - (100.0 / (1.0 + ag / al))

    result[period] = _rsi(avg_gain, avg_loss)

    # Wilder smoothing for the rest
    for i in range(period + 1, n):
        d = deltas[i - 1]
        g = d if d > 0 else 0.0
        l = -d if d < 0 else 0.0
        avg_gain = (avg_gain * (period - 1) + g) / period
        avg_loss = (avg_loss * (period - 1) + l) / period
        result[i] = _rsi(avg_gain, avg_loss)

    return result


# --------------------------------------------------------------------------- #
# DB helpers                                                                    #
# --------------------------------------------------------------------------- #

async def _fetch_closes(ticker: str) -> list[tuple[datetime, float]]:
    """Return [(timestamp, close), ...] sorted ascending for ticker."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT timestamp, value
            FROM raw_signals
            WHERE ticker      = $1
              AND signal_type = 'ohlcv_close'
              AND upload_type = 'manual_backfill'
            ORDER BY timestamp ASC
            """,
            ticker,
        )
    return [(r["timestamp"], float(r["value"])) for r in rows]


async def _already_backfilled(ticker: str) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        n = await conn.fetchval(
            """
            SELECT COUNT(*) FROM raw_signals
            WHERE ticker      = $1
              AND signal_type = 'rsi_14'
              AND upload_type = 'manual_backfill'
            """,
            ticker,
        )
    return n > 0


async def _insert_rsi_rows(rows: list[tuple]) -> None:
    if not rows:
        return
    pool = await get_pool()
    async with pool.acquire() as conn:
        # CAUTION: blind INSERT — bypasses insert_signals()'s dedup guard
        # (NOT EXISTS on ticker/signal_type/timestamp/value/source). Re-running
        # this script can create duplicate rows; dedupe manually or port to
        # scripts.db.queries.raw_signals.insert_signals() before reuse.
        await conn.executemany(
            """
            INSERT INTO raw_signals
                (ticker, signal_type, value, source, upload_type, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            rows,
        )


# --------------------------------------------------------------------------- #
# Main                                                                          #
# --------------------------------------------------------------------------- #

async def main() -> None:
    await init_pool()
    pool = await get_pool()

    async with pool.acquire() as conn:
        tickers: list[str] = [
            r["ticker"]
            for r in await conn.fetch(
                "SELECT ticker FROM ticker_universe"
                " WHERE tier = 'tier1_supported' ORDER BY ticker"
            )
        ]

    total = len(tickers)
    print(f"RSI(14) computation — {total} tier1 tickers.\n")

    processed = 0
    skipped_done = 0
    skipped_no_data = 0

    for idx, ticker in enumerate(tickers, 1):
        if await _already_backfilled(ticker):
            skipped_done += 1
            print(f"  [{idx}/{total}] {ticker}: already done — skipped.")
            continue

        closes_with_ts = await _fetch_closes(ticker)

        if len(closes_with_ts) < 15:   # need at least period+1 points
            skipped_no_data += 1
            print(f"  [{idx}/{total}] {ticker}: insufficient OHLCV data ({len(closes_with_ts)} rows) — skipped.")
            continue

        timestamps = [row[0] for row in closes_with_ts]
        closes     = [row[1] for row in closes_with_ts]

        rsi_values = compute_rsi_14(closes)

        rows: list[tuple] = []
        for ts, rsi in zip(timestamps, rsi_values):
            if rsi is None:
                continue
            # Ensure timestamp is tz-aware
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            rows.append((ticker, "rsi_14", round(rsi, 4), "computed", "manual_backfill", ts))

        if rows:
            await _insert_rsi_rows(rows)
            print(f"  [{idx}/{total}] {ticker}: {len(rows)} RSI rows inserted.")
            processed += 1
        else:
            skipped_no_data += 1
            print(f"  [{idx}/{total}] {ticker}: no RSI values computed.")

    await close_pool()
    print(
        f"\nDone.  Computed: {processed}  |  "
        f"Already done: {skipped_done}  |  "
        f"No OHLCV data: {skipped_no_data}"
    )


if __name__ == "__main__":
    asyncio.run(main())
