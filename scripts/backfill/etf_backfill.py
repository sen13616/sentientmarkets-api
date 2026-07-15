"""
backfill/etf_backfill.py

Pull 30 days of daily close data for the 11 GICS sector ETFs from Alpha
Vantage (TIME_SERIES_DAILY, outputsize=compact) and write to raw_signals.

signal_type is stored as 'ohlcv_close' so that pipeline/sources/macro.py's
call to get_close_history(etf) can find the history when computing the 20-day
sector return.  (get_close_history queries signal_type='ohlcv_close'.)

Note: after this initial backfill is consumed, macro.py's live cycle should
also write ohlcv_close rows for each ETF so the history stays current.  For
now this one-time run is sufficient to unblock the 20d-return computation.

Resumable: ETFs with any existing ohlcv_close rows are skipped.

Rate limiting:
  Premium standard tier: 75 req/min.
  INTER_REQUEST_DELAY = 0.8s → ~62 req/min.

Usage:
  python backfill/etf_backfill.py
"""
import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts.db.connection import close_pool, get_pool, init_pool

load_dotenv(override=False)

AV_BASE            = "https://www.alphavantage.co/query"
LOOKBACK_DAYS      = 30
INTER_REQUEST_DELAY = 0.8   # seconds between AV requests

SECTOR_ETFS = [
    "XLC",   # Communication Services
    "XLY",   # Consumer Discretionary
    "XLP",   # Consumer Staples
    "XLE",   # Energy
    "XLF",   # Financials
    "XLV",   # Health Care
    "XLI",   # Industrials
    "XLK",   # Information Technology
    "XLB",   # Materials
    "XLRE",  # Real Estate
    "XLU",   # Utilities
]


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

async def _already_backfilled(etf: str) -> bool:
    """Return True if any ohlcv_close backfill rows exist for this ETF."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        n = await conn.fetchval(
            """
            SELECT COUNT(*) FROM raw_signals
            WHERE ticker      = $1
              AND signal_type = 'ohlcv_close'
              AND upload_type = 'manual_backfill'
            """,
            etf,
        )
    return (n or 0) > 0


async def _insert_rows(rows: list[tuple]) -> None:
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


# ---------------------------------------------------------------------------
# Alpha Vantage helpers
# ---------------------------------------------------------------------------

async def _fetch_daily(
    client: httpx.AsyncClient,
    etf: str,
    api_key: str,
) -> dict[str, dict]:
    """
    Call TIME_SERIES_DAILY with outputsize=compact (100 trading days).
    Returns the inner time-series dict, or {} on failure.
    """
    try:
        resp = await client.get(
            AV_BASE,
            params={
                "function":   "TIME_SERIES_DAILY",
                "symbol":     etf,
                "outputsize": "compact",
                "apikey":     api_key,
            },
        )
        resp.raise_for_status()
        body = resp.json()
    except Exception as exc:
        print(f"    [!] HTTP error for {etf}: {exc}")
        return {}

    if "Note" in body or "Information" in body:
        msg = (body.get("Note") or body.get("Information") or "")[:120]
        print(f"    [!] AV rate-limit message: {msg}")
        return {}

    ts = body.get("Time Series (Daily)")
    if not ts:
        print(f"    [!] No time-series data for {etf}. Keys: {list(body)}")
        return {}

    return ts


def _build_rows(
    etf: str,
    time_series: dict[str, dict],
    cutoff: datetime,
) -> list[tuple]:
    """
    Convert the AV time-series dict to raw_signals rows within the lookback window.
    Stores signal_type='ohlcv_close' so get_close_history() can find the data.
    """
    rows: list[tuple] = []
    for date_str, ohlcv in time_series.items():
        try:
            ts = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            continue

        if ts < cutoff:
            continue

        try:
            rows.append((
                etf,
                "ohlcv_close",
                float(ohlcv["4. close"]),
                "alpha_vantage",
                "manual_backfill",
                ts,
            ))
        except (KeyError, ValueError) as exc:
            print(f"    [!] Parse error on {date_str} for {etf}: {exc}")
            continue

    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    api_key = os.environ.get("ALPHA_VANTAGE_KEY", "")
    if not api_key:
        sys.exit("ALPHA_VANTAGE_KEY not set in environment / .env")

    await init_pool()
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    total  = len(SECTOR_ETFS)

    print(f"ETF backfill — {total} sector ETFs, last {LOOKBACK_DAYS} days.\n")

    processed = 0
    skipped   = 0

    async with httpx.AsyncClient(timeout=30) as client:
        for idx, etf in enumerate(SECTOR_ETFS, 1):
            if await _already_backfilled(etf):
                skipped += 1
                print(f"  [{idx}/{total}] {etf}: already done — skipped.")
                continue

            print(f"  [{idx}/{total}] Fetching {etf} ...", end="", flush=True)
            time_series = await _fetch_daily(client, etf, api_key)

            if not time_series:
                print(" no data.")
                if idx < total:
                    await asyncio.sleep(INTER_REQUEST_DELAY)
                continue

            rows = _build_rows(etf, time_series, cutoff)
            if rows:
                await _insert_rows(rows)
                print(f" {len(rows)} close rows inserted.")
                processed += 1
            else:
                print(" 0 rows in lookback window.")

            if idx < total:
                await asyncio.sleep(INTER_REQUEST_DELAY)

    await close_pool()
    print(
        f"\nDone.  Processed: {processed}  |  Skipped (already done): {skipped}"
    )


if __name__ == "__main__":
    asyncio.run(main())
