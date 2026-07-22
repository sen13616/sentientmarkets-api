"""
pipeline/scripts/backfill_short_volume.py

Backfill 90 trading days of FINRA REGSHO daily short volume data into
raw_signals.

Usage
-----
    python -m pipeline.scripts.backfill_short_volume                 # default: last 90 trading days
    python -m pipeline.scripts.backfill_short_volume --days 60       # custom day count
    python -m pipeline.scripts.backfill_short_volume --start 2025-01-02 --end 2025-05-22

Design
------
- Reuses fetch_short_volume_for_date() from pipeline/sources/short_volume.py
  (same parsing code as live ingestion — no duplication).
- Idempotent: checks for existing rows per (date, signal_type) before inserting.
  Re-running is a safe no-op.
- Throttled: asyncio.sleep(0.5) between fetches (max 2 req/s to FINRA).
- Timestamps: each historical file's date at 21:00 UTC (market close).
- upload_type: 'manual_backfill' to distinguish from live ingestion.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import httpx

# Ensure project root is importable when running as a script
_project_root = str(Path(__file__).resolve().parent.parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from scripts.db.connection import close_pool, get_pool, init_pool
from pipeline.sources.short_volume import fetch_short_volume_for_date

_log = logging.getLogger(__name__)

# US market holidays in 2025 (dates when FINRA does not publish files).
# This is a minimal list covering the backfill window; extend as needed.
US_MARKET_HOLIDAYS_2025 = frozenset({
    date(2025, 1, 1),   # New Year's Day
    date(2025, 1, 20),  # MLK Day
    date(2025, 2, 17),  # Presidents' Day
    date(2025, 4, 18),  # Good Friday
    date(2025, 5, 26),  # Memorial Day
    date(2025, 6, 19),  # Juneteenth
    date(2025, 7, 4),   # Independence Day
    date(2025, 9, 1),   # Labor Day
    date(2025, 11, 27), # Thanksgiving
    date(2025, 12, 25), # Christmas Day
})


def is_trading_day(d: date) -> bool:
    """Return True if d is a weekday and not a known US market holiday."""
    if d.isoweekday() > 5:
        return False
    if d in US_MARKET_HOLIDAYS_2025:
        return False
    return True


def trading_days_back(ref_date: date, n: int) -> list[date]:
    """Return the last *n* trading days ending at or before *ref_date*, newest first."""
    result: list[date] = []
    candidate = ref_date
    # Walk back far enough to collect n trading days (skip weekends + holidays)
    while len(result) < n:
        if is_trading_day(candidate):
            result.append(candidate)
        candidate -= timedelta(days=1)
        # Safety: don't go back more than 200 calendar days for 90 trading days
        if (ref_date - candidate).days > 200:
            break
    return result


def trading_days_range(start: date, end: date) -> list[date]:
    """Return all trading days in [start, end], oldest first."""
    result: list[date] = []
    candidate = start
    while candidate <= end:
        if is_trading_day(candidate):
            result.append(candidate)
        candidate += timedelta(days=1)
    return result


async def _existing_dates(pool, signal_type: str) -> set[date]:
    """Return the set of dates that already have data for this signal type."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT DISTINCT DATE(timestamp) as d
               FROM raw_signals
               WHERE signal_type = $1""",
            signal_type,
        )
    return {r["d"] for r in rows}


async def backfill(days_back: int = 90, start: date | None = None, end: date | None = None) -> None:
    """
    Backfill FINRA short volume data.

    Parameters
    ----------
    days_back : Number of trading days to backfill (default 90).
    start, end : Explicit date range (overrides days_back if both set).
    """
    await init_pool()
    pool = await get_pool()

    # Determine date list
    if start and end:
        dates = trading_days_range(start, end)
    else:
        # Default: use real-world "today" based on the most recent available data
        # FINRA has real-world dates (2025), so we reference that
        ref = end or date.today()
        dates = list(reversed(trading_days_back(ref, days_back)))  # oldest first

    _log.info("Backfill window: %s → %s (%d trading days)", dates[0], dates[-1], len(dates))

    # Check which dates already have data (idempotency)
    existing = await _existing_dates(pool, "short_volume_ratio_otc")
    to_fetch = [d for d in dates if d not in existing]
    _log.info(
        "Already have %d dates in DB, %d to fetch",
        len(dates) - len(to_fetch), len(to_fetch),
    )

    if not to_fetch:
        _log.info("Nothing to backfill — all dates already present.")
        await close_pool()
        return

    # Fetch active tickers for filtering
    async with pool.acquire() as conn:
        ticker_rows = await conn.fetch(
            "SELECT ticker FROM ticker_universe WHERE tier = 'tier1_supported'"
        )
    universe = {r["ticker"] for r in ticker_rows}
    _log.info("Active universe: %d tickers", len(universe))

    fetched = 0
    skipped = 0
    total_rows = 0

    async with httpx.AsyncClient(timeout=30.0) as client:
        for i, target_date in enumerate(to_fetch):
            data = await fetch_short_volume_for_date(target_date, client)

            if data is None:
                _log.warning("  %s: no data available (skipped)", target_date.isoformat())
                skipped += 1
                await asyncio.sleep(0.5)
                continue

            # Build signal timestamp: target_date at 21:00 UTC (market close)
            ts = datetime(
                target_date.year, target_date.month, target_date.day,
                21, 0, 0, tzinfo=timezone.utc,
            )

            rows: list[tuple] = []
            for ticker, vols in data.items():
                if ticker not in universe:
                    continue
                short_vol = vols["short_volume"]
                total_vol = vols["total_volume"]
                ratio = short_vol / total_vol if total_vol > 0 else 0.0
                rows.append((ticker, "short_volume_otc",       float(short_vol), "finra_regsho", "manual_backfill", ts))
                rows.append((ticker, "short_volume_total_otc", float(total_vol), "finra_regsho", "manual_backfill", ts))
                rows.append((ticker, "short_volume_ratio_otc", ratio,            "finra_regsho", "manual_backfill", ts))

            if rows:
                async with pool.acquire() as conn:
                    # CAUTION: blind INSERT — bypasses insert_signals()'s dedup guard
                    # (NOT EXISTS on ticker/signal_type/timestamp/value/source). Re-running
                    # this script can create duplicate rows; dedupe manually or port to
                    # scripts.db.queries.raw_signals.insert_signals() before reuse.
                    await conn.executemany(
                        """INSERT INTO raw_signals
                             (ticker, signal_type, value, source, upload_type, timestamp)
                           VALUES ($1, $2, $3, $4, $5, $6)""",
                        rows,
                    )

            n_tickers = len(rows) // 3
            total_rows += len(rows)
            fetched += 1

            _log.info(
                "  [%d/%d] %s: %d tickers (%d rows)",
                i + 1, len(to_fetch), target_date.isoformat(), n_tickers, len(rows),
            )

            # Throttle: 0.5s between fetches (max 2 req/s)
            await asyncio.sleep(0.5)

    _log.info(
        "Backfill complete: %d dates fetched, %d skipped, %d total rows inserted",
        fetched, skipped, total_rows,
    )
    await close_pool()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    parser = argparse.ArgumentParser(description="Backfill FINRA short volume data")
    parser.add_argument("--days", type=int, default=90, help="Number of trading days to backfill (default 90)")
    parser.add_argument("--start", type=str, default=None, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None, help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    start = date.fromisoformat(args.start) if args.start else None
    end = date.fromisoformat(args.end) if args.end else None

    asyncio.run(backfill(days_back=args.days, start=start, end=end))


if __name__ == "__main__":
    main()
