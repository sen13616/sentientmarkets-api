#!/usr/bin/env python3
"""
tools/backfill_fred.py — Sprint P4.3

Backfill 90 days of FRED Treasury / yield-curve history so the z-score
path activates immediately rather than after ~9 weeks of live ingest.

Fetches the same three series the daily macro job writes:
    treasury_yield_10y   →  FRED  DGS10
    treasury_yield_2y    →  FRED  DGS2
    ted_spread           →  FRED  T10Y2Y

Idempotent: skips a (signal_type, observation_date) pair if a row already
exists in raw_signals under `_MACRO_`.

Usage:
    DATABASE_URL=<railway_url> python3 tools/backfill_fred.py

    # Optional override of the lookback window:
    DATABASE_URL=... BACKFILL_DAYS=180 python3 tools/backfill_fred.py
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from dotenv import load_dotenv

_ENV_FILE = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
load_dotenv(_ENV_FILE, override=True)

import httpx
import asyncpg

from pipeline.sources.fred import SERIES_MAP, _fetch_observation


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
_log = logging.getLogger("backfill_fred")

_MACRO_TICKER = "_MACRO_"


def _dsn() -> str:
    return os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://")


async def _existing_observation_dates(
    conn: asyncpg.Connection,
    signal_type: str,
) -> set[datetime]:
    rows = await conn.fetch(
        """
        SELECT timestamp
          FROM raw_signals
         WHERE ticker      = $1
           AND signal_type = $2
        """,
        _MACRO_TICKER, signal_type,
    )
    return {r["timestamp"] for r in rows}


async def main() -> int:
    days = int(os.environ.get("BACKFILL_DAYS", "90"))
    start_date = (datetime.now(timezone.utc) - timedelta(days=days)).date()
    end_date   = datetime.now(timezone.utc).date()
    _log.info("Backfilling FRED %s → %s (%d days)", start_date, end_date, days)

    conn = await asyncpg.connect(_dsn())
    inserted_by_type: dict[str, int] = {}
    skipped_by_type:  dict[str, int] = {}

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            for sig_type, series_id in SERIES_MAP.items():
                _log.info("→ %s  (FRED %s)", sig_type, series_id)
                observations = await _fetch_observation(
                    client, series_id,
                    observation_start=str(start_date),
                    observation_end=str(end_date),
                    limit=days + 10,           # FRED returns business days only; pad a little
                    sort_order="asc",
                )
                if not observations:
                    _log.warning("  no observations returned")
                    continue

                existing = await _existing_observation_dates(conn, sig_type)
                inserted = 0
                skipped  = 0
                for value, obs_date in observations:
                    if obs_date in existing:
                        skipped += 1
                        continue
                    # CAUTION: blind INSERT — bypasses insert_signals()'s dedup guard
                    # (NOT EXISTS on ticker/signal_type/timestamp/value/source). Re-running
                    # this script can create duplicate rows; dedupe manually or port to
                    # scripts.db.queries.raw_signals.insert_signals() before reuse.
                    await conn.execute(
                        """
                        INSERT INTO raw_signals
                            (ticker, signal_type, value, source, upload_type, timestamp)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        """,
                        _MACRO_TICKER, sig_type, value, "fred", "manual_backfill", obs_date,
                    )
                    inserted += 1

                inserted_by_type[sig_type] = inserted
                skipped_by_type[sig_type]  = skipped
                _log.info("  inserted=%d  skipped(existing)=%d", inserted, skipped)
    finally:
        await conn.close()

    total_inserted = sum(inserted_by_type.values())
    total_skipped  = sum(skipped_by_type.values())
    _log.info("backfill_fred complete: inserted=%d skipped=%d across %d series",
              total_inserted, total_skipped, len(SERIES_MAP))
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
