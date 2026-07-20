"""
scripts/tools/tiered_retention_backfill.py

One-off backfill for the tiered raw_signals retention introduced 2026-07-20:
archive then delete the backlog that the new shorter tiers cover —

  - DERIVED_INTRADAY_SIGNAL_TYPES older than DERIVED_RETENTION_DAYS (45d)
  - QUOTE_SIGNAL_TYPES older than QUOTE_RETENTION_DAYS (14d)

The daily retention_job enforces these tiers go-forward; this script drains
the existing backlog once. Every affected row is streamed to a gzipped CSV
first — the delete is aborted unless the archived count exactly matches the
matched count, because the 30-90d band includes the paper's backtest window
and order_flow/pressure/spread inputs there are not recomputable.

Usage:
    python3 scripts/tools/tiered_retention_backfill.py --dry-run
    python3 scripts/tools/tiered_retention_backfill.py [--out PATH]
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import gzip
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

load_dotenv(override=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
_log = logging.getLogger(__name__)

_MATCH_PREDICATE = """
   (signal_type = ANY($1::text[]) AND timestamp < $2)
OR (signal_type = ANY($3::text[]) AND timestamp < $4)
"""


async def backfill(out_path: str, dry_run: bool) -> None:
    import asyncpg

    from pipeline.scheduler import DERIVED_RETENTION_DAYS, QUOTE_RETENTION_DAYS
    from scripts.db.connection import close_pool, init_pool
    from scripts.db.queries.raw_signals import (
        DERIVED_INTRADAY_SIGNAL_TYPES,
        QUOTE_SIGNAL_TYPES,
        purge_signals_before,
    )

    now = datetime.now(timezone.utc)
    derived_cutoff = now - timedelta(days=DERIVED_RETENTION_DAYS)
    quote_cutoff   = now - timedelta(days=QUOTE_RETENTION_DAYS)
    args = (DERIVED_INTRADAY_SIGNAL_TYPES, derived_cutoff, QUOTE_SIGNAL_TYPES, quote_cutoff)

    dsn = os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        per_type = await conn.fetch(
            f"""
            SELECT signal_type, count(*) AS n FROM raw_signals
            WHERE {_MATCH_PREDICATE}
            GROUP BY 1 ORDER BY 2 DESC
            """,
            *args,
        )
        matched = sum(r["n"] for r in per_type)
        _log.info(
            "Backlog past tiers (derived <%s, quotes <%s):",
            derived_cutoff.date(), quote_cutoff.date(),
        )
        for r in per_type:
            _log.info("  %-24s %10d", r["signal_type"], r["n"])
        _log.info("Total rows to archive + delete: %d", matched)

        if dry_run:
            _log.info("--dry-run: no archive written, no rows deleted.")
            return
        if matched == 0:
            _log.info("Nothing to do.")
            return

        # ── Archive all matching rows ───────────────────────────────────────
        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
        _log.info("Archiving %d rows to %s ...", matched, out_path)
        archived = 0
        async with conn.transaction():  # asyncpg cursors require a transaction
            cursor = conn.cursor(
                f"""
                SELECT id, ticker, signal_type, value, source, upload_type, timestamp
                FROM raw_signals
                WHERE {_MATCH_PREDICATE}
                ORDER BY timestamp
                """,
                *args,
                prefetch=10_000,
            )
            with gzip.open(out_path, "wt", newline="") as fh:
                writer = csv.writer(fh)
                writer.writerow(
                    ["id", "ticker", "signal_type", "value", "source", "upload_type", "timestamp"]
                )
                async for record in cursor:
                    writer.writerow(record.values())
                    archived += 1
                    if archived % 500_000 == 0:
                        _log.info("  archived %d/%d", archived, matched)

        if archived != matched:
            _log.error("Archive count %d != matched count %d — ABORTING delete.", archived, matched)
            sys.exit(1)
        _log.info("Archive complete: %d rows.", archived)
    finally:
        await conn.close()

    # ── Delete via the same production purge path the daily job uses ────────
    await init_pool()
    try:
        n_derived = await purge_signals_before(derived_cutoff, DERIVED_INTRADAY_SIGNAL_TYPES)
        _log.info("Deleted %d derived-intraday rows (<%s)", n_derived, derived_cutoff.date())
        n_quotes = await purge_signals_before(quote_cutoff, QUOTE_SIGNAL_TYPES)
        _log.info("Deleted %d quote rows (<%s)", n_quotes, quote_cutoff.date())
    finally:
        await close_pool()

    deleted = n_derived + n_quotes
    if deleted != archived:
        _log.error(
            "Deleted %d rows but archived %d — counts differ; investigate before re-running.",
            deleted, archived,
        )
        sys.exit(1)
    _log.info("Done: %d rows archived to %s and deleted.", deleted, out_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Archive + delete raw_signals tier backlog")
    parser.add_argument("--dry-run", action="store_true", help="count matching rows only")
    parser.add_argument(
        "--out",
        default=os.path.join(
            "exports",
            f"raw_signals_tiered_{datetime.now(timezone.utc):%Y%m%d}.csv.gz",
        ),
    )
    args = parser.parse_args()
    asyncio.run(backfill(args.out, args.dry_run))


if __name__ == "__main__":
    main()
