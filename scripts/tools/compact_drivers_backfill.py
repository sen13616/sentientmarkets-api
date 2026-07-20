"""
scripts/tools/compact_drivers_backfill.py

One-off backfill: archive then compact the verbose `top_drivers` JSONB on
sentiment_history rows older than DRIVER_COMPACT_DAYS (30 days).

The daily retention_job keeps the table compacted go-forward; this script
handles the existing backlog once. Every affected row's original verbose
JSONB is streamed to a gzipped CSV first — the compaction is aborted unless
the archived count exactly matches the matched count, because `description`
strings (which embed raw signal values stored nowhere else) are dropped
irreversibly by compaction.

Usage:
    python3 scripts/tools/compact_drivers_backfill.py --dry-run
    python3 scripts/tools/compact_drivers_backfill.py [--out PATH]
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

_MATCH_SQL = """
SELECT count(*) FROM sentiment_history
WHERE timestamp < $1 AND jsonb_typeof(top_drivers->0) = 'object'
"""


async def backfill(out_path: str, dry_run: bool) -> None:
    import asyncpg

    from pipeline.scheduler import DRIVER_COMPACT_DAYS
    from scripts.db.connection import close_pool, init_pool
    from scripts.db.queries.sentiment_history import compact_drivers_before

    cutoff = datetime.now(timezone.utc) - timedelta(days=DRIVER_COMPACT_DAYS)
    dsn = os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        matched = await conn.fetchval(_MATCH_SQL, cutoff)
        _log.info("Rows with verbose top_drivers older than %s: %d", cutoff.date(), matched)

        if dry_run:
            _log.info("--dry-run: no archive written, no rows compacted.")
            return
        if matched == 0:
            _log.info("Nothing to do.")
            return

        # ── Archive originals (id, ticker, timestamp, top_drivers) ──────────
        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
        _log.info("Archiving %d verbose top_drivers rows to %s ...", matched, out_path)
        archived = 0
        async with conn.transaction():  # asyncpg cursors require a transaction
            cursor = conn.cursor(
                """
                SELECT id, ticker, timestamp, top_drivers::text
                FROM sentiment_history
                WHERE timestamp < $1 AND jsonb_typeof(top_drivers->0) = 'object'
                ORDER BY id
                """,
                cutoff,
                prefetch=10_000,
            )
            with gzip.open(out_path, "wt", newline="") as fh:
                writer = csv.writer(fh)
                writer.writerow(["id", "ticker", "timestamp", "top_drivers"])
                async for record in cursor:
                    writer.writerow(record.values())
                    archived += 1
                    if archived % 250_000 == 0:
                        _log.info("  archived %d/%d", archived, matched)

        if archived != matched:
            _log.error("Archive count %d != matched count %d — ABORTING compaction.", archived, matched)
            sys.exit(1)
        _log.info("Archive complete: %d rows.", archived)
    finally:
        await conn.close()

    # ── Compact via the same query function the daily retention job uses ────
    await init_pool()
    try:
        compacted = await compact_drivers_before(cutoff)
    finally:
        await close_pool()

    if compacted != archived:
        _log.error(
            "Compacted %d rows but archived %d — counts differ (new rows may have "
            "crossed the cutoff during the run); investigate before re-running.",
            compacted, archived,
        )
        sys.exit(1)
    _log.info("Done: %d rows archived to %s and compacted.", compacted, out_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Archive + compact old top_drivers")
    parser.add_argument("--dry-run", action="store_true", help="count matching rows only")
    parser.add_argument(
        "--out",
        default=os.path.join(
            "exports",
            f"top_drivers_verbose_{datetime.now(timezone.utc):%Y%m%d}.csv.gz",
        ),
    )
    args = parser.parse_args()
    asyncio.run(backfill(args.out, args.dry_run))


if __name__ == "__main__":
    main()
