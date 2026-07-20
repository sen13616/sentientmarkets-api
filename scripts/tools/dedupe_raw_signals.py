"""
scripts/tools/dedupe_raw_signals.py

One-off cleanup of historical duplicate rows in raw_signals.

Duplicates are rows sharing the natural key (ticker, signal_type, timestamp,
value, source) — the same key the NOT EXISTS insert guard in
scripts/db/queries/raw_signals.py enforces go-forward. For each duplicate
group the lowest-id row is kept; the rest are archived to a gzipped CSV and
then deleted in batches.

Usage:
    python3 scripts/tools/dedupe_raw_signals.py --dry-run
    python3 scripts/tools/dedupe_raw_signals.py [--batch-size 50000] [--out PATH]

The delete is aborted unless the archived row count exactly matches the
duplicate count found in the database.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import gzip
import logging
import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

load_dotenv(override=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
_log = logging.getLogger(__name__)

_DUPE_ID_SQL = """
CREATE TEMPORARY TABLE dupe_ids AS
SELECT id, signal_type
FROM (
    SELECT id, signal_type,
           row_number() OVER (
               PARTITION BY ticker, signal_type, timestamp, value, source
               ORDER BY id
           ) AS rn
    FROM raw_signals
) t
WHERE rn > 1
"""


async def dedupe(batch_size: int, out_path: str, dry_run: bool) -> None:
    import asyncpg

    dsn = os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        _log.info("Scanning raw_signals for natural-key duplicates (keeps lowest id per group)...")
        await conn.execute(_DUPE_ID_SQL)
        await conn.execute("CREATE INDEX ON dupe_ids (id)")

        per_type = await conn.fetch(
            "SELECT signal_type, count(*) AS n FROM dupe_ids GROUP BY 1 ORDER BY 2 DESC"
        )
        total = sum(r["n"] for r in per_type)
        for r in per_type:
            _log.info("  %-24s %10d", r["signal_type"], r["n"])
        _log.info("Total duplicate rows: %d", total)

        if dry_run:
            _log.info("--dry-run: no archive written, no rows deleted.")
            return
        if total == 0:
            _log.info("Nothing to do.")
            return

        # ── Archive every duplicate row to gzipped CSV ──────────────────────
        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
        _log.info("Archiving %d rows to %s ...", total, out_path)
        archived = 0
        async with conn.transaction():  # asyncpg cursors require a transaction
            cursor = conn.cursor(
                "SELECT r.* FROM raw_signals r JOIN dupe_ids d USING (id) ORDER BY r.id",
                prefetch=10_000,
            )
            with gzip.open(out_path, "wt", newline="") as fh:
                writer = None
                async for record in cursor:
                    if writer is None:
                        writer = csv.writer(fh)
                        writer.writerow(record.keys())
                    writer.writerow(record.values())
                    archived += 1
                    if archived % 500_000 == 0:
                        _log.info("  archived %d/%d", archived, total)

        if archived != total:
            _log.error("Archive count %d != duplicate count %d — ABORTING delete.", archived, total)
            sys.exit(1)
        _log.info("Archive complete: %d rows.", archived)

        # ── Delete in batches (short transactions against the live scheduler) ─
        deleted = 0
        while True:
            ids = [
                r["id"]
                for r in await conn.fetch(
                    "SELECT id FROM dupe_ids ORDER BY id LIMIT $1", batch_size
                )
            ]
            if not ids:
                break
            async with conn.transaction():
                status = await conn.execute(
                    "DELETE FROM raw_signals WHERE id = ANY($1::int[])", ids
                )
                await conn.execute("DELETE FROM dupe_ids WHERE id = ANY($1::int[])", ids)
            deleted += int(status.split()[-1])
            _log.info("  deleted %d/%d", deleted, total)

        if deleted != archived:
            _log.error("Deleted %d rows but archived %d — investigate before re-running.", deleted, archived)
            sys.exit(1)
        _log.info("Done: %d duplicate rows archived to %s and deleted.", deleted, out_path)
    finally:
        await conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Archive + delete raw_signals duplicates")
    parser.add_argument("--dry-run", action="store_true", help="count duplicates only")
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument(
        "--out",
        default=os.path.join(
            "exports",
            f"raw_signals_dupes_{datetime.now(timezone.utc):%Y%m%d}.csv.gz",
        ),
    )
    args = parser.parse_args()
    asyncio.run(dedupe(args.batch_size, args.out, args.dry_run))


if __name__ == "__main__":
    main()
