#!/usr/bin/env python3
"""
tools/seed_sectors.py

Populate the `sector` column in `ticker_universe` from the static
`TICKER_SECTORS` dict in `tools/sector_map.py` (Sprint P4.1).

Run AFTER applying migrations/008_add_ticker_sector.sql:

    psql $DATABASE_URL < migrations/008_add_ticker_sector.sql
    python3 tools/seed_sectors.py

Idempotent — re-runs only touch rows whose `sector` value actually changes
(DISTINCT FROM guard). Prints counts of updated / unchanged / missing.

Requires DATABASE_URL in .env (or the environment).
"""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from dotenv import load_dotenv

_ENV_FILE = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
load_dotenv(_ENV_FILE, override=True)

import asyncpg

from sector_map import TICKER_SECTORS


def _dsn() -> str:
    url = os.environ["DATABASE_URL"]
    # asyncpg uses postgresql://, not postgresql+asyncpg://
    return url.replace("postgresql+asyncpg://", "postgresql://")


async def main() -> None:
    dsn = _dsn()
    conn = await asyncpg.connect(dsn)
    try:
        updated = 0
        unchanged = 0
        missing = 0
        for ticker, sector in TICKER_SECTORS.items():
            # UPDATE returns "UPDATE N"; N=1 only when sector actually changes
            # (or was NULL). DISTINCT FROM treats NULL as different from any value.
            result = await conn.execute(
                """
                UPDATE ticker_universe
                   SET sector = $2
                 WHERE ticker = $1
                   AND (sector IS DISTINCT FROM $2)
                """,
                ticker, sector,
            )
            # asyncpg's execute returns "UPDATE 0" or "UPDATE 1"
            count = int(result.split()[-1]) if result.startswith("UPDATE") else 0
            if count == 1:
                updated += 1
            else:
                # No row changed: either ticker not in universe, or value already correct.
                exists = await conn.fetchval(
                    "SELECT 1 FROM ticker_universe WHERE ticker = $1", ticker,
                )
                if exists:
                    unchanged += 1
                else:
                    missing += 1
                    print(f"  ⚠ ticker not in universe: {ticker}", file=sys.stderr)

        total_with_sector = await conn.fetchval(
            "SELECT COUNT(*) FROM ticker_universe WHERE sector IS NOT NULL"
        )
        total_null = await conn.fetchval(
            "SELECT COUNT(*) FROM ticker_universe WHERE sector IS NULL"
        )

        print(f"seed_sectors: updated={updated} unchanged={unchanged} missing={missing}")
        print(f"  ticker_universe.sector populated: {total_with_sector} non-null, "
              f"{total_null} null")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
