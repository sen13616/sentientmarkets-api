#!/usr/bin/env python3
"""
tools/seed_company_names.py

Populate the company_name column in ticker_universe from the hardcoded
COMPANY_NAMES dict in tools/company_names.py.

Run after applying migrations/005_add_company_name.sql:

    psql $DATABASE_URL < migrations/005_add_company_name.sql
    python3 tools/seed_company_names.py

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

from company_names import COMPANY_NAMES


def _dsn() -> str:
    url = os.environ["DATABASE_URL"]
    # asyncpg uses postgresql://, not postgresql+asyncpg://
    return url.replace("postgresql+asyncpg://", "postgresql://")


async def main() -> None:
    dsn = _dsn()
    conn = await asyncpg.connect(dsn)
    try:
        updated = 0
        skipped = 0
        for ticker, name in COMPANY_NAMES.items():
            result = await conn.execute(
                """
                UPDATE ticker_universe
                   SET company_name = $2
                 WHERE ticker = $1
                """,
                ticker,
                name,
            )
            # result is like "UPDATE 1" or "UPDATE 0"
            n = int(result.split()[-1])
            if n:
                updated += 1
            else:
                skipped += 1

        print(f"Done. Updated: {updated}  Not found: {skipped}")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
