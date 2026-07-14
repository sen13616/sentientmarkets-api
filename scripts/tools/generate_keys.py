#!/usr/bin/env python3
"""
tools/generate_keys.py

Generate and insert API keys for SentientMarkets into the api_keys table.

Generates two keys:
  sk-sm-prod-*   (Pro tier)
  sk-sm-dev-*    (Free tier)

The plaintext keys are printed once and never stored — only their
SHA-256 hashes are written to the database.

Usage
-----
    python3 tools/generate_keys.py

Requires DATABASE_URL in .env (or the environment).
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import secrets
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from dotenv import load_dotenv

_ENV_FILE = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
load_dotenv(_ENV_FILE, override=True)

import asyncpg


def _dsn() -> str:
    url = os.environ["DATABASE_URL"]
    return url.replace("postgresql+asyncpg://", "postgresql://")


def _generate_key(prefix: str) -> str:
    """Return a cryptographically random key with the given prefix."""
    token = secrets.token_urlsafe(32)
    return f"{prefix}{token}"


def _hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


async def main() -> None:
    dsn = _dsn()
    conn = await asyncpg.connect(dsn)

    keys_to_insert = [
        ("sk-sm-prod-", "pro",  "SentientMarkets Pro key"),
        ("sk-sm-dev-",  "free", "SentientMarkets Free/dev key"),
    ]

    print("\nGenerating SentientMarkets API keys…\n")
    print("=" * 70)

    try:
        for prefix, tier, label in keys_to_insert:
            plaintext = _generate_key(prefix)
            key_hash  = _hash_key(plaintext)

            await conn.execute(
                """
                INSERT INTO api_keys (key_hash, tier, owner_email, is_active)
                VALUES ($1, $2, $3, TRUE)
                ON CONFLICT (key_hash) DO NOTHING
                """,
                key_hash,
                tier,
                "sentientmarkets@internal",
            )

            print(f"  {label}")
            print(f"  Tier     : {tier}")
            print(f"  Key      : {plaintext}")
            print(f"  Hash     : {key_hash[:16]}…")
            print()

        print("=" * 70)
        print("Keys inserted into api_keys table.")
        print("Save the plaintext keys above — they cannot be recovered.\n")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
