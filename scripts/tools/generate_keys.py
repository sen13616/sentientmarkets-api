#!/usr/bin/env python3
"""
scripts/tools/generate_keys.py

Manage per-consumer API keys in the api_keys table (migration 009 adds
`label`). Every consumer (website, research job, backtest) should have its
own key so one cannot exhaust another's rate-limit bucket.

Plaintext keys are printed once and never stored — only SHA-256 hashes are
written to the database.

Usage
-----
    python3 scripts/tools/generate_keys.py create --tier pro --label website
    python3 scripts/tools/generate_keys.py create --tier free --label dev \
        --owner someone@example.com
    python3 scripts/tools/generate_keys.py list
    python3 scripts/tools/generate_keys.py revoke --id 3

Requires DATABASE_URL in .env (or the environment).
"""
from __future__ import annotations

import argparse
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

_PREFIXES = {"pro": "sk-sm-prod-", "free": "sk-sm-dev-"}
_DEFAULT_OWNER = "sentientmarkets@internal"


def _dsn() -> str:
    url = os.environ["DATABASE_URL"]
    return url.replace("postgresql+asyncpg://", "postgresql://")


def generate_key(tier: str) -> str:
    """Return a cryptographically random key with the tier's prefix."""
    return f"{_PREFIXES[tier]}{secrets.token_urlsafe(32)}"


def hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


async def _create(conn: asyncpg.Connection, args) -> None:
    plaintext = generate_key(args.tier)
    key_hash = hash_key(plaintext)
    await conn.execute(
        """
        INSERT INTO api_keys (key_hash, tier, owner_email, label, is_active)
        VALUES ($1, $2, $3, $4, TRUE)
        """,
        key_hash,
        args.tier,
        args.owner,
        args.label,
    )
    print("=" * 70)
    print(f"  Label : {args.label}")
    print(f"  Tier  : {args.tier}")
    print(f"  Owner : {args.owner}")
    print(f"  Key   : {plaintext}")
    print(f"  Hash  : {key_hash[:16]}…")
    print("=" * 70)
    print("Save the plaintext key above — it cannot be recovered.")


async def _list(conn: asyncpg.Connection, args) -> None:
    rows = await conn.fetch(
        """
        SELECT id, label, tier, owner_email, is_active, created_at, last_used_at
          FROM api_keys
         ORDER BY id
        """
    )
    if not rows:
        print("no keys in api_keys")
        return
    fmt = "{:>4}  {:<28} {:<5} {:<30} {:<7} {:<11} {}"
    print(fmt.format("id", "label", "tier", "owner", "active", "created", "last_used"))
    for r in rows:
        print(
            fmt.format(
                r["id"],
                (r["label"] or "-")[:28],
                r["tier"],
                (r["owner_email"] or "-")[:30],
                "yes" if r["is_active"] else "NO",
                str(r["created_at"].date()),
                str(r["last_used_at"].date()) if r["last_used_at"] else "-",
            )
        )


async def _revoke(conn: asyncpg.Connection, args) -> None:
    result = await conn.execute(
        "UPDATE api_keys SET is_active = FALSE WHERE id = $1", args.id
    )
    if result.endswith("0"):
        print(f"no key with id {args.id}")
        sys.exit(1)
    print(f"key {args.id} revoked (auth cache propagates within ~60s)")


def _parse_args(argv=None):
    p = argparse.ArgumentParser(description="Manage SentientMarkets API keys")
    sub = p.add_subparsers(dest="command", required=True)

    c = sub.add_parser("create", help="mint a new key (plaintext printed once)")
    c.add_argument("--tier", choices=list(_PREFIXES), required=True)
    c.add_argument("--label", required=True,
                   help="consumer name, e.g. 'website' or 'research'")
    c.add_argument("--owner", default=_DEFAULT_OWNER)

    sub.add_parser("list", help="list keys (never shows hashes or plaintext)")

    r = sub.add_parser("revoke", help="deactivate a key by id")
    r.add_argument("--id", type=int, required=True)

    return p.parse_args(argv)


async def main(argv=None) -> None:
    args = _parse_args(argv)
    conn = await asyncpg.connect(_dsn())
    try:
        await {"create": _create, "list": _list, "revoke": _revoke}[args.command](
            conn, args
        )
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
