"""
db/queries/api_keys.py

Lookup and update operations for the api_keys table.

Key validity (docs/APIACCESSPAGE.md §3.2): a key is valid iff
`is_active AND (expires_at IS NULL OR expires_at > now())`.  Standard
keys keep expires_at NULL and never expire; demo keys (key_type='demo')
carry a sliding expiry that every successful lookup pushes forward by
DEMO_KEY_TTL_DAYS — inside the same UPDATE that already writes
last_used_at, so the slide costs zero extra statements.
"""
from __future__ import annotations

import os
from datetime import datetime

from scripts.db.connection import get_pool

DEMO_KEY_TTL_DAYS = int(os.getenv("DEMO_KEY_TTL_DAYS", "7"))


async def get_key_tier(key_hash: str) -> str | None:
    """
    Return the tier for a valid API key hash, or None if not found,
    inactive, or expired.

    Also updates last_used_at on each successful lookup, and — for demo
    keys — slides expires_at forward by DEMO_KEY_TTL_DAYS in the same
    statement.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE api_keys
               SET last_used_at = now(),
                   expires_at   = CASE WHEN key_type = 'demo'
                                       THEN now() + make_interval(days => $2)
                                       ELSE expires_at END
             WHERE key_hash   = $1
               AND is_active  = TRUE
               AND (expires_at IS NULL OR expires_at > now())
            RETURNING tier
            """,
            key_hash,
            DEMO_KEY_TTL_DAYS,
        )
    return row["tier"] if row else None


async def get_key_tier_readonly(key_hash: str) -> str | None:
    """
    SELECT-only tier lookup — same validity rule as get_key_tier() but with
    no last_used_at write and no demo-key expiry slide.

    Used only on the degraded (Redis-unavailable) auth path: when the tier
    cache is down, every request would otherwise fall back to get_key_tier()
    and drive one DB UPDATE per request. An unauthenticated, unthrottled
    /health flood of arbitrary tokens could turn that into a DB-write
    amplifier. This read-only variant keeps authentication working during a
    Redis outage without the write cost (last_used_at telemetry simply
    doesn't advance while Redis is down).
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT tier
              FROM api_keys
             WHERE key_hash   = $1
               AND is_active  = TRUE
               AND (expires_at IS NULL OR expires_at > now())
            """,
            key_hash,
        )
    return row["tier"] if row else None


async def extend_demo_key(key_hash: str) -> datetime | None:
    """
    Refresh the sliding expiry of a still-valid demo key.

    Returns the new expires_at, or None when the hash is absent, inactive,
    expired, or not a demo key (a standard/pro key sent as `existing_key`
    must never be extended or echoed back by the mint endpoint).
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE api_keys
               SET expires_at   = now() + make_interval(days => $2),
                   last_used_at = now()
             WHERE key_hash   = $1
               AND is_active  = TRUE
               AND key_type   = 'demo'
               AND expires_at > now()
            RETURNING expires_at
            """,
            key_hash,
            DEMO_KEY_TTL_DAYS,
        )
    return row["expires_at"] if row else None


async def insert_demo_key(key_hash: str) -> datetime:
    """
    Insert a freshly minted demo key (hash only — plaintext is never
    persisted) and return its expires_at.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO api_keys
                (key_hash, tier, owner_email, label, is_active, key_type, expires_at)
            VALUES
                ($1, 'free', NULL, 'demo', TRUE, 'demo',
                 now() + make_interval(days => $2))
            RETURNING expires_at
            """,
            key_hash,
            DEMO_KEY_TTL_DAYS,
        )
    return row["expires_at"]


async def delete_expired_demo_keys() -> int:
    """
    Delete expired demo keys and return the count.

    Both predicates live in one statement, so a standard key
    (expires_at NULL) can never match — the "never delete standard keys"
    guarantee is structural, not procedural.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM api_keys WHERE key_type = 'demo' AND expires_at < now()"
        )
    return int(result.split()[-1])
