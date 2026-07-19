"""
api/auth.py

API key authentication.

Incoming Bearer tokens are SHA-256 hashed and looked up in the api_keys
table.  The tier string ('free' or 'pro') is returned on success; an HTTP
401 is raised on failure.

Lookups are cached in Redis (`auth:tier:{key_hash}`, 60s TTL) so the hot
path is pure Redis; the Postgres lookup — which also writes last_used_at —
runs at most once per key per TTL.  Consequences of the TTL:
    - Deactivating a key takes up to 60s to propagate.
    - last_used_at is now accurate only to ~60s.
Unknown keys are cached too (as a miss marker), so repeated bad keys
cannot hammer Postgres.  If Redis is unavailable, auth falls back to the
direct DB lookup rather than failing.
"""
from __future__ import annotations

import hashlib

from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from scripts.db.queries.api_keys import get_key_tier
from scripts.db.redis import get_redis

_bearer = HTTPBearer(auto_error=False)

_TIER_CACHE_TTL = 60
_TIER_CACHE_MISS = "__none__"


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


async def _lookup_tier(key_hash: str) -> str | None:
    """Redis-cached wrapper around get_key_tier(); see module docstring."""
    cache_key = f"auth:tier:{key_hash}"
    try:
        client = get_redis()
        cached = await client.get(cache_key)
    except Exception:
        return await get_key_tier(key_hash)

    if cached is not None:
        return None if cached == _TIER_CACHE_MISS else cached

    tier = await get_key_tier(key_hash)
    try:
        await client.set(
            cache_key,
            tier if tier is not None else _TIER_CACHE_MISS,
            ex=_TIER_CACHE_TTL,
        )
    except Exception:
        pass
    return tier


async def authenticate(
    credentials: HTTPAuthorizationCredentials | None = Security(_bearer),
) -> str:
    """
    FastAPI dependency.  Returns the caller's tier ('free' or 'pro').

    Raises
    ------
    HTTPException(401) — missing, malformed, or inactive API key.
    """
    if credentials is None:
        raise HTTPException(
            status_code=401,
            detail={"error": "unauthorized", "message": "Missing API key"},
        )
    key_hash = _hash_token(credentials.credentials)
    tier = await _lookup_tier(key_hash)
    if tier is None:
        raise HTTPException(
            status_code=401,
            detail={"error": "unauthorized", "message": "Invalid or missing API key"},
        )
    return tier
