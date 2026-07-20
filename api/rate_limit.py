"""
api/rate_limit.py

Per-tier fixed-window rate limiting backed by Redis.

Limits
------
    free  — 10 requests / minute
    pro   — 600 requests / minute

Each API key gets its own Redis counter key with a 60-second TTL.  The
INCR + EXPIRE pair runs as a single Lua script so a counter can never be
left behind without a TTL (which would lock the key out permanently once
the count exceeded its limit).

Note this is a fixed 60-second window, not a sliding one: a client can
burst up to 2× its per-minute limit across a window boundary.

If Redis is unavailable the limiter fails OPEN — the request is allowed
through unthrottled (and logged) rather than 500-ing, matching auth's
graceful-degradation contract.

Routes enforce the quota through the `rate_limited` dependency, which
chains authentication and keys the bucket off the same parsed bearer
token that authentication used.
"""
from __future__ import annotations

import hashlib
import logging

from fastapi import Depends, HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from api.auth import authenticate
from scripts.db.redis import get_redis

_log = logging.getLogger(__name__)

_LIMITS: dict[str, int] = {
    "free": 10,
    "pro":  600,
}

_bearer = HTTPBearer(auto_error=False)

# The TTL < 0 check (rather than count == 1) also heals any counter that a
# pre-Lua deploy left behind without an expiry.
_INCR_WITH_TTL = """
local count = redis.call('INCR', KEYS[1])
if redis.call('TTL', KEYS[1]) < 0 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return count
"""


async def check_rate_limit(raw_token: str, tier: str) -> None:
    """
    Raise HTTP 429 if the caller has exceeded their per-minute quota.

    Parameters
    ----------
    raw_token : The plaintext Bearer token (used only to derive a Redis key).
    tier      : 'free' or 'pro'.
    """
    key_hash = hashlib.sha256(raw_token.encode()).hexdigest()
    redis_key = f"rate:{key_hash}"

    try:
        count = await get_redis().eval(_INCR_WITH_TTL, 1, redis_key, 60)
    except Exception as exc:
        # Fail open: a Redis outage must not take the whole API down. Auth
        # already degrades to a direct DB lookup when Redis is unavailable, so
        # we match that contract — allow the request through, unthrottled, and
        # log it rather than 500-ing every /v1 call.
        _log.warning("rate limit check skipped (Redis unavailable): %s", exc)
        return

    limit = _LIMITS.get(tier, _LIMITS["free"])
    if count > limit:
        raise HTTPException(
            status_code=429,
            detail={
                "error":   "rate_limit_exceeded",
                "message": "Too many requests for your tier",
            },
        )


async def rate_limited(
    tier: str = Depends(authenticate),
    credentials: HTTPAuthorizationCredentials | None = Security(_bearer),
) -> str:
    """
    FastAPI dependency: authenticate, then enforce the per-tier quota.

    Returns the caller's tier, so routes can swap `Depends(authenticate)`
    for `Depends(rate_limited)` unchanged.  The bucket is keyed off the
    HTTPBearer-parsed token — not a re-parse of the raw header — so scheme
    casing ("bearer" vs "Bearer") cannot split one key across buckets.

    `credentials` can only be None when `authenticate` is overridden
    (tests); real requests without credentials already failed with 401.
    """
    if credentials is not None:
        await check_rate_limit(credentials.credentials, tier)
    return tier
