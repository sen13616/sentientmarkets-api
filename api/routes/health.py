"""
api/routes/health.py

GET /health

Always returns HTTP 200. Never raises an exception.

Without Authorization header:
    {"status": "ok"}

With Authorization: Bearer <token>:
    {"status": "ok", "tier": "pro" | "free" | null}
    (null when key is invalid or not found)

GET /health/pipeline lives in api/routes/pipeline_health.py. It is kept
separate from /health because Railway gates deploys on /health — right after
a restart the pipeline is legitimately stale, and failing there would block
the deploy that fixes it.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Request

from api.auth import _hash_token, _lookup_tier

_log = logging.getLogger(__name__)

router = APIRouter()


@router.get("/health")
async def health(request: Request) -> dict:
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return {"status": "ok"}

    token = auth_header[len("Bearer ") :].strip()
    if not token:
        return {"status": "ok"}

    try:
        # Reuse the Redis-cached tier lookup (caches hits AND misses, 60s TTL)
        # so unauthenticated /health calls with arbitrary tokens cannot drive a
        # DB write (last_used_at) per request.
        key_hash = _hash_token(token)
        tier = await _lookup_tier(key_hash)
        return {"status": "ok", "tier": tier}
    except Exception as exc:
        _log.debug("health check tier lookup failed (returning ok): %s", exc)
        return {"status": "ok"}
