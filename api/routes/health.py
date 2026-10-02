"""
api/routes/health.py

GET /health

Always returns HTTP 200. Never raises an exception.

Without Authorization header:
    {"status": "ok"}

With Authorization: Bearer <token>:
    {"status": "ok", "tier": "pro" | "free" | null}
    (null when key is invalid or not found)

GET /health/pipeline

Unauthenticated freshness probe for an uptime monitor. Returns 200 when the
scoring tick and narrative job have both completed recently, 503 otherwise
(including when Redis is unreachable). Kept separate from /health because
Railway gates deploys on /health — right after a restart the pipeline is
legitimately stale, and failing there would block the deploy that fixes it.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from api.auth import _hash_token, _lookup_tier
from scripts.db.redis import get_redis

_log = logging.getLogger(__name__)

router = APIRouter()

# Max age (minutes) of each job's last successful completion
# (pipeline:last_run:{job}). Scoring runs every 15-30 min and takes ~12 min;
# narrative runs every 30 min and can take ~20-50 min.
PIPELINE_MAX_AGE_MIN: dict[str, int] = {
    "scoring_tick": 60,
    "narrative": 120,
}


@router.get("/health")
async def health(request: Request) -> dict:
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return {"status": "ok"}

    token = auth_header[len("Bearer "):].strip()
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


@router.get("/health/pipeline")
async def health_pipeline() -> JSONResponse:
    now = datetime.now(timezone.utc)
    checks: dict[str, dict] = {}
    healthy = True
    for job_id, max_age in PIPELINE_MAX_AGE_MIN.items():
        check: dict = {"last_run": None, "age_minutes": None, "max_age_minutes": max_age}
        try:
            raw = await get_redis().get(f"pipeline:last_run:{job_id}")
            if raw is not None:
                if isinstance(raw, bytes):
                    raw = raw.decode()
                age = (now - datetime.fromisoformat(raw)).total_seconds() / 60
                check["last_run"] = raw
                check["age_minutes"] = round(age, 1)
        except Exception as exc:
            _log.warning("health_pipeline: last_run lookup failed for %s: %s", job_id, exc)
        check["ok"] = check["age_minutes"] is not None and check["age_minutes"] <= max_age
        healthy = healthy and check["ok"]
        checks[job_id] = check

    return JSONResponse(
        status_code=200 if healthy else 503,
        content={"status": "ok" if healthy else "stale", "checks": checks},
    )
