"""tests/test_api_hardening.py

Regression tests for the 2026-07-20 audit fixes:
  - /health routes through the Redis-cached tier lookup (no per-call DB write)
  - rate limiter fails OPEN when Redis is unavailable (no 500)
  - assembler tolerates a null confidence score / flags without crashing
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest


# ---------------------------------------------------------------------------
# /health uses the cached lookup, not a direct DB write
# ---------------------------------------------------------------------------

async def test_health_uses_cached_lookup_not_direct_db():
    """health() must call the cached _lookup_tier, never get_key_tier directly."""
    from api.routes import health

    from fastapi import Request

    scope = {
        "type": "http",
        "headers": [(b"authorization", b"Bearer sk-test")],
    }
    req = Request(scope)

    with (
        patch.object(health, "_lookup_tier", new_callable=AsyncMock, return_value="pro") as mock_lookup,
        patch("scripts.db.queries.api_keys.get_key_tier", new_callable=AsyncMock) as mock_db,
    ):
        result = await health.health(req)

    assert result == {"status": "ok", "tier": "pro"}
    mock_lookup.assert_awaited_once()
    mock_db.assert_not_called()  # no direct DB (last_used_at) write


# ---------------------------------------------------------------------------
# Rate limiter fails open on Redis error
# ---------------------------------------------------------------------------

async def test_rate_limit_fails_open_on_redis_error():
    """A Redis outage must not raise — the request is allowed through."""
    from api.rate_limit import check_rate_limit

    broken = AsyncMock()
    broken.eval = AsyncMock(side_effect=ConnectionError("redis down"))
    with patch("api.rate_limit.get_redis", return_value=broken):
        # Must NOT raise (no HTTPException, no ConnectionError)
        await check_rate_limit("some-token", "free")


async def test_rate_limit_still_enforces_when_redis_up():
    """Sanity: over-limit still raises 429 when Redis works."""
    from fastapi import HTTPException

    from api.rate_limit import check_rate_limit

    client = AsyncMock()
    client.eval = AsyncMock(return_value=11)  # over free limit of 10
    with patch("api.rate_limit.get_redis", return_value=client):
        with pytest.raises(HTTPException) as exc:
            await check_rate_limit("tok", "free")
    assert exc.value.status_code == 429


# ---------------------------------------------------------------------------
# Assembler tolerates null confidence score / flags
# ---------------------------------------------------------------------------

def _minimal_state():
    return {
        "ticker": "TEST",
        "composite_score": 0.0,          # legitimate 0.0 must survive
        "confidence": {"score": None, "flags": None},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def test_build_free_handles_null_confidence():
    from api.response.assembler import _build_free

    resp = _build_free(_minimal_state())
    assert resp.score == 0          # 0.0 composite preserved, not dropped
    assert resp.confidence == 0     # None score → 0, no crash


def test_build_pro_handles_null_confidence_and_flags():
    from api.response.assembler import _build_pro

    resp = _build_pro(_minimal_state())
    assert resp.confidence == 0
    assert resp.confidence_flags == []   # None flags → [], no list(None) crash
