"""
tests/test_auth_cache.py

Unit tests for the Redis-backed tier cache in api/auth.py.

All external I/O (DB, Redis) is mocked — no live connections required.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from api.auth import _TIER_CACHE_MISS, _TIER_CACHE_TTL, _lookup_tier

KEY_HASH = "a" * 64


def _fake_redis(get_returns):
    client = AsyncMock()
    client.get = AsyncMock(return_value=get_returns)
    client.set = AsyncMock()
    return client


class TestLookupTier:
    async def test_cache_hit_skips_db(self):
        redis = _fake_redis("pro")
        with (
            patch("api.auth.get_redis", return_value=redis),
            patch("api.auth.get_key_tier", AsyncMock()) as db,
        ):
            assert await _lookup_tier(KEY_HASH) == "pro"
        db.assert_not_awaited()
        redis.set.assert_not_awaited()

    async def test_cached_miss_marker_returns_none_without_db(self):
        redis = _fake_redis(_TIER_CACHE_MISS)
        with (
            patch("api.auth.get_redis", return_value=redis),
            patch("api.auth.get_key_tier", AsyncMock()) as db,
        ):
            assert await _lookup_tier(KEY_HASH) is None
        db.assert_not_awaited()

    async def test_cache_miss_hits_db_and_populates_cache(self):
        redis = _fake_redis(None)
        with (
            patch("api.auth.get_redis", return_value=redis),
            patch("api.auth.get_key_tier", AsyncMock(return_value="free")) as db,
        ):
            assert await _lookup_tier(KEY_HASH) == "free"
        db.assert_awaited_once_with(KEY_HASH)
        redis.set.assert_awaited_once_with(
            f"auth:tier:{KEY_HASH}", "free", ex=_TIER_CACHE_TTL
        )

    async def test_unknown_key_caches_miss_marker(self):
        redis = _fake_redis(None)
        with (
            patch("api.auth.get_redis", return_value=redis),
            patch("api.auth.get_key_tier", AsyncMock(return_value=None)),
        ):
            assert await _lookup_tier(KEY_HASH) is None
        redis.set.assert_awaited_once_with(
            f"auth:tier:{KEY_HASH}", _TIER_CACHE_MISS, ex=_TIER_CACHE_TTL
        )

    async def test_redis_unavailable_falls_back_to_db(self):
        with (
            patch(
                "api.auth.get_redis",
                side_effect=RuntimeError("Redis client not initialised"),
            ),
            patch("api.auth.get_key_tier", AsyncMock(return_value="pro")) as db,
        ):
            assert await _lookup_tier(KEY_HASH) == "pro"
        db.assert_awaited_once_with(KEY_HASH)

    async def test_redis_get_error_falls_back_to_db(self):
        redis = _fake_redis(None)
        redis.get = AsyncMock(side_effect=ConnectionError("redis down"))
        with (
            patch("api.auth.get_redis", return_value=redis),
            patch("api.auth.get_key_tier", AsyncMock(return_value="free")) as db,
        ):
            assert await _lookup_tier(KEY_HASH) == "free"
        db.assert_awaited_once_with(KEY_HASH)

    async def test_redis_set_error_does_not_break_lookup(self):
        redis = _fake_redis(None)
        redis.set = AsyncMock(side_effect=ConnectionError("redis down"))
        with (
            patch("api.auth.get_redis", return_value=redis),
            patch("api.auth.get_key_tier", AsyncMock(return_value="pro")),
        ):
            assert await _lookup_tier(KEY_HASH) == "pro"


class TestProTierLimit:
    def test_pro_limit_is_600(self):
        from api.rate_limit import _LIMITS

        assert _LIMITS["pro"] == 600
        assert _LIMITS["free"] == 10
