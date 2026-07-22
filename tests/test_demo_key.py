"""
Tests for the demo-key feature (docs/APIACCESSPAGE.md §4.7):

  1. Mint returns a working free key (and it authenticates).
  2. Reuse with a valid existing_key creates no row and pushes expiry.
  3. Expired keys are invalid (401 path + SQL contract guards).
  4. Exceeding the per-IP mint cap → 429.
  5. Cleanup deletes only expired demo rows, never standard keys.

DB and Redis are always mocked; patch targets are the names as imported
into api.routes.demo_key / pipeline.scheduler.
"""
from __future__ import annotations

import hashlib
import inspect
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from fastapi.testclient import TestClient
from starlette.middleware.cors import CORSMiddleware

import scripts.db.queries.api_keys as api_keys_queries
from api.auth import authenticate
from api.routes.demo_key import DEMO_KEY_IP_CAP, SITE_ORIGINS, _client_ip
from main import app

ALLOWED_ORIGIN = SITE_ORIGINS[0]
FUTURE = datetime.now(timezone.utc) + timedelta(days=7)


def _sha256(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def _redis_mock(eval_return=1, ttl_return=3600):
    client = AsyncMock()
    client.eval = AsyncMock(return_value=eval_return)
    client.ttl = AsyncMock(return_value=ttl_return)
    return client


@pytest.fixture
def client():
    return TestClient(app)


# ---------------------------------------------------------------------------
# Origin gate
# ---------------------------------------------------------------------------

class TestOriginGate:
    def test_missing_origin_returns_403(self, client):
        r = client.post("/v1/demo-key", json={"existing_key": None})
        assert r.status_code == 403
        assert r.json()["detail"]["error"] == "origin_not_allowed"

    def test_disallowed_origin_returns_403(self, client):
        r = client.post(
            "/v1/demo-key",
            json={"existing_key": None},
            headers={"Origin": "https://evil.example.com"},
        )
        assert r.status_code == 403
        assert r.json()["detail"]["error"] == "origin_not_allowed"

    def test_trailing_slash_origin_is_normalized(self, client):
        with patch(
            "api.routes.demo_key.insert_demo_key",
            AsyncMock(return_value=FUTURE),
        ), patch(
            "api.routes.demo_key.get_redis", return_value=_redis_mock()
        ):
            r = client.post(
                "/v1/demo-key",
                json={"existing_key": None},
                headers={"Origin": ALLOWED_ORIGIN + "/"},
            )
        assert r.status_code == 200


# ---------------------------------------------------------------------------
# §4.7.1 — Mint returns a working free key
# ---------------------------------------------------------------------------

class TestMint:
    def test_mint_returns_free_key(self, client):
        insert = AsyncMock(return_value=FUTURE)
        with patch("api.routes.demo_key.insert_demo_key", insert), patch(
            "api.routes.demo_key.get_redis", return_value=_redis_mock()
        ):
            r = client.post(
                "/v1/demo-key",
                json={"existing_key": None},
                headers={"Origin": ALLOWED_ORIGIN},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["tier"] == "free"
        assert body["rate_limit_per_min"] == 10
        assert body["api_key"].startswith("sk-sm-free-")
        # The stored hash is the SHA-256 of the returned plaintext.
        insert.assert_awaited_once_with(_sha256(body["api_key"]))

    async def test_minted_key_authenticates(self, client):
        """The plaintext handed to the browser passes api.auth.authenticate
        when its hash is in the DB (contract at the seam — a live round-trip
        needs a real DB)."""
        with patch("api.routes.demo_key.insert_demo_key", AsyncMock(return_value=FUTURE)), patch(
            "api.routes.demo_key.get_redis", return_value=_redis_mock()
        ):
            r = client.post(
                "/v1/demo-key",
                json={"existing_key": None},
                headers={"Origin": ALLOWED_ORIGIN},
            )
        plaintext = r.json()["api_key"]
        key_hash = _sha256(plaintext)

        async def db_lookup(h):
            return "free" if h == key_hash else None

        creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials=plaintext)
        with patch("api.auth.get_redis", side_effect=Exception("no redis")), patch(
            "api.auth.get_key_tier", AsyncMock(side_effect=db_lookup)
        ):
            assert await authenticate(creds) == "free"

    def test_mint_survives_redis_outage(self, client):
        """Cap check fails open — matches api/rate_limit.py's contract."""
        with patch("api.routes.demo_key.insert_demo_key", AsyncMock(return_value=FUTURE)), patch(
            "api.routes.demo_key.get_redis", side_effect=Exception("redis down")
        ):
            r = client.post(
                "/v1/demo-key",
                json={"existing_key": None},
                headers={"Origin": ALLOWED_ORIGIN},
            )
        assert r.status_code == 200


# ---------------------------------------------------------------------------
# §4.7.2 — Reuse: no new row, expiry pushed
# ---------------------------------------------------------------------------

class TestReuse:
    def test_valid_existing_key_is_extended_not_replaced(self, client):
        existing = "sk-sm-free-existing-token"
        insert = AsyncMock()
        redis = _redis_mock()
        with patch(
            "api.routes.demo_key.extend_demo_key", AsyncMock(return_value=FUTURE)
        ) as extend, patch(
            "api.routes.demo_key.insert_demo_key", insert
        ), patch(
            "api.routes.demo_key.get_redis", return_value=redis
        ):
            r = client.post(
                "/v1/demo-key",
                json={"existing_key": existing},
                headers={"Origin": ALLOWED_ORIGIN},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["api_key"] == existing
        assert body["expires_at"] == FUTURE.isoformat().replace("+00:00", "Z")
        extend.assert_awaited_once_with(_sha256(existing))
        insert.assert_not_awaited()          # no new row
        redis.eval.assert_not_awaited()      # extend consumes no mint cap

    def test_invalid_existing_key_falls_through_to_mint(self, client):
        """Expired / unknown / standard existing_key (extend returns None)
        → cap is enforced and a fresh key is minted."""
        old = "sk-sm-free-expired-token"
        redis = _redis_mock()
        with patch(
            "api.routes.demo_key.extend_demo_key", AsyncMock(return_value=None)
        ), patch(
            "api.routes.demo_key.insert_demo_key", AsyncMock(return_value=FUTURE)
        ), patch(
            "api.routes.demo_key.get_redis", return_value=redis
        ):
            r = client.post(
                "/v1/demo-key",
                json={"existing_key": old},
                headers={"Origin": ALLOWED_ORIGIN},
            )
        assert r.status_code == 200
        assert r.json()["api_key"] != old
        redis.eval.assert_awaited_once()


# ---------------------------------------------------------------------------
# §4.7.4 — IP cap
# ---------------------------------------------------------------------------

class TestMintCap:
    def test_exceeding_cap_returns_429(self, client):
        insert = AsyncMock()
        with patch("api.routes.demo_key.insert_demo_key", insert), patch(
            "api.routes.demo_key.get_redis",
            return_value=_redis_mock(eval_return=DEMO_KEY_IP_CAP + 1, ttl_return=3600),
        ):
            r = client.post(
                "/v1/demo-key",
                json={"existing_key": None},
                headers={"Origin": ALLOWED_ORIGIN},
            )
        assert r.status_code == 429
        detail = r.json()["detail"]
        assert detail["error"] == "demo_key_rate_limited"
        assert detail["retry_after_seconds"] == 3600
        insert.assert_not_awaited()


# ---------------------------------------------------------------------------
# §4.7.3 — Expiry contract (auth + SQL guards)
# ---------------------------------------------------------------------------

class TestExpiryContract:
    async def test_expired_key_gets_401(self):
        """get_key_tier returning None (expired = no row) → 401."""
        creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials="sk-sm-free-x")
        with patch("api.auth.get_redis", side_effect=Exception("no redis")), patch(
            "api.auth.get_key_tier", AsyncMock(return_value=None)
        ):
            with pytest.raises(HTTPException) as exc:
                await authenticate(creds)
        assert exc.value.status_code == 401

    def test_get_key_tier_sql_enforces_expiry_and_slides_demo(self):
        src = inspect.getsource(api_keys_queries.get_key_tier)
        assert "expires_at IS NULL OR expires_at > now()" in src
        assert "CASE WHEN key_type = 'demo'" in src
        assert "is_active" in src

    def test_extend_sql_requires_demo_type_and_unexpired(self):
        src = inspect.getsource(api_keys_queries.extend_demo_key)
        assert "key_type   = 'demo'" in src
        assert "expires_at > now()" in src


# ---------------------------------------------------------------------------
# §4.7.5 — Cleanup: demo-only, never standard
# ---------------------------------------------------------------------------

class TestCleanup:
    def test_delete_sql_targets_only_expired_demo_rows(self):
        src = inspect.getsource(api_keys_queries.delete_expired_demo_keys)
        assert "key_type = 'demo' AND expires_at < now()" in src

    async def test_job_deletes_and_records_run(self):
        from pipeline.scheduler import demo_key_cleanup_job

        delete = AsyncMock(return_value=3)
        record = AsyncMock()
        with patch("pipeline.scheduler.delete_expired_demo_keys", delete), patch(
            "pipeline.scheduler._record_run", record
        ):
            await demo_key_cleanup_job()
        delete.assert_awaited_once()
        record.assert_awaited_once_with("demo_key_cleanup")

    async def test_job_survives_db_error(self):
        from pipeline.scheduler import demo_key_cleanup_job

        record = AsyncMock()
        with patch(
            "pipeline.scheduler.delete_expired_demo_keys",
            AsyncMock(side_effect=Exception("db down")),
        ), patch("pipeline.scheduler._record_run", record):
            await demo_key_cleanup_job()   # must not raise
        record.assert_awaited_once_with("demo_key_cleanup")


# ---------------------------------------------------------------------------
# Helpers & wiring
# ---------------------------------------------------------------------------

class TestClientIp:
    def _request(self, headers, client_host="10.0.0.1"):
        return SimpleNamespace(
            headers=headers,
            client=SimpleNamespace(host=client_host) if client_host else None,
        )

    def test_first_hop_of_xff(self):
        req = self._request({"x-forwarded-for": "203.0.113.7, 10.0.0.2"})
        assert _client_ip(req) == "203.0.113.7"

    def test_falls_back_to_peer_when_no_xff(self):
        assert _client_ip(self._request({})) == "10.0.0.1"

    def test_empty_xff_falls_back(self):
        req = self._request({"x-forwarded-for": ""})
        assert _client_ip(req) == "10.0.0.1"

    def test_no_client_at_all(self):
        assert _client_ip(self._request({}, client_host=None)) == "unknown"


class TestWiring:
    def _cors_kwargs(self):
        for m in app.user_middleware:
            if m.cls is CORSMiddleware:
                return m.kwargs
        pytest.fail("CORSMiddleware not registered")

    def test_cors_allows_post(self):
        assert "POST" in self._cors_kwargs()["allow_methods"]

    def test_cors_origins_single_sourced_with_mint_gate(self):
        assert self._cors_kwargs()["allow_origins"] == list(SITE_ORIGINS)

    def test_default_origins_match_historical_list(self):
        # Guard: unset SITE_ORIGINS env must degrade to the pre-existing
        # production allowlist, not brick CORS.
        assert "https://sentientmarkets.vercel.app" in SITE_ORIGINS
        assert "http://localhost:3000" in SITE_ORIGINS
