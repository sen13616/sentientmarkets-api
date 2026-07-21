"""
tests/test_api.py

Unit tests for the API layer (Block 9).
All external I/O (DB, Redis) is mocked — no live connections required.

TestClient runs routes synchronously without triggering the FastAPI lifespan,
so no DB pool or Redis client is initialised during these tests.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from api.auth import authenticate
from main import app

# ---------------------------------------------------------------------------
# Shared mock state — simulates a scored AAPL entry returned from Redis
# ---------------------------------------------------------------------------

_MOCK_STATE: dict = {
    "ticker":          "AAPL",
    "composite_score": 72.0,
    "confidence":      {"score": 81, "flags": []},
    "timestamp":       "2026-04-23T14:32:00+00:00",
    "sub_indices": {
        "market":     {"value": 78.0},
        "narrative":  {"value": 69.0},
        "influencer": {"value": 80.0},
        "macro":      {"value": 61.0},
    },
    "divergence":  "aligned",
    "top_drivers": [
        {
            "signal":       "Insider transaction",
            "description":  "Insider purchased 5,000 shares for AAPL",
            "direction":    "bullish",
            "magnitude":    0.8,
            "source_layer": "influencer",
            "confidence":   0.9,
        }
    ],
    "explanation": "Sentiment is primarily driven by strong insider conviction.",
    "freshness": {
        "market_as_of":     "2026-04-23T14:30:00+00:00",
        "narrative_as_of":  "2026-04-23T14:00:00+00:00",
        "influencer_as_of": "2026-04-23T08:00:00+00:00",
        "macro_as_of":      "2026-04-23T02:00:00+00:00",
    },
}

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def plain_client():
    """TestClient with NO dependency overrides — used for auth tests."""
    return TestClient(app)


@pytest.fixture
def free_client():
    """TestClient with authenticate stubbed to return 'free'."""
    app.dependency_overrides[authenticate] = lambda: "free"
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def pro_client():
    """TestClient with authenticate stubbed to return 'pro'."""
    app.dependency_overrides[authenticate] = lambda: "pro"
    yield TestClient(app)
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Helper — patches common to every sentiment route call
# ---------------------------------------------------------------------------

def _sentiment_patches(*, supported: bool = True, state: dict | None = _MOCK_STATE):
    """Return a combined context manager for the three most common patches."""
    return (
        patch("api.rate_limit.check_rate_limit", AsyncMock()),
        patch("api.routes.sentiment.is_supported_ticker", AsyncMock(return_value=supported)),
        patch("api.response.assembler._load_from_redis", AsyncMock(return_value=state)),
    )


# ===========================================================================
# GET /health
# ===========================================================================

class TestHealth:

    def test_returns_200_and_ok_body(self, plain_client):
        r = plain_client.get("/health")
        assert r.status_code == 200
        assert r.json() == {"status": "ok"}


# ===========================================================================
# GET /v1/sentiment/{ticker} — authentication
# ===========================================================================

class TestSentimentAuth:

    def test_no_auth_header_returns_401(self, plain_client):
        # No Authorization header → HTTPBearer returns None → 401
        r = plain_client.get("/v1/sentiment/AAPL")
        assert r.status_code == 401

    def test_invalid_key_returns_401(self, plain_client):
        # Valid header format but key not in DB → get_key_tier returns None → 401
        with patch("api.auth.get_key_tier", AsyncMock(return_value=None)):
            r = plain_client.get(
                "/v1/sentiment/AAPL",
                headers={"Authorization": "Bearer sk-sm-invalid-key"},
            )
        assert r.status_code == 401


# ===========================================================================
# GET /v1/sentiment/{ticker} — free tier field filtering
# ===========================================================================

class TestSentimentFreeTier:

    def _request(self, client):
        p1, p2, p3 = _sentiment_patches()
        with p1, p2, p3:
            return client.get(
                "/v1/sentiment/AAPL",
                headers={"Authorization": "Bearer sk-sm-test"},
            )

    def test_returns_200(self, free_client):
        assert self._request(free_client).status_code == 200

    def test_required_fields_present(self, free_client):
        data = self._request(free_client).json()
        for field in ("ticker", "score", "label", "confidence", "timestamp", "cache_age_seconds"):
            assert field in data, f"Missing expected field: {field!r}"

    def test_pro_fields_absent(self, free_client):
        data = self._request(free_client).json()
        for field in ("sub_indices", "top_drivers", "explanation", "freshness",
                      "score_raw_z", "score_raw_percentile", "sector_percentile",
                      "score_exo", "score_exo_percentile"):
            assert field not in data, f"Pro-only field leaked to free tier: {field!r}"

    def test_score_label_confidence_values(self, free_client):
        data = self._request(free_client).json()
        assert data["score"] == 72
        assert data["label"] == "Bullish"
        assert data["confidence"] == 81


# ===========================================================================
# GET /v1/sentiment/{ticker}?detail=full — pro tier
# ===========================================================================

class TestSentimentProTier:

    def _request(self, client):
        p1, p2, p3 = _sentiment_patches()
        with p1, p2, p3:
            return client.get(
                "/v1/sentiment/AAPL?detail=full",
                headers={"Authorization": "Bearer sk-sm-test"},
            )

    def test_returns_200(self, pro_client):
        assert self._request(pro_client).status_code == 200

    def test_pro_fields_present(self, pro_client):
        data = self._request(pro_client).json()
        for field in ("sub_indices", "top_drivers", "explanation", "freshness"):
            assert field in data, f"Missing pro field: {field!r}"

    def test_sub_indices_has_all_four_layers(self, pro_client):
        data = self._request(pro_client).json()
        assert set(data["sub_indices"].keys()) >= {"market", "narrative", "influencer", "macro"}


# ===========================================================================
# GET /v1/sentiment/{ticker} — unknown ticker
# ===========================================================================

class TestSentimentUnknownTicker:

    def test_status_field_is_ticker_not_found(self, free_client):
        p1, p2, p3 = _sentiment_patches(supported=False)
        with p1, p2, p3:
            r = free_client.get(
                "/v1/sentiment/FAKEXYZ",
                headers={"Authorization": "Bearer sk-sm-test"},
            )
        data = r.json()
        assert "status" in data
        assert data["status"] == "ticker_not_found"


# ===========================================================================
# GET /v1/tickers
# ===========================================================================

class TestTickers:

    _MOCK_ROWS = [
        {"ticker": "AAPL", "company_name": "Apple Inc.",     "sector": "Information Technology"},
        {"ticker": "MSFT", "company_name": "Microsoft Corp.", "sector": "Information Technology"},
        {"ticker": "NVDA", "company_name": "NVIDIA Corp.",   "sector": "Information Technology"},
    ]

    def test_returns_universe_size_and_tickers_list(self, free_client):
        with (
            patch("api.rate_limit.check_rate_limit", AsyncMock()),
            patch(
                "api.routes.tickers.get_all_tickers",
                AsyncMock(return_value=self._MOCK_ROWS),
            ),
        ):
            r = free_client.get(
                "/v1/tickers",
                headers={"Authorization": "Bearer sk-sm-test"},
            )
        assert r.status_code == 200
        data = r.json()
        assert "universe_size" in data
        assert "tickers" in data
        assert data["universe_size"] == 3
        tickers_list = [t["ticker"] for t in data["tickers"]]
        assert "AAPL" in tickers_list

    def test_tickers_include_name_field(self, free_client):
        with (
            patch("api.rate_limit.check_rate_limit", AsyncMock()),
            patch(
                "api.routes.tickers.get_all_tickers",
                AsyncMock(return_value=self._MOCK_ROWS),
            ),
        ):
            r = free_client.get(
                "/v1/tickers",
                headers={"Authorization": "Bearer sk-sm-test"},
            )
        data = r.json()
        first = data["tickers"][0]
        assert "ticker" in first
        assert "name" in first
        assert first["name"] == "Apple Inc."


# ===========================================================================
# GET /v1/status
# ===========================================================================

class TestStatus:

    def test_requires_authentication(self):
        # /v1/status is auth-gated (any tier); anonymous pollers get 401.
        r = TestClient(app).get("/v1/status")
        assert r.status_code == 401

    def test_returns_operational_status(self, free_client):
        with (
            patch("api.rate_limit.check_rate_limit", AsyncMock()),
            patch("api.routes.status._read_all_ts", AsyncMock(side_effect=lambda keys: {k: None for k in keys})),
        ):
            r = free_client.get("/v1/status", headers={"Authorization": "Bearer k"})
        assert r.status_code == 200
        data = r.json()
        assert "status" in data
        assert data["status"] == "operational"

    def test_last_macro_run_is_most_recent_of_split_macro_jobs(self, free_client):
        # The macro job was split into macro_daily + macro_intraday; last_macro_run
        # must report the most recent of the two (not the retired `macro` key).
        from datetime import datetime
        runs = {
            "pipeline:last_run:macro_daily":    "2026-04-24T02:00:00+00:00",
            "pipeline:last_run:macro_intraday": "2026-04-24T18:00:00+00:00",
        }

        async def _fake_read_all_ts(keys):
            return {k: (datetime.fromisoformat(runs[k]) if k in runs else None) for k in keys}

        with (
            patch("api.rate_limit.check_rate_limit", AsyncMock()),
            patch("api.routes.status._read_all_ts", side_effect=_fake_read_all_ts),
        ):
            r = free_client.get("/v1/status", headers={"Authorization": "Bearer k"})
        assert r.status_code == 200
        # intraday (18:00) is more recent than daily (02:00)
        assert r.json()["last_macro_run"] == "2026-04-24T18:00:00Z"

    def test_last_macro_run_null_when_no_macro_runs_recorded(self, free_client):
        with (
            patch("api.rate_limit.check_rate_limit", AsyncMock()),
            patch("api.routes.status._read_all_ts", AsyncMock(side_effect=lambda keys: {k: None for k in keys})),
        ):
            r = free_client.get("/v1/status", headers={"Authorization": "Bearer k"})
        assert r.json()["last_macro_run"] is None
