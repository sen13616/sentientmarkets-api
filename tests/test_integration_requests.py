"""
tests/test_integration_requests.py

Unit tests for the six SentientMarkets integration requests:
  1. CORS middleware
  3. /health endpoint with optional tier lookup
  4. missing_layers in ProTierResponse and HistoryEntry
  5. Company names in /v1/tickers (TickerItem shape)

All external I/O is mocked — no live DB or Redis connections needed.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from api.auth import authenticate
from main import app

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def free_client():
    app.dependency_overrides[authenticate] = lambda: "free"
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def pro_client():
    app.dependency_overrides[authenticate] = lambda: "pro"
    yield TestClient(app)
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Shared mock data
# ---------------------------------------------------------------------------

_MOCK_STATE_FULL_LAYERS: dict = {
    "ticker":          "AAPL",
    "composite_score": 65.0,
    "confidence":      {"score": 75, "flags": []},
    "timestamp":       "2026-04-25T14:00:00+00:00",
    "sub_indices": {
        "market":     {"value": 70.0},
        "narrative":  {"value": 60.0},
        "influencer": {"value": 72.0},
        "macro":      {"value": 58.0},
    },
    "divergence":  None,
    "top_drivers": [],
    "explanation": "",
    "freshness": {
        "market_as_of":     "2026-04-25T14:00:00+00:00",
        "narrative_as_of":  "2026-04-25T13:00:00+00:00",
        "influencer_as_of": "2026-04-25T10:00:00+00:00",
        "macro_as_of":      "2026-04-25T02:00:00+00:00",
    },
}

_MOCK_STATE_MISSING_LAYERS: dict = {
    **_MOCK_STATE_FULL_LAYERS,
    "sub_indices": {
        "market":     {"value": 70.0},
        "narrative":  None,              # missing
        "influencer": None,              # missing
        "macro":      {"value": 58.0},
    },
}

_MOCK_TICKER_ROWS = [
    {"ticker": "AAPL", "company_name": "Apple Inc.",     "sector": "Information Technology"},
    {"ticker": "MSFT", "company_name": "Microsoft Corp.", "sector": "Information Technology"},
    {"ticker": "NVDA", "company_name": "NVIDIA Corp.",   "sector": "Information Technology"},
]


# ===========================================================================
# 1. CORS middleware
# ===========================================================================

class TestCORS:

    def test_cors_header_present_for_allowed_origin(self, client):
        r = client.get(
            "/health",
            headers={"Origin": "https://sentientmarkets.vercel.app"},
        )
        assert "access-control-allow-origin" in r.headers

    def test_cors_preflight_options_returns_200(self, client):
        r = client.options(
            "/health",
            headers={
                "Origin":                         "http://localhost:3000",
                "Access-Control-Request-Method":  "GET",
                "Access-Control-Request-Headers": "Authorization",
            },
        )
        # FastAPI CORS middleware responds 200 for valid preflight
        assert r.status_code == 200


# ===========================================================================
# 3. /health endpoint
# ===========================================================================

class TestHealth:

    def test_no_auth_returns_status_ok(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json() == {"status": "ok"}

    def test_invalid_key_still_returns_200(self, client):
        """Health must never raise 401 — bad key → tier: null."""
        with patch("api.routes.health._lookup_tier", AsyncMock(return_value=None)):
            r = client.get(
                "/health",
                headers={"Authorization": "Bearer sk-sm-bad-key"},
            )
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "ok"
        assert data["tier"] is None

    def test_valid_pro_key_returns_tier_pro(self, client):
        with patch("api.routes.health._lookup_tier", AsyncMock(return_value="pro")):
            r = client.get(
                "/health",
                headers={"Authorization": "Bearer sk-sm-prod-validkey"},
            )
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "ok"
        assert data["tier"] == "pro"

    def test_valid_free_key_returns_tier_free(self, client):
        with patch("api.routes.health._lookup_tier", AsyncMock(return_value="free")):
            r = client.get(
                "/health",
                headers={"Authorization": "Bearer sk-sm-dev-validkey"},
            )
        assert r.status_code == 200
        assert r.json()["tier"] == "free"

    def test_db_error_during_health_still_returns_200(self, client):
        """Even if DB is down, /health must not crash."""
        with patch(
            "api.routes.health._lookup_tier",
            AsyncMock(side_effect=Exception("DB unavailable")),
        ):
            r = client.get(
                "/health",
                headers={"Authorization": "Bearer sk-sm-prod-key"},
            )
        assert r.status_code == 200
        assert r.json()["status"] == "ok"


# ===========================================================================
# 4. missing_layers in ProTierResponse
# ===========================================================================

class TestMissingLayers:

    def _pro_request(self, client, state):
        with (
            patch("api.rate_limit.check_rate_limit", AsyncMock()),
            patch("api.routes.sentiment.is_supported_ticker", AsyncMock(return_value=True)),
            patch("api.response.assembler._load_from_redis", AsyncMock(return_value=state)),
        ):
            return client.get(
                "/v1/sentiment/AAPL?detail=full",
                headers={"Authorization": "Bearer sk-sm-test"},
            )

    def test_missing_layers_empty_when_all_present(self, pro_client):
        r = self._pro_request(pro_client, _MOCK_STATE_FULL_LAYERS)
        assert r.status_code == 200
        data = r.json()
        assert "missing_layers" in data
        assert data["missing_layers"] == []

    def test_missing_layers_lists_null_sub_indices(self, pro_client):
        r = self._pro_request(pro_client, _MOCK_STATE_MISSING_LAYERS)
        assert r.status_code == 200
        missing = r.json()["missing_layers"]
        assert set(missing) == {"narrative", "influencer"}

    def test_missing_layers_field_present_in_pro_response(self, pro_client):
        r = self._pro_request(pro_client, _MOCK_STATE_FULL_LAYERS)
        assert "missing_layers" in r.json()


# ===========================================================================
# 5. Company names in /v1/tickers
# ===========================================================================

class TestTickersCompanyNames:

    def test_tickers_returns_list_of_objects(self, free_client):
        with (
            patch("api.rate_limit.check_rate_limit", AsyncMock()),
            patch(
                "api.routes.tickers.get_all_tickers",
                AsyncMock(return_value=_MOCK_TICKER_ROWS),
            ),
        ):
            r = free_client.get(
                "/v1/tickers",
                headers={"Authorization": "Bearer sk-sm-test"},
            )
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data["tickers"], list)
        assert isinstance(data["tickers"][0], dict)

    def test_ticker_item_has_ticker_and_name_fields(self, free_client):
        with (
            patch("api.rate_limit.check_rate_limit", AsyncMock()),
            patch(
                "api.routes.tickers.get_all_tickers",
                AsyncMock(return_value=_MOCK_TICKER_ROWS),
            ),
        ):
            r = free_client.get(
                "/v1/tickers",
                headers={"Authorization": "Bearer sk-sm-test"},
            )
        item = r.json()["tickers"][0]
        assert "ticker" in item
        assert "name" in item
        assert item["ticker"] == "AAPL"
        assert item["name"] == "Apple Inc."

    def test_universe_size_matches_tickers_list_length(self, free_client):
        with (
            patch("api.rate_limit.check_rate_limit", AsyncMock()),
            patch(
                "api.routes.tickers.get_all_tickers",
                AsyncMock(return_value=_MOCK_TICKER_ROWS),
            ),
        ):
            r = free_client.get(
                "/v1/tickers",
                headers={"Authorization": "Bearer sk-sm-test"},
            )
        data = r.json()
        assert data["universe_size"] == len(data["tickers"])
