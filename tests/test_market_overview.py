"""
tests/test_market_overview.py

Unit tests for the universe-stats additions:
  • pipeline/scoring/market_overview.py pure functions
    (compute_percentiles, build_overview)
  • score_change_1d / score_change_1d_pct on the sentiment response
  • universe_percentile on the pro sentiment response
  • GET /v1/market/overview (pro-only route)

All external I/O (DB, Redis) is mocked — no live connections required.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from api.auth import authenticate
from main import app
from pipeline.scoring.market_overview import build_overview, compute_percentiles

_NOW = datetime(2026, 7, 15, 14, 30, tzinfo=timezone.utc)

# ---------------------------------------------------------------------------
# Shared mock state — mirrors test_api.py's _MOCK_STATE plus the new fields
# ---------------------------------------------------------------------------

_MOCK_STATE: dict = {
    "ticker":              "AAPL",
    "composite_score":     72.0,
    "score_change_1d":     3.25,
    "score_change_1d_pct": 4.73,
    "confidence":          {"score": 81, "flags": []},
    "timestamp":           "2026-07-15T14:30:00+00:00",
    "sub_indices": {
        "market":     {"value": 78.0},
        "narrative":  {"value": 69.0},
        "influencer": {"value": 80.0},
        "macro":      {"value": 61.0},
    },
    "divergence":  "aligned",
    "top_drivers": [],
    "explanation": "",
    "freshness": {
        "market_as_of":     "2026-07-15T14:30:00+00:00",
        "narrative_as_of":  "2026-07-15T14:00:00+00:00",
        "influencer_as_of": "2026-07-15T08:00:00+00:00",
        "macro_as_of":      "2026-07-15T02:00:00+00:00",
    },
}

_MOCK_OVERVIEW: dict = build_overview(
    scores={"AAPL": 72.0, "MSFT": 55.0, "XOM": 41.0, "CVX": 47.5},
    changes={"AAPL": 3.25, "MSFT": -1.5, "XOM": None, "CVX": 0.5},
    change_pcts={"AAPL": 4.73, "MSFT": -2.65, "XOM": None, "CVX": 1.06},
    sector_map={"AAPL": "Information Technology", "MSFT": "Information Technology",
                "XOM": "Energy", "CVX": "Energy"},
    timestamp=_NOW,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

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


def _overview_redis(payload: str | None):
    """Patch the market route's Redis client so GET returns `payload`."""
    client = MagicMock()
    client.get = AsyncMock(return_value=payload)
    return patch("api.routes.market.get_redis", return_value=client)


# ===========================================================================
# compute_percentiles
# ===========================================================================

class TestComputePercentiles:

    def test_empty_universe(self):
        assert compute_percentiles({}) == {}

    def test_single_ticker_is_50(self):
        assert compute_percentiles({"AAPL": 72.0}) == {"AAPL": 50.0}

    def test_bounds_and_ordering(self):
        pct = compute_percentiles({"LOW": 10.0, "MID": 50.0, "HIGH": 90.0})
        assert pct["LOW"] == 0.0
        assert pct["MID"] == 50.0
        assert pct["HIGH"] == 100.0

    def test_ties_share_percentile(self):
        pct = compute_percentiles({"A": 40.0, "B": 40.0, "C": 60.0})
        assert pct["A"] == pct["B"]
        assert pct["C"] == 100.0


# ===========================================================================
# build_overview
# ===========================================================================

class TestBuildOverview:

    def test_empty_universe(self):
        blob = build_overview({}, {}, {}, {}, _NOW)
        assert blob["universe_scored"] == 0
        assert blob["average_score"] is None
        assert blob["top_movers"] == []
        assert blob["sectors"] == []

    def test_average_and_breadth(self):
        assert _MOCK_OVERVIEW["universe_scored"] == 4
        assert _MOCK_OVERVIEW["average_score"] == round((72.0 + 55.0 + 41.0 + 47.5) / 4, 2)
        # 2 of 4 above 50
        assert _MOCK_OVERVIEW["breadth_above_50_pct"] == 50.0
        # improving = AAPL, CVX of the 3 with non-null change
        assert _MOCK_OVERVIEW["breadth_improving_pct"] == round(100 * 2 / 3, 1)

    def test_movers_exclude_null_change_and_order(self):
        top = [m["ticker"] for m in _MOCK_OVERVIEW["top_movers"]]
        bottom = [m["ticker"] for m in _MOCK_OVERVIEW["bottom_movers"]]
        assert "XOM" not in top and "XOM" not in bottom  # null change → excluded
        assert top[0] == "AAPL"        # largest gain first
        assert bottom[0] == "MSFT"     # largest loss first

    def test_gap_safety_all_changes_null(self):
        """First tick after an outage: no baselines → no movers, null breadth."""
        blob = build_overview(
            scores={"A": 60.0, "B": 40.0},
            changes={"A": None, "B": None},
            change_pcts={"A": None, "B": None},
            sector_map={},
            timestamp=_NOW,
        )
        assert blob["breadth_improving_pct"] is None
        assert blob["top_movers"] == [] and blob["bottom_movers"] == []
        assert blob["average_score"] == 50.0  # level stats still present

    def test_sector_ranks_and_size(self):
        tech = next(s for s in _MOCK_OVERVIEW["sectors"] if s["sector"] == "Information Technology")
        assert tech["size"] == 2
        assert tech["average_score"] == round((72.0 + 55.0) / 2, 2)
        assert tech["tickers"][0] == {"ticker": "AAPL", "score": 72.0, "rank": 1}
        assert tech["tickers"][1] == {"ticker": "MSFT", "score": 55.0, "rank": 2}

    def test_unsectored_ticker_in_universe_but_not_sectors(self):
        blob = build_overview(
            scores={"A": 60.0, "B": 40.0},
            changes={"A": 1.0, "B": -1.0},
            change_pcts={"A": 1.7, "B": -2.4},
            sector_map={"A": "Energy"},   # B has no sector
            timestamp=_NOW,
        )
        assert blob["universe_scored"] == 2
        all_sector_tickers = [t["ticker"] for s in blob["sectors"] for t in s["tickers"]]
        assert all_sector_tickers == ["A"]


# ===========================================================================
# Sentiment response — new fields
# ===========================================================================

class TestSentimentChangeFields:

    def _patches(self, state=_MOCK_STATE):
        return (
            patch("api.rate_limit.check_rate_limit", AsyncMock()),
            patch("api.routes.sentiment.is_supported_ticker", AsyncMock(return_value=True)),
            patch("api.response.assembler._load_from_redis", AsyncMock(return_value=state)),
        )

    def test_free_tier_includes_change_fields(self, free_client):
        p1, p2, p3 = self._patches()
        with p1, p2, p3:
            r = free_client.get("/v1/sentiment/AAPL", headers={"Authorization": "Bearer k"})
        assert r.status_code == 200
        body = r.json()
        assert body["score_change_1d"] == 3.25
        assert body["score_change_1d_pct"] == 4.73

    def test_pro_tier_includes_change_and_percentile(self, pro_client):
        p1, p2, p3 = self._patches()
        with p1, p2, p3, patch(
            "api.response.assembler._load_percentile", AsyncMock(return_value=87.3)
        ):
            r = pro_client.get(
                "/v1/sentiment/AAPL?detail=full", headers={"Authorization": "Bearer k"}
            )
        assert r.status_code == 200
        body = r.json()
        assert body["score_change_1d"] == 3.25
        assert body["score_change_1d_pct"] == 4.73
        assert body["universe_percentile"] == 87.3

    def test_pre_feature_state_yields_nulls(self, pro_client):
        """States written before this feature (or across a gap) have no
        change keys — fields must be null, never interpolated."""
        old_state = {k: v for k, v in _MOCK_STATE.items()
                     if k not in ("score_change_1d", "score_change_1d_pct")}
        p1, p2, p3 = self._patches(state=old_state)
        with p1, p2, p3, patch(
            "api.response.assembler._load_percentile", AsyncMock(return_value=None)
        ):
            r = pro_client.get(
                "/v1/sentiment/AAPL?detail=full", headers={"Authorization": "Bearer k"}
            )
        assert r.status_code == 200
        body = r.json()
        assert body["score_change_1d"] is None
        assert body["score_change_1d_pct"] is None
        assert body["universe_percentile"] is None


# ===========================================================================
# GET /v1/market/overview
# ===========================================================================

class TestMarketOverviewRoute:

    def test_free_tier_forbidden(self, free_client):
        with patch("api.rate_limit.check_rate_limit", AsyncMock()):
            r = free_client.get("/v1/market/overview", headers={"Authorization": "Bearer k"})
        assert r.status_code == 403
        assert r.json()["detail"]["error"] == "forbidden"

    def test_pro_tier_gets_blob(self, pro_client):
        with (
            patch("api.rate_limit.check_rate_limit", AsyncMock()),
            _overview_redis(json.dumps(_MOCK_OVERVIEW)),
        ):
            r = pro_client.get("/v1/market/overview", headers={"Authorization": "Bearer k"})
        assert r.status_code == 200
        body = r.json()
        assert body["universe_scored"] == 4
        assert body["timestamp"].startswith("2026-07-15T14:30")
        assert body["top_movers"][0]["ticker"] == "AAPL"
        tech = next(s for s in body["sectors"] if s["sector"] == "Information Technology")
        assert tech["size"] == 2
        assert tech["tickers"][0]["rank"] == 1

    def test_missing_blob_returns_503(self, pro_client):
        with (
            patch("api.rate_limit.check_rate_limit", AsyncMock()),
            _overview_redis(None),
        ):
            r = pro_client.get("/v1/market/overview", headers={"Authorization": "Bearer k"})
        assert r.status_code == 503
        assert r.json()["detail"]["error"] == "temporarily_unavailable"

    def test_unauthenticated_401(self):
        client = TestClient(app)
        r = client.get("/v1/market/overview")
        assert r.status_code in (401, 403)
