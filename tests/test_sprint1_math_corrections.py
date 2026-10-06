"""
tests/test_sprint1_math_corrections.py

Tests for Sprint 1 (G-S4, G-S6, G-S13, G-S14) and Sprint 2 (G-S3) changes.

Sprint 4 note: score_market_signals, score_influencer_signals, and
score_macro_signals are now async (z-score requires DB reads).  Tests mock
get_signal_history to return [] so z-score falls back to parametric.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

from pipeline.features.normalize import (
    _SOURCE_WEIGHTS,
    score_influencer_signals,
    score_macro_signals,
    score_market_signals,
    score_narrative_signals,
)
from pipeline.sources.market import _compute_returns

# ---------------------------------------------------------------------------
# G-S4: Log returns
# ---------------------------------------------------------------------------

class TestLogReturns:
    """_compute_returns should use log returns: math.log(current/prev)."""

    def test_basic_log_return(self):
        """5% simple return → log return = ln(1.05) ≈ 0.04879."""
        history = [
            (datetime(2026, 5, 1, 21, 0, tzinfo=timezone.utc), 100.0),
        ]
        results = dict(_compute_returns(105.0, history))
        expected = math.log(105.0 / 100.0)
        assert abs(results["return_1d"] - expected) < 1e-6

    def test_negative_log_return(self):
        """Price decline → negative log return."""
        history = [
            (datetime(2026, 5, 1, 21, 0, tzinfo=timezone.utc), 100.0),
        ]
        results = dict(_compute_returns(95.0, history))
        expected = math.log(95.0 / 100.0)
        assert abs(results["return_1d"] - expected) < 1e-6
        assert results["return_1d"] < 0

    def test_log_return_symmetry(self):
        """Log returns are additive: +5% then -5% should not equal zero.
        log(1.05) + log(0.95) ≈ -0.0013, not 0."""
        r_up = math.log(105.0 / 100.0)
        r_down = math.log(95.0 / 100.0)
        assert abs(r_up + r_down) > 0.001  # Not zero — that's the point of log returns

    def test_zero_prev_close_skipped(self):
        """If previous close is 0, skip (can't take log)."""
        history = [(datetime(2026, 5, 1, 21, 0, tzinfo=timezone.utc), 0.0)]
        results = _compute_returns(105.0, history)
        assert len(results) == 0

    def test_zero_current_close_skipped(self):
        """If current close is 0, skip (can't take log)."""
        history = [(datetime(2026, 5, 1, 21, 0, tzinfo=timezone.utc), 100.0)]
        results = _compute_returns(0.0, history)
        assert len(results) == 0

    def test_negative_close_skipped(self):
        """Negative prices are invalid — skip."""
        history = [(datetime(2026, 5, 1, 21, 0, tzinfo=timezone.utc), -10.0)]
        results = _compute_returns(100.0, history)
        assert len(results) == 0

    def test_5d_20d_log_returns(self):
        """5d and 20d horizons also use log returns."""
        history = [
            (datetime(2026, 4, i + 1, 21, 0, tzinfo=timezone.utc), 80.0 + i)
            for i in range(25)
        ]
        current = 110.0
        results = dict(_compute_returns(current, history))

        closes = [80.0 + i for i in range(25)]
        assert abs(results["return_1d"] - math.log(current / closes[-1])) < 1e-6
        assert abs(results["return_5d"] - math.log(current / closes[-5])) < 1e-6
        assert abs(results["return_20d"] - math.log(current / closes[-20])) < 1e-6

    def test_unchanged_price_returns_zero(self):
        """Same price → log(1) = 0."""
        history = [(datetime(2026, 5, 1, 21, 0, tzinfo=timezone.utc), 100.0)]
        results = dict(_compute_returns(100.0, history))
        assert results["return_1d"] == 0.0


# ---------------------------------------------------------------------------
# G-S6: NaN/Inf validation
# ---------------------------------------------------------------------------

def _make_ts():
    return datetime(2026, 5, 1, 14, 0, tzinfo=timezone.utc)


# Mock get_signal_history to return [] so z-score falls back to parametric
_mock_history = patch("scripts.db.queries.raw_signals.get_signal_history",
                      new_callable=AsyncMock, return_value=[])


class TestNanInfGuardsMarket:
    """score_market_signals should skip NaN/Inf values."""

    def _row(self, sig_type: str, value: float) -> dict:
        return {"signal_type": sig_type, "value": value, "source": "computed", "timestamp": _make_ts()}

    async def test_nan_value_skipped(self):
        raw = [self._row("rsi_14", float("nan"))]
        with _mock_history:
            result = await score_market_signals("TEST", raw, _make_ts())
        assert len(result) == 0

    async def test_inf_value_skipped(self):
        raw = [self._row("rsi_14", float("inf"))]
        with _mock_history:
            result = await score_market_signals("TEST", raw, _make_ts())
        assert len(result) == 0

    async def test_neg_inf_value_skipped(self):
        raw = [self._row("rsi_14", float("-inf"))]
        with _mock_history:
            result = await score_market_signals("TEST", raw, _make_ts())
        assert len(result) == 0

    async def test_valid_value_passes(self):
        raw = [self._row("rsi_14", 50.0)]
        with _mock_history:
            result = await score_market_signals("TEST", raw, _make_ts())
        assert len(result) == 1

    async def test_mixed_nan_and_valid(self):
        raw = [
            self._row("rsi_14", float("nan")),
            self._row("volume_ratio", 1.2),
        ]
        with _mock_history:
            result = await score_market_signals("TEST", raw, _make_ts())
        assert len(result) == 1
        assert result[0]["signal_type"] == "volume_ratio"


class TestNanInfGuardsNarrative:
    """score_narrative_signals should skip NaN/Inf sentiment values."""

    def _art(self, sentiment, relevance=0.7) -> dict:
        return {
            "finbert_score": sentiment,
            "finbert_pos": max(0, sentiment) if sentiment is not None and not (isinstance(sentiment, float) and (math.isnan(sentiment) or math.isinf(sentiment))) else 0.5,
            "finbert_neg": max(0, -sentiment) if sentiment is not None and not (isinstance(sentiment, float) and (math.isnan(sentiment) or math.isinf(sentiment))) else 0.3,
            "finbert_neu": 0.2,
            "relevance_score": relevance,
            "source": "alpha_vantage",
            "published_at": _make_ts(),
        }

    def test_nan_sentiment_skipped(self):
        result = score_narrative_signals("TEST", [self._art(float("nan"))], _make_ts())
        assert len(result) == 0

    def test_inf_sentiment_skipped(self):
        result = score_narrative_signals("TEST", [self._art(float("inf"))], _make_ts())
        assert len(result) == 0

    def test_valid_sentiment_passes(self):
        result = score_narrative_signals("TEST", [self._art(0.5)], _make_ts())
        assert len(result) == 1


class TestNanInfGuardsInfluencer:
    """score_influencer_signals should skip NaN/Inf values."""

    def _row(self, sig_type: str, value: float) -> dict:
        return {"signal_type": sig_type, "value": value, "source": "finnhub", "timestamp": _make_ts()}

    async def test_nan_value_skipped(self):
        raw = [self._row("insider_net_shares", float("nan"))]
        with _mock_history:
            result = await score_influencer_signals("TEST", raw, _make_ts())
        assert len(result) == 0

    async def test_inf_value_skipped(self):
        raw = [self._row("analyst_buy_pct", float("inf"))]
        with _mock_history:
            result = await score_influencer_signals("TEST", raw, _make_ts())
        assert len(result) == 0

    async def test_valid_value_passes(self):
        raw = [self._row("insider_net_shares", 1000.0)]
        with _mock_history:
            result = await score_influencer_signals("TEST", raw, _make_ts())
        assert len(result) == 1


class TestNanInfGuardsMacro:
    """score_macro_signals should skip NaN/Inf values."""

    def _row(self, sig_type: str, value: float) -> dict:
        return {"signal_type": sig_type, "value": value, "source": "alpha_vantage", "timestamp": _make_ts()}

    async def test_nan_vix_skipped(self):
        raw = [self._row("vix", float("nan"))]
        with _mock_history:
            # Sprint P4.2: score_macro_signals now takes (ticker, sector, raw, now).
            # VIX is shared/global, so passing "_MACRO_" preserves the original intent.
            result = await score_macro_signals("_MACRO_", None, raw, _make_ts())
        assert len(result) == 0

    async def test_inf_vix_skipped(self):
        raw = [self._row("vix", float("inf"))]
        with _mock_history:
            result = await score_macro_signals("_MACRO_", None, raw, _make_ts())
        assert len(result) == 0

    async def test_valid_vix_passes(self):
        raw = [self._row("vix", 22.0)]
        with _mock_history:
            result = await score_macro_signals("_MACRO_", None, raw, _make_ts())
        assert len(result) == 1


# ---------------------------------------------------------------------------
# G-S13 / Sprint A: Source weight reconciliation
# ---------------------------------------------------------------------------

class TestSourceWeights:

    def test_alpha_vantage_weight_is_075(self):
        """Paper §Event-Level Weighting: AV = 0.75 (Sprint A)."""
        assert _SOURCE_WEIGHTS["alpha_vantage"] == 0.75

    def test_finnhub_weight_is_065(self):
        """Paper §Event-Level Weighting: Finnhub = 0.65 (Sprint A)."""
        assert _SOURCE_WEIGHTS["finnhub"] == 0.65

    def test_av_weight_lower_than_exchange_data(self):
        """AV is a third-party aggregator — weight should be lower than exchange data."""
        assert _SOURCE_WEIGHTS["alpha_vantage"] < _SOURCE_WEIGHTS["polygon"]

    def test_finnhub_lower_than_av(self):
        """Per paper: Finnhub < AV in source hierarchy."""
        assert _SOURCE_WEIGHTS["finnhub"] < _SOURCE_WEIGHTS["alpha_vantage"]

    def test_sec_edgar_absent(self):
        """Phase 5 retraction (2026-05-16): Sprint C (EDGAR 8-K narrative) never
        merged to main; the only ingester that wrote source='sec_edgar' rows
        (Form 4) was removed in P3.4. The label is no longer a valid source —
        a future re-implementation should land via a fresh weight entry."""
        assert "sec_edgar" not in _SOURCE_WEIGHTS


# ---------------------------------------------------------------------------
# G-S14 / Sprint D / Sprint A: Relevance threshold 0.6 + finbert_score
# ---------------------------------------------------------------------------

class TestRelevanceThreshold:
    """Paper Stage 2: articles with relevance_score < 0.6 excluded.
    Articles without explicit relevance_score excluded (not defaulted).
    Sprint A: uses finbert_score instead of provider_sentiment."""

    def _art(self, sentiment: float, relevance) -> dict:
        return {
            "finbert_score": sentiment,
            "finbert_pos": 0.7,
            "finbert_neg": 0.1,
            "finbert_neu": 0.2,
            "relevance_score": relevance,
            "source": "alpha_vantage",
            "published_at": _make_ts(),
        }

    def test_above_threshold_included(self):
        """relevance_score=0.65 → included (above 0.6)."""
        result = score_narrative_signals("TEST", [self._art(0.5, 0.65)], _make_ts())
        assert len(result) == 1

    def test_below_threshold_excluded(self):
        """relevance_score=0.55 → excluded (below 0.6)."""
        result = score_narrative_signals("TEST", [self._art(0.5, 0.55)], _make_ts())
        assert len(result) == 0

    def test_none_relevance_excluded(self):
        """relevance_score=None → excluded (not defaulted to 0.5).
        Paper Stage 2: articles without explicit relevance should not contribute."""
        art = {
            "finbert_score": 0.5,
            "finbert_pos": 0.7,
            "finbert_neg": 0.1,
            "finbert_neu": 0.2,
            "relevance_score": None,
            "source": "alpha_vantage",
            "published_at": _make_ts(),
        }
        result = score_narrative_signals("TEST", [art], _make_ts())
        assert len(result) == 0

    def test_at_threshold_included(self):
        """relevance_score=0.6 → included (boundary: >= 0.6)."""
        result = score_narrative_signals("TEST", [self._art(0.5, 0.6)], _make_ts())
        assert len(result) == 1

    def test_just_below_threshold_excluded(self):
        """relevance_score=0.59 → excluded."""
        result = score_narrative_signals("TEST", [self._art(0.5, 0.59)], _make_ts())
        assert len(result) == 0

    def test_high_relevance_included(self):
        """relevance_score=1.0 → included."""
        result = score_narrative_signals("TEST", [self._art(0.5, 1.0)], _make_ts())
        assert len(result) == 1

    def test_zero_relevance_excluded(self):
        """relevance_score=0.0 → excluded (below 0.6)."""
        result = score_narrative_signals("TEST", [self._art(0.5, 0.0)], _make_ts())
        assert len(result) == 0

    def test_relevance_used_directly_in_weight(self):
        """Weight uses relevance directly — higher relevance = higher weight."""
        art_low = self._art(0.5, 0.65)
        art_high = self._art(0.5, 0.95)
        result_low = score_narrative_signals("TEST", [art_low], _make_ts())
        result_high = score_narrative_signals("TEST", [art_high], _make_ts())
        assert len(result_low) == 1
        assert len(result_high) == 1
        assert result_high[0]["weight"] > result_low[0]["weight"]

    def test_mixed_articles_filter(self):
        """Mix of above and below threshold — only above-threshold articles scored."""
        articles = [
            self._art(0.8, 0.05),   # excluded (well below 0.6)
            self._art(0.5, 0.7),    # included (above 0.6)
            self._art(-0.2, 0.55),  # excluded (below 0.6)
            self._art(0.1, 0.8),    # included (above 0.6)
        ]
        result = score_narrative_signals("TEST", articles, _make_ts())
        assert len(result) == 2
