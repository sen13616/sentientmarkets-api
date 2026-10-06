"""
tests/test_short_volume_normalizer.py

Unit tests for short_volume_ratio_otc normalization via RollingZScorer
(Sprint 4: migrated from legacy _normalize_short_volume_z).

Validates:
- _ZSCORE_CONFIG params match G-K2/G-K3 (window=90, min_obs=30, σ_floor=1e-4)
- Scoring direction (negate=True: high short volume → bearish → score < 50)
- Clamping, gating, zero-variance behavior delegated to RollingZScorer
- Driver label, description, explanation template, source weight
- _MARKET_SIGNAL_TYPES registry
"""

from __future__ import annotations

import pytest

from pipeline.features.normalize import _ZSCORE_CONFIG, RollingZScorer

# ---------------------------------------------------------------------------
# Helpers — build synthetic history with known mean and std
# ---------------------------------------------------------------------------


def _make_history(mean: float, std: float, n: int = 50) -> list[float]:
    """
    Build a synthetic history of `n` values with known mean and population std.

    Uses half values at (mean - std) and half at (mean + std).
    Population std of this arrangement = std exactly.
    """
    half = n // 2
    low = mean - std
    high = mean + std
    return [low] * half + [high] * (n - half)


def _scorer() -> RollingZScorer:
    """Return the RollingZScorer instance registered for short_volume_ratio_otc."""
    return _ZSCORE_CONFIG["short_volume_ratio_otc"]


# ---------------------------------------------------------------------------
# Config params match G-K2 / G-K3
# ---------------------------------------------------------------------------


class TestZScoreConfigParams:
    def test_is_rolling_z_scorer(self):
        assert isinstance(_scorer(), RollingZScorer)

    def test_window_is_90(self):
        assert _scorer().window == 90

    def test_min_obs_is_30(self):
        """G-K2: min_obs 10 → 30."""
        assert _scorer().min_obs == 30

    def test_fill_threshold_is_05(self):
        assert _scorer().fill_threshold == 0.5

    def test_effective_min_obs_is_45(self):
        """fill_threshold=0.5 + window=90 → effective_min = max(30, 45) = 45."""
        assert _scorer().effective_min_obs == 45

    def test_sigma_floor_is_1e4(self):
        """G-K3: sigma_floor 1e-12 → 1e-4."""
        assert _scorer().sigma_floor == 1e-4

    def test_negate_is_true(self):
        """High short volume = bearish → negate."""
        assert _scorer().negate is True


# ---------------------------------------------------------------------------
# (a) Today is +2 std above mean → score < 50 (bearish)
# ---------------------------------------------------------------------------


class TestZScorePlusTwoSigma:
    def test_output_below_50(self):
        """z = +2, negate → score ≈ 16.67."""
        history = _make_history(mean=0.50, std=0.10, n=50)
        today = 0.50 + 2 * 0.10  # = 0.70
        result = _scorer().score_from_history(history, today)
        assert result is not None
        # negate: score = 50 + 50*(-2/3) ≈ 16.67
        expected = 50.0 + 50.0 * (-2.0 / 3.0)
        assert abs(result - expected) < 0.01

    def test_output_is_bearish(self):
        """High short volume (positive z, negated) → score < 50."""
        history = _make_history(mean=0.45, std=0.05, n=50)
        today = 0.45 + 2 * 0.05
        result = _scorer().score_from_history(history, today)
        assert result is not None
        assert result < 50.0


# ---------------------------------------------------------------------------
# (b) Today is -2 std below mean → score > 50 (bullish)
# ---------------------------------------------------------------------------


class TestZScoreMinusTwoSigma:
    def test_output_above_50(self):
        """z = -2, negate → score ≈ 83.33."""
        history = _make_history(mean=0.50, std=0.10, n=50)
        today = 0.50 - 2 * 0.10  # = 0.30
        result = _scorer().score_from_history(history, today)
        assert result is not None
        expected = 50.0 + 50.0 * (2.0 / 3.0)
        assert abs(result - expected) < 0.01

    def test_output_is_bullish(self):
        """Low short volume (negative z, negated) → score > 50."""
        history = _make_history(mean=0.45, std=0.05, n=50)
        today = 0.45 - 2 * 0.05
        result = _scorer().score_from_history(history, today)
        assert result is not None
        assert result > 50.0


# ---------------------------------------------------------------------------
# (c) Today is at mean (z = 0) → score = 50 (neutral)
# ---------------------------------------------------------------------------


class TestZScoreAtMean:
    def test_output_is_50(self):
        history = _make_history(mean=0.50, std=0.10, n=50)
        result = _scorer().score_from_history(history, 0.50)
        assert result is not None
        assert abs(result - 50.0) < 0.01

    def test_exact_50(self):
        history = _make_history(mean=0.42, std=0.08, n=50)
        result = _scorer().score_from_history(history, 0.42)
        assert result is not None
        assert abs(result - 50.0) < 1e-6


# ---------------------------------------------------------------------------
# (d) Insufficient history → returns None (effective_min_obs=45)
# ---------------------------------------------------------------------------


class TestInsufficientHistory:
    def test_44_points_returns_none(self):
        """window=90, fill_threshold=0.5 → effective_min=45. So 44 is insufficient."""
        history = _make_history(mean=0.50, std=0.10, n=44)
        assert _scorer().score_from_history(history, 0.50) is None

    def test_29_points_returns_none(self):
        history = _make_history(mean=0.50, std=0.10, n=29)
        assert _scorer().score_from_history(history, 0.50) is None

    def test_10_points_returns_none(self):
        """Legacy min_obs was 10 — now effectively 45, so 10 is insufficient."""
        history = _make_history(mean=0.50, std=0.10, n=10)
        assert _scorer().score_from_history(history, 0.50) is None

    def test_0_points_returns_none(self):
        assert _scorer().score_from_history([], 0.50) is None

    def test_45_points_returns_value(self):
        """45 is the effective minimum — should produce a result."""
        history = _make_history(mean=0.50, std=0.10, n=45)
        result = _scorer().score_from_history(history, 0.50)
        assert result is not None


# ---------------------------------------------------------------------------
# (e) std = 0 (constant history) → returns None
# ---------------------------------------------------------------------------


class TestZeroVariance:
    def test_constant_history_returns_none(self):
        history = [0.45] * 50
        assert _scorer().score_from_history(history, 0.45) is None

    def test_constant_history_different_today_returns_none(self):
        history = [0.50] * 50
        assert _scorer().score_from_history(history, 0.60) is None


# ---------------------------------------------------------------------------
# Clipping at ±3 sigma → output clamped to [0, 100]
# ---------------------------------------------------------------------------


class TestClipping:
    def test_extreme_positive_z_clamped_to_0(self):
        """z > 3, negate → clamped to -3 → score = 0."""
        history = _make_history(mean=0.50, std=0.01, n=50)
        today = 0.50 + 5 * 0.01  # z = 5 → clamped
        result = _scorer().score_from_history(history, today)
        assert result is not None
        assert abs(result - 0.0) < 0.01

    def test_extreme_negative_z_clamped_to_100(self):
        """z < -3, negate → clamped to +3 → score = 100."""
        history = _make_history(mean=0.50, std=0.01, n=50)
        today = 0.50 - 5 * 0.01  # z = -5 → clamped
        result = _scorer().score_from_history(history, today)
        assert result is not None
        assert abs(result - 100.0) < 0.01

    def test_output_always_in_range(self):
        """Output is always in [0, 100]."""
        history = _make_history(mean=0.40, std=0.05, n=50)
        for today in [0.0, 0.20, 0.40, 0.60, 0.80, 1.0]:
            result = _scorer().score_from_history(history, today)
            assert result is not None
            assert 0.0 <= result <= 100.0, f"Out of range for today={today}: {result}"


# ---------------------------------------------------------------------------
# _MARKET_SIGNAL_TYPES includes new types
# ---------------------------------------------------------------------------


class TestMarketSignalTypesRegistry:
    def test_short_volume_types_registered(self):
        from pipeline.orchestrator import _MARKET_SIGNAL_TYPES

        assert "short_volume_otc" in _MARKET_SIGNAL_TYPES
        assert "short_volume_total_otc" in _MARKET_SIGNAL_TYPES
        assert "short_volume_ratio_otc" in _MARKET_SIGNAL_TYPES

    def test_existing_types_still_present(self):
        from pipeline.orchestrator import _MARKET_SIGNAL_TYPES

        for expected in [
            "rsi_14",
            "return_1d",
            "return_5d",
            "return_20d",
            "volume_ratio",
            "order_flow_imbalance",
            "buy_pressure",
            "sell_pressure",
            "bid_ask_spread_bps",
        ]:
            assert expected in _MARKET_SIGNAL_TYPES, f"Missing: {expected}"


# ---------------------------------------------------------------------------
# Score conversion: RollingZScorer [0, 100] consistency
# ---------------------------------------------------------------------------


class TestScoreConversion:
    """Verify that scores align with pipeline convention (50 = neutral)."""

    def test_neutral_maps_to_50(self):
        """z = 0 → score = 50"""
        history = _make_history(mean=0.50, std=0.10, n=50)
        result = _scorer().score_from_history(history, 0.50)
        assert result is not None
        assert abs(result - 50.0) < 0.5

    def test_bearish_maps_below_50(self):
        """High short volume (z = +2, negated) → score < 50"""
        history = _make_history(mean=0.50, std=0.10, n=50)
        result = _scorer().score_from_history(history, 0.70)
        assert result is not None
        assert result < 50.0

    def test_bullish_maps_above_50(self):
        """Low short volume (z = -2, negated) → score > 50"""
        history = _make_history(mean=0.50, std=0.10, n=50)
        result = _scorer().score_from_history(history, 0.30)
        assert result is not None
        assert result > 50.0


# ---------------------------------------------------------------------------
# Driver label and description
# ---------------------------------------------------------------------------


class TestDriverMetadata:
    def test_signal_label_exists(self):
        from pipeline.scoring.drivers import _SIGNAL_LABELS

        assert "short_volume_ratio_otc" in _SIGNAL_LABELS
        assert _SIGNAL_LABELS["short_volume_ratio_otc"] == "Short volume ratio"

    def test_description_handler(self):
        from pipeline.scoring.drivers import _describe

        desc = _describe("short_volume_ratio_otc", 0.60, ticker="AAPL")
        assert "60.0%" in desc
        assert "elevated" in desc
        assert "AAPL" in desc

    def test_description_normal_level(self):
        from pipeline.scoring.drivers import _describe

        desc = _describe("short_volume_ratio_otc", 0.45, ticker="MSFT")
        assert "normal" in desc

    def test_description_low_level(self):
        from pipeline.scoring.drivers import _describe

        desc = _describe("short_volume_ratio_otc", 0.30, ticker="GOOG")
        assert "low" in desc


# ---------------------------------------------------------------------------
# Explanation template
# ---------------------------------------------------------------------------


class TestExplanationTemplate:
    def test_phrase_exists(self):
        from pipeline.explanation.templates import _PHRASES

        assert "Short volume ratio" in _PHRASES
        phrases = _PHRASES["Short volume ratio"]
        assert "bullish" in phrases
        assert "bearish" in phrases
        assert "neutral" in phrases

    def test_source_weight_registered(self):
        from pipeline.features.normalize import _SOURCE_WEIGHTS

        assert "finra_regsho" in _SOURCE_WEIGHTS
        assert _SOURCE_WEIGHTS["finra_regsho"] == 0.9


# ---------------------------------------------------------------------------
# Regression: legacy _normalize_short_volume_z removed
# ---------------------------------------------------------------------------


class TestLegacyRemoved:
    def test_legacy_function_not_importable(self):
        """_normalize_short_volume_z was replaced by RollingZScorer."""
        with pytest.raises(ImportError):
            from pipeline.features.normalize import _normalize_short_volume_z  # noqa: F401

    def test_legacy_score_function_not_importable(self):
        """score_short_volume_ratio was replaced by score_market_signals."""
        with pytest.raises(ImportError):
            from pipeline.features.normalize import score_short_volume_ratio  # noqa: F401
