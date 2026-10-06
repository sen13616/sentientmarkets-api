"""
tests/test_zscore_normalizer.py — Unit tests for the RollingZScorer class.

Sprint 4 (G-C1): Validates z-score computation, clamping, negation,
minimum observation gating, and sigma floor behavior.

All tests are pure and synchronous — the async `score()` method that
calls the DB is tested in integration tests only.
"""
from __future__ import annotations

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


# ===========================================================================
# Basic z-score computation
# ===========================================================================

class TestBasicZScore:

    def test_at_mean_returns_50(self):
        """Value at the mean → z=0 → score=50."""
        scorer = RollingZScorer(window=100)
        history = _make_history(mean=0.50, std=0.10, n=50)
        result = scorer.score_from_history(history, 0.50)
        assert result is not None
        assert abs(result - 50.0) < 0.01

    def test_one_sigma_above_bullish(self):
        """Value 1σ above mean → z=1 → score ≈ 66.7 (bullish)."""
        scorer = RollingZScorer(window=100)
        history = _make_history(mean=0.50, std=0.10, n=50)
        result = scorer.score_from_history(history, 0.60)
        assert result is not None
        expected = 50.0 + 50.0 * (1.0 / 3.0)  # ≈ 66.67
        assert abs(result - expected) < 0.01

    def test_one_sigma_below_bearish(self):
        """Value 1σ below mean → z=-1 → score ≈ 33.3 (bearish)."""
        scorer = RollingZScorer(window=100)
        history = _make_history(mean=0.50, std=0.10, n=50)
        result = scorer.score_from_history(history, 0.40)
        assert result is not None
        expected = 50.0 + 50.0 * (-1.0 / 3.0)  # ≈ 33.33
        assert abs(result - expected) < 0.01

    def test_two_sigma_above(self):
        """Value 2σ above → z=2 → score ≈ 83.3."""
        scorer = RollingZScorer(window=100)
        history = _make_history(mean=100.0, std=10.0, n=50)
        result = scorer.score_from_history(history, 120.0)
        assert result is not None
        expected = 50.0 + 50.0 * (2.0 / 3.0)  # ≈ 83.33
        assert abs(result - expected) < 0.01


# ===========================================================================
# Negation (bearish-when-high signals)
# ===========================================================================

class TestNegation:

    def test_negate_flips_direction(self):
        """With negate=True, value above mean → score < 50 (bearish)."""
        scorer = RollingZScorer(window=100, negate=True)
        history = _make_history(mean=50.0, std=10.0, n=50)
        result = scorer.score_from_history(history, 60.0)  # 1σ above
        assert result is not None
        assert result < 50.0

    def test_negate_below_mean_bullish(self):
        """With negate=True, value below mean → score > 50 (bullish)."""
        scorer = RollingZScorer(window=100, negate=True)
        history = _make_history(mean=50.0, std=10.0, n=50)
        result = scorer.score_from_history(history, 40.0)  # 1σ below
        assert result is not None
        assert result > 50.0

    def test_negate_at_mean_neutral(self):
        """With negate=True, value at mean → still 50."""
        scorer = RollingZScorer(window=100, negate=True)
        history = _make_history(mean=50.0, std=10.0, n=50)
        result = scorer.score_from_history(history, 50.0)
        assert result is not None
        assert abs(result - 50.0) < 0.01


# ===========================================================================
# Clamping at ±3σ
# ===========================================================================

class TestClamping:

    def test_extreme_positive_clamped_to_100(self):
        """z > 3 → clamped to 3 → score = 100."""
        scorer = RollingZScorer(window=100)
        history = _make_history(mean=50.0, std=1.0, n=50)
        result = scorer.score_from_history(history, 60.0)  # z=10
        assert result is not None
        assert abs(result - 100.0) < 0.01

    def test_extreme_negative_clamped_to_0(self):
        """z < -3 → clamped to -3 → score = 0."""
        scorer = RollingZScorer(window=100)
        history = _make_history(mean=50.0, std=1.0, n=50)
        result = scorer.score_from_history(history, 40.0)  # z=-10
        assert result is not None
        assert abs(result - 0.0) < 0.01

    def test_exactly_three_sigma(self):
        """z = exactly 3 → score = 100."""
        scorer = RollingZScorer(window=100)
        history = _make_history(mean=50.0, std=10.0, n=50)
        result = scorer.score_from_history(history, 80.0)  # z=3
        assert result is not None
        assert abs(result - 100.0) < 0.01

    def test_output_always_in_range(self):
        """Score is always in [0, 100] for any input."""
        scorer = RollingZScorer(window=100)
        history = _make_history(mean=50.0, std=5.0, n=50)
        for val in [0.0, 10.0, 30.0, 50.0, 70.0, 90.0, 100.0]:
            result = scorer.score_from_history(history, val)
            assert result is not None
            assert 0.0 <= result <= 100.0, f"Out of range for val={val}: {result}"

    def test_negated_clamping(self):
        """With negate=True, z=+5 (clamped +3, then negated) → score = 0."""
        scorer = RollingZScorer(window=100, negate=True)
        history = _make_history(mean=50.0, std=1.0, n=50)
        result = scorer.score_from_history(history, 60.0)  # z=10, negate→-3
        assert result is not None
        assert abs(result - 0.0) < 0.01


# ===========================================================================
# Minimum observation gate
# ===========================================================================

class TestMinObsGate:
    """Tests for the min_obs gate in isolation (fill_threshold=0 to disable fill gate)."""

    def test_below_min_obs_returns_none(self):
        """Fewer than min_obs → None."""
        scorer = RollingZScorer(window=100, min_obs=30, fill_threshold=0.0)
        history = _make_history(mean=50.0, std=10.0, n=29)
        assert scorer.score_from_history(history, 50.0) is None

    def test_exactly_min_obs_returns_score(self):
        """Exactly min_obs → produces a score."""
        scorer = RollingZScorer(window=100, min_obs=30, fill_threshold=0.0)
        history = _make_history(mean=50.0, std=10.0, n=30)
        result = scorer.score_from_history(history, 50.0)
        assert result is not None

    def test_empty_history_returns_none(self):
        scorer = RollingZScorer(window=100, fill_threshold=0.0)
        assert scorer.score_from_history([], 50.0) is None

    def test_custom_min_obs(self):
        """Custom min_obs=10 allows 10 observations."""
        scorer = RollingZScorer(window=100, min_obs=10, fill_threshold=0.0)
        history = _make_history(mean=50.0, std=10.0, n=10)
        result = scorer.score_from_history(history, 50.0)
        assert result is not None

    def test_custom_min_obs_below_threshold(self):
        scorer = RollingZScorer(window=100, min_obs=10, fill_threshold=0.0)
        history = _make_history(mean=50.0, std=10.0, n=9)
        assert scorer.score_from_history(history, 50.0) is None


# ===========================================================================
# Window fill threshold gate
# ===========================================================================

class TestFillThresholdGate:

    def test_effective_min_obs_default(self):
        """Default fill_threshold=0.5: effective_min = max(30, ceil(500*0.5)) = 250."""
        scorer = RollingZScorer(window=500)
        assert scorer.effective_min_obs == 250

    def test_effective_min_obs_daily(self):
        """Daily window=90, fill=0.5: effective_min = max(30, ceil(90*0.5)) = 45."""
        scorer = RollingZScorer(window=90)
        assert scorer.effective_min_obs == 45

    def test_effective_min_obs_when_min_obs_dominates(self):
        """If min_obs is higher than ceil(window*fill), min_obs wins."""
        scorer = RollingZScorer(window=50, min_obs=30)  # ceil(50*0.5)=25, min_obs=30
        assert scorer.effective_min_obs == 30

    def test_exactly_50pct_fill_passes(self):
        """Exactly at the fill threshold boundary → produces a score."""
        scorer = RollingZScorer(window=100)
        # effective_min = max(30, ceil(100*0.5)) = 50
        history = _make_history(mean=50.0, std=10.0, n=50)
        result = scorer.score_from_history(history, 50.0)
        assert result is not None

    def test_below_50pct_fill_but_above_min_obs_returns_none(self):
        """49 obs with window=100: above min_obs=30, below fill gate (50). Must fall back."""
        scorer = RollingZScorer(window=100)
        history = _make_history(mean=50.0, std=10.0, n=49)
        assert scorer.score_from_history(history, 50.0) is None

    def test_above_50pct_fill_passes(self):
        """75 obs with window=100: above fill gate (50). Passes."""
        scorer = RollingZScorer(window=100)
        history = _make_history(mean=50.0, std=10.0, n=75)
        result = scorer.score_from_history(history, 50.0)
        assert result is not None

    def test_intraday_500_window_requires_250(self):
        """Intra-day window=500: 207 obs (the current return_1d count) → None."""
        scorer = RollingZScorer(window=500)
        history = _make_history(mean=50.0, std=10.0, n=207)
        assert scorer.score_from_history(history, 50.0) is None

    def test_intraday_500_window_250_passes(self):
        """Intra-day window=500: exactly 250 obs → produces score."""
        scorer = RollingZScorer(window=500)
        history = _make_history(mean=50.0, std=10.0, n=250)
        result = scorer.score_from_history(history, 50.0)
        assert result is not None

    def test_daily_90_window_44_returns_none(self):
        """Daily window=90: 44 obs → below ceil(90*0.5)=45 → None."""
        scorer = RollingZScorer(window=90)
        history = _make_history(mean=50.0, std=10.0, n=44)
        assert scorer.score_from_history(history, 50.0) is None

    def test_daily_90_window_45_passes(self):
        """Daily window=90: exactly 45 obs → passes."""
        scorer = RollingZScorer(window=90)
        history = _make_history(mean=50.0, std=10.0, n=45)
        result = scorer.score_from_history(history, 50.0)
        assert result is not None

    def test_fill_threshold_zero_disables_gate(self):
        """fill_threshold=0.0 → effective_min = min_obs only."""
        scorer = RollingZScorer(window=500, fill_threshold=0.0)
        assert scorer.effective_min_obs == 30
        history = _make_history(mean=50.0, std=10.0, n=30)
        result = scorer.score_from_history(history, 50.0)
        assert result is not None

    def test_fill_threshold_one_requires_full_window(self):
        """fill_threshold=1.0 → requires full window."""
        scorer = RollingZScorer(window=100, fill_threshold=1.0)
        assert scorer.effective_min_obs == 100
        history = _make_history(mean=50.0, std=10.0, n=99)
        assert scorer.score_from_history(history, 50.0) is None
        history_full = _make_history(mean=50.0, std=10.0, n=100)
        assert scorer.score_from_history(history_full, 50.0) is not None


# ===========================================================================
# Sigma floor
# ===========================================================================

class TestSigmaFloor:

    def test_zero_variance_returns_none(self):
        """Constant history (σ=0) → None."""
        scorer = RollingZScorer(window=100)
        history = [42.0] * 50
        assert scorer.score_from_history(history, 42.0) is None

    def test_near_zero_variance_returns_none(self):
        """Variance below sigma_floor → None."""
        scorer = RollingZScorer(window=100, sigma_floor=1e-4)
        # std = 1e-6, below floor
        history = _make_history(mean=50.0, std=1e-6, n=50)
        assert scorer.score_from_history(history, 50.0) is None

    def test_variance_above_floor_returns_score(self):
        """Variance above sigma_floor → valid score."""
        scorer = RollingZScorer(window=100, sigma_floor=1e-4)
        history = _make_history(mean=50.0, std=0.01, n=50)  # std=0.01 > 1e-4
        result = scorer.score_from_history(history, 50.0)
        assert result is not None

    def test_custom_sigma_floor(self):
        """Custom sigma_floor=1.0 rejects std < 1.0."""
        scorer = RollingZScorer(window=100, sigma_floor=1.0)
        history = _make_history(mean=50.0, std=0.5, n=50)
        assert scorer.score_from_history(history, 50.0) is None


# ===========================================================================
# _ZSCORE_CONFIG registry
# ===========================================================================

class TestZScoreConfig:

    def test_intraday_signals_have_window_500(self):
        for sig in ("rsi_14", "return_1d", "return_5d", "return_20d",
                     "volume_ratio", "order_flow_imbalance", "bid_ask_spread_bps"):
            cfg = _ZSCORE_CONFIG.get(sig)
            assert cfg is not None, f"Missing z-score config for {sig}"
            assert cfg.window == 500, f"Wrong window for {sig}: {cfg.window}"
            assert cfg.min_obs == 30
            assert cfg.sigma_floor == 1e-4
            assert cfg.fill_threshold == 0.5
            assert cfg.effective_min_obs == 250, f"Wrong effective_min for {sig}"

    def test_daily_signals_have_window_90(self):
        for sig in ("short_volume_ratio_otc", "insider_net_shares",
                     "analyst_buy_pct", "vix"):
            cfg = _ZSCORE_CONFIG.get(sig)
            assert cfg is not None, f"Missing z-score config for {sig}"
            assert cfg.window == 90, f"Wrong window for {sig}: {cfg.window}"
            assert cfg.effective_min_obs == 45, f"Wrong effective_min for {sig}"

    def test_bearish_signals_are_negated(self):
        for sig in ("rsi_14", "bid_ask_spread_bps",
                     "short_volume_ratio_otc", "vix"):
            assert _ZSCORE_CONFIG[sig].negate is True, f"{sig} should be negated"

    def test_bullish_signals_are_not_negated(self):
        for sig in ("return_1d", "return_5d", "return_20d",
                     "volume_ratio", "order_flow_imbalance",
                     "insider_net_shares", "analyst_buy_pct"):
            assert _ZSCORE_CONFIG[sig].negate is False, f"{sig} should not be negated"

    def test_no_config_for_excluded_signals(self):
        """Signals excluded from z-score should NOT be in _ZSCORE_CONFIG.

        Sprint P3.2: analyst_target_price now HAS a z-score config (against upside).
        Sprint P4.2: sector_etf_return_20d now HAS a z-score config (per-ETF rolling).
        """
        for sig in ("buy_pressure", "sell_pressure"):
            assert sig not in _ZSCORE_CONFIG, f"{sig} should not have z-score config"

    def test_analyst_target_price_has_zscore_config(self):
        """Sprint P3.2 I9: analyst_target_price uses 90-day rolling z-score over upside."""
        assert "analyst_target_price" in _ZSCORE_CONFIG
        cfg = _ZSCORE_CONFIG["analyst_target_price"]
        assert cfg.window == 90
        assert cfg.fill_threshold == 0.5
        assert cfg.negate is False
