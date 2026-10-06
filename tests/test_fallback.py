"""
tests/test_fallback.py

Unit tests for graceful degradation in the scoring pipeline.

All functions under test are pure and synchronous — no DB, Redis, or async
required.  These tests verify that the composite score is still computed
correctly when one or more layers are missing, and that the confidence
scorer applies the right penalties.
"""

from __future__ import annotations

from pipeline.confidence.scorer import PENALTIES, compute_confidence
from pipeline.scoring.composite import LAYER_WEIGHTS, compute_composite
from pipeline.scoring.divergence import compute_divergence

# ---------------------------------------------------------------------------
# Helper — lightweight SubIndexResult stand-in (has a .value attribute)
# ---------------------------------------------------------------------------


def _si(v: float):
    class _R:
        value = v

    return _R()


# ===========================================================================
# Composite — weight redistribution when one or more layers are missing
# ===========================================================================


class TestCompositeFallback:
    def test_market_missing_weights_sum_to_one(self):
        sub = {
            "market": None,
            "narrative": _si(60.0),
            "influencer": _si(65.0),
            "macro": _si(55.0),
        }
        r = compute_composite(sub)
        assert abs(sum(r.weights_used.values()) - 1.0) < 1e-9

    def test_market_missing_score_uses_remaining_three(self):
        """Score must equal the normalised weighted average of the three present layers."""
        sub = {
            "market": None,
            "narrative": _si(60.0),
            "influencer": _si(65.0),
            "macro": _si(55.0),
        }
        r = compute_composite(sub)
        # Remaining raw weights: narrative 0.30, influencer 0.25, macro 0.10 → total 0.65
        expected = (0.30 * 60 + 0.25 * 65 + 0.10 * 55) / 0.65
        assert abs(r.score - expected) < 1e-4

    def test_market_missing_recorded_in_missing_layers(self):
        sub = {"market": None, "narrative": _si(60.0), "influencer": _si(65.0), "macro": _si(55.0)}
        r = compute_composite(sub)
        assert "market" in r.missing_layers

    def test_two_layers_missing_weights_sum_to_one(self):
        sub = {
            "market": None,
            "narrative": None,
            "influencer": _si(70.0),
            "macro": _si(50.0),
        }
        r = compute_composite(sub)
        assert abs(sum(r.weights_used.values()) - 1.0) < 1e-9
        assert len(r.missing_layers) == 2

    def test_two_layers_missing_score_correct(self):
        sub = {
            "market": None,
            "narrative": None,
            "influencer": _si(70.0),
            "macro": _si(50.0),
        }
        r = compute_composite(sub)
        # Remaining: influencer 0.25, macro 0.10 → total 0.35
        expected = (0.25 * 70 + 0.10 * 50) / 0.35
        assert abs(r.score - expected) < 1e-4

    def test_all_layers_missing_returns_neutral_50(self):
        sub = {k: None for k in LAYER_WEIGHTS}
        r = compute_composite(sub)
        assert r.score == 50.0
        assert set(r.missing_layers) == set(LAYER_WEIGHTS.keys())


# ===========================================================================
# Confidence — missing-layer and divergence penalties
# ===========================================================================


class TestConfidenceFallback:
    """
    Isolate specific penalty types by holding all other factors neutral:
      n_signals = 10  (≥ 5, no low_signal_volume penalty)
      stale_sources = []
      divergence_flag = "aligned"  (unless testing divergence)
    """

    def _conf(
        self,
        missing: list[str] | None = None,
        stale: list[str] | None = None,
        n: int = 10,
        div: str = "aligned",
    ):
        return compute_confidence(
            missing_layers=missing or [],
            stale_sources=stale or [],
            n_signals=n,
            divergence_flag=div,
        )

    def test_one_missing_layer_subtracts_15(self):
        r = self._conf(missing=["market"])
        assert r.score == 100 - PENALTIES["missing_layer"]  # 85

    def test_two_missing_layers_subtract_30(self):
        r = self._conf(missing=["market", "narrative"])
        assert r.score == 100 - 2 * PENALTIES["missing_layer"]  # 70

    def test_missing_layer_adds_flag(self):
        r = self._conf(missing=["market", "influencer"])
        assert any("market" in f for f in r.flags)
        assert any("influencer" in f for f in r.flags)

    def test_high_divergence_subtracts_15(self):
        r = self._conf(div="high_divergence")
        assert r.score == 100 - PENALTIES["high_divergence"]  # 85
        assert "high_divergence" in r.flags

    def test_moderate_divergence_no_penalty(self):
        r = self._conf(div="moderate_divergence")
        assert r.score == 100

    def test_confidence_never_below_zero(self):
        # 4 missing layers × 15 = 60
        # 1 stale source   × 10 = 10  → 70
        # low_signal_volume × 20 = 20  → 90
        # high_divergence  × 15 = 15  → 105 total penalties > 100 → clips to 0
        r = self._conf(
            missing=["market", "narrative", "influencer", "macro"],
            stale=["market"],
            div="high_divergence",
            n=1,
        )
        assert r.score == 0


# ===========================================================================
# Divergence — spread threshold logic
# ===========================================================================


class TestDivergenceFallback:
    def test_spread_over_40_is_high_divergence(self):
        # market=80, narrative=30 → spread=50
        result, _ = compute_divergence({"market": 80.0, "narrative": 30.0}, 55.0)
        assert result.flag == "high_divergence"
        assert result.spread > 40

    def test_spread_under_20_is_aligned(self):
        result, _ = compute_divergence({"market": 65.0, "narrative": 60.0}, 62.5)
        assert result.flag == "aligned"

    def test_single_layer_is_always_aligned(self):
        # Fewer than two layers → spread=0, no divergence possible
        result, _ = compute_divergence({"market": 90.0}, 90.0)
        assert result.flag == "aligned"
        assert result.spread == 0.0
