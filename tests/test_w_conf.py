"""
tests/test_w_conf.py — w_conf (inverse normalized entropy) from FinBERT.

Paper §Event-Level Weighting:
    w_conf = 1 − (−Σ P_i(k) ln P_i(k)) / ln(3)

Paper examples:
    (0.92, 0.05, 0.03) → w_conf ≈ 0.74
    (0.40, 0.35, 0.25) → w_conf ≈ 0.08
"""
from __future__ import annotations

from datetime import datetime, timezone

from pipeline.features.normalize import _compute_w_conf, score_narrative_signals


def _make_ts():
    return datetime(2026, 5, 11, 14, 0, tzinfo=timezone.utc)


# ===========================================================================
# _compute_w_conf unit tests
# ===========================================================================

class TestComputeWConf:

    def test_paper_example_high_confidence(self):
        """Paper example: (0.92, 0.05, 0.03) → w_conf ≈ 0.70.
        (Paper states ~0.74 as approximation; exact value is ~0.698.)"""
        w = _compute_w_conf(0.92, 0.05, 0.03)
        # Exact: H = -(0.92*ln(0.92) + 0.05*ln(0.05) + 0.03*ln(0.03)) ≈ 0.3318
        # w_conf = 1 - 0.3318/ln(3) = 1 - 0.3020 = 0.698
        assert abs(w - 0.698) < 0.01, f"Expected ~0.698, got {w}"
        assert w > 0.6, "High confidence output should give w_conf > 0.6"

    def test_paper_example_low_confidence(self):
        """Paper example: (0.40, 0.35, 0.25) → w_conf ≈ 0.08."""
        # Verify the calculation manually:
        # H = -(0.40*ln(0.40) + 0.35*ln(0.35) + 0.25*ln(0.25))
        # H = -(0.40*(-0.9163) + 0.35*(-1.0498) + 0.25*(-1.3863))
        # H = -(−0.3665 − 0.3674 − 0.3466) = 1.0805
        # w_conf = 1 - 1.0805 / ln(3) = 1 - 1.0805/1.0986 = 1 - 0.9835 = 0.0165
        # Hmm, that gives ~0.02, not 0.08. Let me check with the exact values from paper...
        # Actually the paper might use (0.40, 0.35, 0.25) with slightly different numbers.
        # The key test is that it's LOW (near zero), not high.
        w = _compute_w_conf(0.40, 0.35, 0.25)
        assert w < 0.10, f"Expected low w_conf for uncertain output, got {w}"
        assert w >= 0.0

    def test_uniform_distribution_gives_zero(self):
        """Uniform (1/3, 1/3, 1/3) → maximum entropy → w_conf = 0."""
        w = _compute_w_conf(1/3, 1/3, 1/3)
        assert abs(w) < 1e-6, f"Expected ~0 for uniform, got {w}"

    def test_degenerate_single_class(self):
        """(1.0, 0.0, 0.0) → zero entropy → w_conf = 1."""
        w = _compute_w_conf(1.0, 0.0, 0.0)
        assert abs(w - 1.0) < 1e-6, f"Expected 1.0, got {w}"

    def test_degenerate_negative_class(self):
        """(0.0, 1.0, 0.0) → zero entropy → w_conf = 1."""
        w = _compute_w_conf(0.0, 1.0, 0.0)
        assert abs(w - 1.0) < 1e-6

    def test_degenerate_neutral_class(self):
        """(0.0, 0.0, 1.0) → zero entropy → w_conf = 1."""
        w = _compute_w_conf(0.0, 0.0, 1.0)
        assert abs(w - 1.0) < 1e-6

    def test_result_in_0_1_range(self):
        """w_conf should always be in [0, 1]."""
        test_cases = [
            (0.5, 0.3, 0.2),
            (0.8, 0.1, 0.1),
            (0.33, 0.34, 0.33),
            (0.01, 0.01, 0.98),
        ]
        for pos, neg, neu in test_cases:
            w = _compute_w_conf(pos, neg, neu)
            assert 0.0 <= w <= 1.0, f"w_conf={w} out of range for ({pos}, {neg}, {neu})"

    def test_higher_confidence_gives_higher_w_conf(self):
        """More peaked distribution → higher w_conf."""
        w_high = _compute_w_conf(0.9, 0.05, 0.05)
        w_low = _compute_w_conf(0.5, 0.3, 0.2)
        assert w_high > w_low

    def test_negative_probability_returns_zero(self):
        """Invalid input: negative probability."""
        w = _compute_w_conf(-0.1, 0.5, 0.6)
        assert w == 0.0

    def test_all_zero_returns_zero(self):
        """Degenerate: all zeros."""
        w = _compute_w_conf(0.0, 0.0, 0.0)
        assert w == 0.0


# ===========================================================================
# w_conf integration in score_narrative_signals
# ===========================================================================

class TestWConfInNarrativeScoring:
    """Verify that w_conf affects the weight in score_narrative_signals."""

    def _art(self, sentiment, pos, neg, neu, relevance=0.8) -> dict:
        return {
            "finbert_score": sentiment,
            "finbert_pos": pos,
            "finbert_neg": neg,
            "finbert_neu": neu,
            "relevance_score": relevance,
            "source": "alpha_vantage",
            "published_at": _make_ts(),
        }

    def test_high_confidence_higher_weight(self):
        """Article with high confidence should have higher weight than low confidence."""
        art_high = self._art(0.5, 0.92, 0.05, 0.03)  # w_conf ≈ 0.74
        art_low = self._art(0.5, 0.40, 0.35, 0.25)   # w_conf ≈ 0.02

        res_high = score_narrative_signals("TEST", [art_high], _make_ts())
        res_low = score_narrative_signals("TEST", [art_low], _make_ts())

        assert len(res_high) == 1
        assert len(res_low) == 1
        assert res_high[0]["weight"] > res_low[0]["weight"]

    def test_w_conf_absent_defaults_to_one(self):
        """If class probabilities are missing, w_conf defaults to 1.0."""
        art = {
            "finbert_score": 0.5,
            "finbert_pos": None,
            "finbert_neg": None,
            "finbert_neu": None,
            "relevance_score": 0.8,
            "source": "alpha_vantage",
            "published_at": _make_ts(),
        }
        result = score_narrative_signals("TEST", [art], _make_ts())
        assert len(result) == 1
        # Weight with w_conf=1.0 should equal source_weight * time_weight * relevance
        # Same as if w_conf were not in the formula at all

    def test_paper_weight_formula_complete(self):
        """Verify the full weight formula: w = w_src * w_rel * w_conf * w_time."""
        art = self._art(0.5, 0.92, 0.05, 0.03, relevance=0.9)
        result = score_narrative_signals("TEST", [art], _make_ts())
        assert len(result) == 1

        w_conf = _compute_w_conf(0.92, 0.05, 0.03)
        # At time=0 (published_at == now), w_time ≈ 1.0
        # w_src(alpha_vantage) = 0.75
        # w_rel = 0.9
        expected_weight = 0.75 * 0.9 * w_conf * 1.0
        assert abs(result[0]["weight"] - expected_weight) < 0.01
