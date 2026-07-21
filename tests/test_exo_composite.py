"""Unit tests for the exogenous sentiment-only composite (nowcasting Phase 3).

compute_exo_composite = composite over narrative/influencer/macro with the
price-derived market layer excluded; None (never a fabricated neutral 50)
when all three exo layers are missing.
"""

from __future__ import annotations

import pytest

from pipeline.scoring.composite import (
    EXO_LAYERS,
    LAYER_WEIGHTS,
    compute_composite,
    compute_exo_composite,
)


def _subs(market=None, narrative=None, influencer=None, macro=None):
    return {
        "market": market,
        "narrative": narrative,
        "influencer": influencer,
        "macro": macro,
    }


class TestComputeExoComposite:
    def test_all_three_present_renormalized_weights(self):
        result = compute_exo_composite(_subs(narrative=60.0, influencer=70.0, macro=40.0))
        expected = (0.30 * 60 + 0.25 * 70 + 0.10 * 40) / 0.65
        assert result.score == pytest.approx(expected, abs=1e-4)
        assert result.weights_used["narrative"] == pytest.approx(0.30 / 0.65)
        assert result.weights_used["influencer"] == pytest.approx(0.25 / 0.65)
        assert result.weights_used["macro"] == pytest.approx(0.10 / 0.65)

    def test_market_present_but_ignored(self):
        with_market = compute_exo_composite(
            _subs(market=99.0, narrative=60.0, influencer=70.0, macro=40.0)
        )
        without_market = compute_exo_composite(
            _subs(narrative=60.0, influencer=70.0, macro=40.0)
        )
        assert with_market.score == without_market.score
        assert "market" not in with_market.weights_used

    @pytest.mark.parametrize("missing", ["narrative", "influencer", "macro"])
    def test_one_exo_layer_missing_renormalizes(self, missing):
        values = {"narrative": 60.0, "influencer": 70.0, "macro": 40.0}
        values[missing] = None
        result = compute_exo_composite(_subs(**values))
        present = {k: v for k, v in values.items() if v is not None}
        total_w = sum(LAYER_WEIGHTS[k] for k in present)
        expected = sum(LAYER_WEIGHTS[k] * v for k, v in present.items()) / total_w
        assert result.score == pytest.approx(expected, abs=1e-4)

    def test_single_exo_layer_returns_its_value(self):
        result = compute_exo_composite(_subs(narrative=63.5))
        assert result.score == pytest.approx(63.5)

    def test_all_exo_missing_returns_none_even_with_market(self):
        assert compute_exo_composite(_subs(market=80.0)) is None

    def test_all_missing_returns_none(self):
        assert compute_exo_composite(_subs()) is None

    def test_accepts_subindex_like_objects(self):
        class Sub:
            def __init__(self, value):
                self.value = value

        result = compute_exo_composite(_subs(narrative=Sub(60.0), influencer=Sub(70.0)))
        expected = (0.30 * 60 + 0.25 * 70) / 0.55
        assert result.score == pytest.approx(expected, abs=1e-4)

    def test_consistency_with_full_composite_when_market_absent(self):
        """With no market layer, exo and full composite must agree exactly."""
        subs = _subs(narrative=58.0, influencer=61.0, macro=47.0)
        assert compute_exo_composite(subs).score == compute_composite(subs).score

    def test_exo_layers_constant_matches_weights(self):
        assert set(EXO_LAYERS) == set(LAYER_WEIGHTS) - {"market"}
