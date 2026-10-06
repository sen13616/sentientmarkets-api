"""
tests/test_new_scorers.py

Unit tests for the 4 new market scorer functions added in normalize.py.
Pure functions — no DB, no async.
"""
from __future__ import annotations

from pipeline.features.normalize import (
    _score_bid_ask_spread_bps,
    _score_buy_pressure,
    _score_order_flow_imbalance,
    _score_sell_pressure,
)

# ---------------------------------------------------------------------------
# order_flow_imbalance  (CLV in [-1, 1] → score [0, 100])
# ---------------------------------------------------------------------------

class TestScoreOrderFlowImbalance:

    def test_positive_clv_is_bullish(self):
        assert _score_order_flow_imbalance(0.5) == 75.0

    def test_negative_clv_is_bearish(self):
        assert _score_order_flow_imbalance(-0.5) == 25.0

    def test_zero_clv_is_neutral(self):
        assert _score_order_flow_imbalance(0.0) == 50.0

    def test_max_clv(self):
        assert _score_order_flow_imbalance(1.0) == 100.0

    def test_min_clv(self):
        assert _score_order_flow_imbalance(-1.0) == 0.0

    def test_clamped_above(self):
        # CLV > 1 should clamp to 100
        assert _score_order_flow_imbalance(1.5) == 100.0

    def test_clamped_below(self):
        # CLV < -1 should clamp to 0
        assert _score_order_flow_imbalance(-1.5) == 0.0


# ---------------------------------------------------------------------------
# buy_pressure  (x in [0, 1] → score [0, 100])
# ---------------------------------------------------------------------------

class TestScoreBuyPressure:

    def test_neutral(self):
        assert _score_buy_pressure(0.5) == 50.0

    def test_max_buy(self):
        assert _score_buy_pressure(1.0) == 100.0

    def test_min_buy(self):
        assert _score_buy_pressure(0.0) == 0.0

    def test_high_buy_is_bullish(self):
        assert _score_buy_pressure(0.8) == 80.0

    def test_low_buy_is_bearish(self):
        assert _score_buy_pressure(0.2) == 20.0

    def test_clamped_above(self):
        assert _score_buy_pressure(1.5) == 100.0

    def test_clamped_below(self):
        assert _score_buy_pressure(-0.5) == 0.0


# ---------------------------------------------------------------------------
# sell_pressure  (x in [0, 1] → score [0, 100], inverted)
# ---------------------------------------------------------------------------

class TestScoreSellPressure:

    def test_neutral(self):
        assert _score_sell_pressure(0.5) == 50.0

    def test_max_sell_is_bearish(self):
        # x=1.0 → 100*(1-1)=0 → bearish
        assert _score_sell_pressure(1.0) == 0.0

    def test_min_sell_is_bullish(self):
        # x=0.0 → 100*(1-0)=100 → bullish
        assert _score_sell_pressure(0.0) == 100.0

    def test_high_sell_is_bearish(self):
        assert abs(_score_sell_pressure(0.8) - 20.0) < 1e-9

    def test_low_sell_is_bullish(self):
        assert _score_sell_pressure(0.2) == 80.0

    def test_symmetry_with_buy_pressure(self):
        # buy_pressure(x) + sell_pressure(x) should equal 100 for valid inputs
        for x in [0.0, 0.25, 0.5, 0.75, 1.0]:
            bp = _score_buy_pressure(x)
            sp = _score_sell_pressure(x)
            assert abs(bp + sp - 100.0) < 1e-9, f"x={x}: bp={bp}, sp={sp}"


# ---------------------------------------------------------------------------
# bid_ask_spread_bps  (bps → score [0, 100], tanh-based)
# ---------------------------------------------------------------------------

class TestScoreBidAskSpreadBps:

    def test_neutral_at_10bps(self):
        score = _score_bid_ask_spread_bps(10.0)
        assert score == 50.0

    def test_tight_spread_is_bullish(self):
        # 0 bps → score > 50
        score = _score_bid_ask_spread_bps(0.0)
        assert score > 50.0

    def test_wide_spread_is_bearish(self):
        # 100 bps → score < 50
        score = _score_bid_ask_spread_bps(100.0)
        assert score < 50.0

    def test_very_tight_approaches_max(self):
        # Very negative (bps well below 10) → approaches 100
        score = _score_bid_ask_spread_bps(-100.0)
        assert score > 95.0

    def test_very_wide_approaches_min(self):
        # Very large bps → approaches 0
        score = _score_bid_ask_spread_bps(500.0)
        assert score < 5.0

    def test_direction_monotonic(self):
        # Score should decrease as spread widens
        scores = [_score_bid_ask_spread_bps(bps) for bps in [1, 5, 10, 20, 50, 100]]
        for i in range(len(scores) - 1):
            assert scores[i] > scores[i + 1], f"Not monotonic at bps index {i}"

    def test_output_clamped(self):
        assert 0.0 <= _score_bid_ask_spread_bps(10000.0) <= 100.0
        assert 0.0 <= _score_bid_ask_spread_bps(-10000.0) <= 100.0

    def test_sp500_typical_range(self):
        # Typical S&P 500 large-cap: 1-5 bps → should be bullish (> 50)
        score = _score_bid_ask_spread_bps(3.0)
        assert score > 55.0
        # Mid-cap: 15-25 bps → should be mildly bearish (< 50)
        score = _score_bid_ask_spread_bps(20.0)
        assert score < 50.0
