"""
tests/test_market_subindex.py

Unit tests for the structured 6-component market sub-index aggregation.

All tests are pure and synchronous — no DB, Redis, or async required.

Sprint 2 (G-S3): volume_ratio added as 6th component at weight 0.10.
New weights: returns=0.30, momentum=0.15, order_flow=0.20, liquidity=0.10,
short_volume=0.15, volume=0.10.

Note: many tests use ``_full_signals()`` which builds only the 4 original
components (returns, momentum, order_flow, liquidity) — short_volume and
volume are absent. The missing weight (0.15 + 0.10 = 0.25) is redistributed
across the 4 present components (sum = 0.75), so effective weights differ
from the nominal values.
"""
from __future__ import annotations

from pipeline.scoring.subindices import (
    MARKET_COMPONENT_WEIGHTS,
    compute_market_sub_index,
)

# ---------------------------------------------------------------------------
# Helpers — build synthetic signal dicts
# ---------------------------------------------------------------------------

def _sig(signal_type: str, score: float, value: float = 0.0, weight: float = 0.9) -> dict:
    """Build a minimal signal dict for testing."""
    return {
        "signal_type": signal_type,
        "score": score,
        "value": value,
        "weight": weight,
        "source": "test",
        "layer": "market",
        "ticker": "TEST",
    }


def _full_signals(
    return_score: float = 50.0,
    rsi_raw: float = 50.0,
    ofi_score: float = 50.0,
    spread_score: float = 50.0,
) -> list[dict]:
    """Build signals for the 4 original components (short_volume + volume absent)."""
    return [
        _sig("return_1d",  return_score),
        _sig("return_5d",  return_score),
        _sig("return_20d", return_score),
        _sig("rsi_14",     0.0, value=rsi_raw),   # score unused; raw value matters
        _sig("order_flow_imbalance", ofi_score),
        _sig("bid_ask_spread_bps",   spread_score),
    ]


# ---------------------------------------------------------------------------
# (a) 4 original components present and positive → sub-index near 100
#     (short_volume + volume missing → weight redistributed across 4 present)
# ---------------------------------------------------------------------------

class TestAllPositive:

    def test_strongly_bullish(self):
        sigs = _full_signals(
            return_score=100.0,     # → +1.0
            rsi_raw=70.0,           # → +1.0
            ofi_score=100.0,        # → +1.0
            spread_score=100.0,     # → +1.0 (very tight spread)
        )
        si = compute_market_sub_index(sigs)
        assert si is not None
        # All components at +1 → combined = +1 → value = 100
        assert si.value == 100.0

    def test_moderately_bullish(self):
        sigs = _full_signals(
            return_score=75.0,      # → +0.5
            rsi_raw=60.0,           # → +0.5
            ofi_score=75.0,         # → +0.5
            spread_score=75.0,      # → +0.5
        )
        si = compute_market_sub_index(sigs)
        assert si is not None
        # All components at +0.5 → combined = +0.5 → value = 75
        assert si.value == 75.0


# ---------------------------------------------------------------------------
# (b) All 4 negative → sub-index near 0
# ---------------------------------------------------------------------------

class TestAllNegative:

    def test_strongly_bearish(self):
        sigs = _full_signals(
            return_score=0.0,       # → -1.0
            rsi_raw=30.0,           # → -1.0
            ofi_score=0.0,          # → -1.0
            spread_score=0.0,       # → -1.0 (very wide spread)
        )
        si = compute_market_sub_index(sigs)
        assert si is not None
        assert si.value == 0.0

    def test_moderately_bearish(self):
        sigs = _full_signals(
            return_score=25.0,      # → -0.5
            rsi_raw=40.0,           # → -0.5
            ofi_score=25.0,         # → -0.5
            spread_score=25.0,      # → -0.5
        )
        si = compute_market_sub_index(sigs)
        assert si is not None
        assert si.value == 25.0


# ---------------------------------------------------------------------------
# (c) Order flow and returns disagree → partial cancellation
# ---------------------------------------------------------------------------

class TestDisagreement:

    def test_returns_bullish_order_flow_bearish(self):
        """Returns strongly bullish (+1) but order flow strongly bearish (-1).

        4 components present (short_volume + volume missing), sum=0.75:
        Effective: returns=0.30/0.75=0.40, momentum=0.15/0.75=0.20,
                   order_flow=0.20/0.75=0.2667, liquidity=0.10/0.75=0.1333
        Combined = 0.40*(+1) + 0.20*(0) + 0.2667*(-1) + 0.1333*(0)
                 = (0.40 - 0.2667) = +0.1333
        Value = 50 + 50*0.1333 ≈ 56.67
        """
        sigs = _full_signals(
            return_score=100.0,     # → +1.0
            rsi_raw=50.0,           # → 0.0 (neutral)
            ofi_score=0.0,          # → -1.0
            spread_score=50.0,      # → 0.0 (neutral)
        )
        si = compute_market_sub_index(sigs)
        assert si is not None
        assert abs(si.value - 56.67) < 0.1

    def test_returns_bearish_order_flow_bullish(self):
        """Opposite of above: returns=-1, order_flow=+1.

        Combined = 0.40*(-1) + 0.2667*(+1) = -0.1333
        Value = 50 + 50*(-0.1333) ≈ 43.33
        """
        sigs = _full_signals(
            return_score=0.0,       # → -1.0
            rsi_raw=50.0,           # → 0.0
            ofi_score=100.0,        # → +1.0
            spread_score=50.0,      # → 0.0
        )
        si = compute_market_sub_index(sigs)
        assert si is not None
        assert abs(si.value - 43.33) < 0.1


# ---------------------------------------------------------------------------
# (d) One component missing → weight redistributed
# ---------------------------------------------------------------------------

class TestMissingOneComponent:

    def test_liquidity_missing_weight_redistributed(self):
        """Liquidity missing (no bid_ask_spread_bps signal).

        Present weights: returns=0.30, momentum=0.15, order_flow=0.20 → sum=0.65
        Effective: returns=0.30/0.65, momentum=0.15/0.65, order_flow=0.20/0.65

        All present at +1:
        combined = (0.30+0.15+0.20)/0.65 = 0.65/0.65 = 1.0
        value = 50 + 50*1.0 = 100
        """
        sigs = [
            _sig("return_1d",  100.0),
            _sig("return_5d",  100.0),
            _sig("return_20d", 100.0),
            _sig("rsi_14", 0.0, value=70.0),
            _sig("order_flow_imbalance", 100.0),
            # No bid_ask_spread_bps
        ]
        si = compute_market_sub_index(sigs)
        assert si is not None
        assert si.value == 100.0

    def test_momentum_missing_weight_redistributed(self):
        """Momentum (RSI) missing.

        Present: returns=0.30, order_flow=0.20, liquidity=0.10 → sum=0.60
        Effective: returns=0.50, order_flow=0.3333, liquidity=0.1667

        returns=+1, order_flow=+1, liquidity=0 (neutral):
        combined = 0.50*1 + 0.3333*1 + 0.1667*0 = 0.8333
        value = 50 + 50*0.8333 ≈ 91.67
        """
        sigs = [
            _sig("return_1d",  100.0),
            _sig("return_5d",  100.0),
            _sig("return_20d", 100.0),
            # No rsi_14
            _sig("order_flow_imbalance", 100.0),
            _sig("bid_ask_spread_bps",    50.0),
        ]
        si = compute_market_sub_index(sigs)
        assert si is not None
        assert abs(si.value - 91.67) < 0.01

    def test_returns_missing_weight_redistributed(self):
        """Returns missing. Only momentum, order_flow, liquidity present.

        Present: momentum=0.15, order_flow=0.20, liquidity=0.10 → sum=0.45
        Effective: momentum=0.3333, order_flow=0.4444, liquidity=0.2222

        All at +0.5:
        combined = 0.3333*0.5 + 0.4444*0.5 + 0.2222*0.5 = 0.5*(1.0) = 0.5
        value = 50 + 50*0.5 = 75.0
        """
        sigs = [
            # No return_* signals
            _sig("rsi_14", 0.0, value=60.0),              # → +0.5
            _sig("order_flow_imbalance", 75.0),            # → +0.5
            _sig("bid_ask_spread_bps",   75.0),            # → +0.5
        ]
        si = compute_market_sub_index(sigs)
        assert si is not None
        assert abs(si.value - 75.0) < 0.01

    def test_non_component_signals_ignored_for_subindex(self):
        """buy_pressure and sell_pressure are present but don't affect the sub-index.
        volume_ratio IS now a component signal, so it will affect the result."""
        base = _full_signals(return_score=75.0, rsi_raw=60.0, ofi_score=75.0, spread_score=75.0)
        base_si = compute_market_sub_index(base)

        with_extras = base + [
            _sig("buy_pressure",   95.0),
            _sig("sell_pressure",   5.0),
        ]
        extras_si = compute_market_sub_index(with_extras)

        assert base_si is not None
        assert extras_si is not None
        assert base_si.value == extras_si.value


# ---------------------------------------------------------------------------
# (e) 5 of 6 components missing → sub-index returns None
# ---------------------------------------------------------------------------

class TestFiveMissing:

    def test_only_returns_present(self):
        sigs = [_sig("return_1d", 80.0)]
        assert compute_market_sub_index(sigs) is None

    def test_only_rsi_present(self):
        sigs = [_sig("rsi_14", 0.0, value=65.0)]
        assert compute_market_sub_index(sigs) is None

    def test_only_order_flow_present(self):
        sigs = [_sig("order_flow_imbalance", 80.0)]
        assert compute_market_sub_index(sigs) is None

    def test_only_liquidity_present(self):
        sigs = [_sig("bid_ask_spread_bps", 60.0)]
        assert compute_market_sub_index(sigs) is None

    def test_only_short_volume_present(self):
        sigs = [_sig("short_volume_ratio_otc", 40.0)]
        assert compute_market_sub_index(sigs) is None

    def test_only_volume_ratio_present(self):
        sigs = [_sig("volume_ratio", 60.0)]
        assert compute_market_sub_index(sigs) is None

    def test_no_signals_returns_none(self):
        assert compute_market_sub_index([]) is None

    def test_all_zero_weight_returns_none(self):
        sigs = _full_signals()
        for s in sigs:
            s["weight"] = 0.0
        assert compute_market_sub_index(sigs) is None


# ---------------------------------------------------------------------------
# RSI momentum sign convention
# ---------------------------------------------------------------------------

class TestRSIMomentumConvention:
    """RSI is treated as a MOMENTUM indicator in the sub-index: high RSI = bullish."""

    def test_rsi_70_maps_to_plus_one(self):
        """RSI 70 → momentum component = +1.0

        4 components present (short_volume + volume missing), sum=0.75:
        Effective momentum weight = 0.15/0.75 = 0.20
        combined = 0.20 * 1.0 = 0.20
        value = 50 + 50*0.20 = 60.0
        """
        sigs = [
            _sig("return_1d", 50.0),             # neutral
            _sig("rsi_14", 0.0, value=70.0),     # → +1.0
            _sig("order_flow_imbalance", 50.0),   # neutral
            _sig("bid_ask_spread_bps",   50.0),   # neutral
        ]
        si = compute_market_sub_index(sigs)
        assert si is not None
        assert abs(si.value - 60.0) < 0.01

    def test_rsi_30_maps_to_minus_one(self):
        """RSI 30 → momentum component = -1.0

        4 components present (short_volume + volume missing), sum=0.75:
        Effective momentum weight = 0.15/0.75 = 0.20
        combined = -0.20
        value = 50 + 50*(-0.20) = 40.0
        """
        sigs = [
            _sig("return_1d", 50.0),
            _sig("rsi_14", 0.0, value=30.0),     # → -1.0
            _sig("order_flow_imbalance", 50.0),
            _sig("bid_ask_spread_bps",   50.0),
        ]
        si = compute_market_sub_index(sigs)
        assert si is not None
        assert abs(si.value - 40.0) < 0.01

    def test_rsi_50_maps_to_zero(self):
        """RSI 50 → momentum component = 0.0 (neutral)."""
        sigs = _full_signals(return_score=50.0, rsi_raw=50.0, ofi_score=50.0, spread_score=50.0)
        si = compute_market_sub_index(sigs)
        assert si is not None
        assert si.value == 50.0


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

class TestMetadata:

    def test_n_signals_counts_only_component_signals(self):
        sigs = _full_signals() + [_sig("buy_pressure", 60.0)]
        si = compute_market_sub_index(sigs)
        assert si is not None
        # 3 return_* + rsi_14 + order_flow_imbalance + bid_ask_spread_bps = 6
        assert si.n_signals == 6

    def test_sources_collected(self):
        sigs = [
            {**_sig("return_1d", 75.0), "source": "computed"},
            {**_sig("rsi_14", 0.0, value=60.0), "source": "computed"},
            {**_sig("order_flow_imbalance", 75.0), "source": "yfinance"},
            {**_sig("bid_ask_spread_bps", 75.0), "source": "yfinance"},
        ]
        si = compute_market_sub_index(sigs)
        assert si is not None
        assert si.sources == ["computed", "yfinance"]

    def test_returns_component_averages_multiple_periods(self):
        """Returns component uses the mean of available return periods."""
        # return_1d = +1.0, return_5d = -1.0, return_20d = 0.0
        # mean = 0.0 → neutral
        sigs = [
            _sig("return_1d",  100.0),  # → +1.0
            _sig("return_5d",    0.0),  # → -1.0
            _sig("return_20d",  50.0),  # →  0.0
            _sig("rsi_14", 0.0, value=50.0),
            _sig("order_flow_imbalance", 50.0),
            _sig("bid_ask_spread_bps",   50.0),
        ]
        si = compute_market_sub_index(sigs)
        assert si is not None
        # returns component = 0.0, all others neutral → value = 50
        assert abs(si.value - 50.0) < 0.01


# ---------------------------------------------------------------------------
# 6-component aggregation (all 6 including short_volume + volume)
# ---------------------------------------------------------------------------

def _full_6_signals(
    return_score: float = 50.0,
    rsi_raw: float = 50.0,
    ofi_score: float = 50.0,
    spread_score: float = 50.0,
    sv_score: float = 50.0,
    vol_score: float = 50.0,
) -> list[dict]:
    """Build signals for all 6 components."""
    return [
        _sig("return_1d",  return_score),
        _sig("return_5d",  return_score),
        _sig("return_20d", return_score),
        _sig("rsi_14",     0.0, value=rsi_raw),
        _sig("order_flow_imbalance", ofi_score),
        _sig("bid_ask_spread_bps",   spread_score),
        _sig("short_volume_ratio_otc", sv_score),
        _sig("volume_ratio", vol_score),
    ]


class TestSixComponentAggregation:
    """Tests with all 6 components present (no weight redistribution)."""

    def test_all_six_strongly_bullish(self):
        """All 6 components at +1.0 → value = 100."""
        sigs = _full_6_signals(
            return_score=100.0,    # → +1.0
            rsi_raw=70.0,          # → +1.0
            ofi_score=100.0,       # → +1.0
            spread_score=100.0,    # → +1.0
            sv_score=100.0,        # → +1.0
            vol_score=80.0,        # → +0.6 (volume_ratio scorer caps at 80)
        )
        si = compute_market_sub_index(sigs)
        assert si is not None
        # volume component = (80-50)/50 = +0.6, all others +1.0
        # combined = 0.30*1 + 0.15*1 + 0.20*1 + 0.10*1 + 0.15*1 + 0.10*0.6 = 0.96
        assert abs(si.value - (50 + 50*0.96)) < 0.01

    def test_all_six_strongly_bearish(self):
        """All 6 components at -1.0 → value = 0."""
        sigs = _full_6_signals(
            return_score=0.0,      # → -1.0
            rsi_raw=30.0,          # → -1.0
            ofi_score=0.0,         # → -1.0
            spread_score=0.0,      # → -1.0
            sv_score=0.0,          # → -1.0
            vol_score=20.0,        # → -0.6 (volume_ratio scorer floors at 20)
        )
        si = compute_market_sub_index(sigs)
        assert si is not None
        # volume component = (20-50)/50 = -0.6
        # combined = 0.30*(-1) + ... + 0.10*(-0.6) = -0.96
        assert abs(si.value - (50 + 50*(-0.96))) < 0.01

    def test_all_six_neutral(self):
        """All 6 at neutral → value = 50."""
        sigs = _full_6_signals()
        si = compute_market_sub_index(sigs)
        assert si is not None
        assert si.value == 50.0

    def test_short_volume_spike_others_flat(self):
        """Short volume spike (score=0 → -1) with everything else neutral.

        All 6 present, sum=1.0:
        combined = 0.30*0 + 0.15*0 + 0.20*0 + 0.10*0 + 0.15*(-1) + 0.10*0 = -0.15
        value = 50 + 50*(-0.15) = 42.5
        """
        sigs = _full_6_signals(
            return_score=50.0,     # neutral
            rsi_raw=50.0,          # neutral
            ofi_score=50.0,        # neutral
            spread_score=50.0,     # neutral
            sv_score=0.0,          # → -1.0 (bearish spike)
            vol_score=50.0,        # neutral
        )
        si = compute_market_sub_index(sigs)
        assert si is not None
        assert abs(si.value - 42.5) < 0.01

    def test_short_volume_bullish_others_flat(self):
        """Short volume low (score=100 → +1) with everything else neutral.

        combined = 0.15*(+1) = +0.15
        value = 50 + 50*0.15 = 57.5
        """
        sigs = _full_6_signals(
            return_score=50.0,
            rsi_raw=50.0,
            ofi_score=50.0,
            spread_score=50.0,
            sv_score=100.0,        # → +1.0 (low short volume = bullish)
            vol_score=50.0,
        )
        si = compute_market_sub_index(sigs)
        assert si is not None
        assert abs(si.value - 57.5) < 0.01

    def test_volume_spike_others_flat(self):
        """Volume ratio elevated (score=80 → +0.6) with everything else neutral.

        combined = 0.10*(+0.6) = +0.06
        value = 50 + 50*0.06 = 53.0
        """
        sigs = _full_6_signals(
            return_score=50.0,
            rsi_raw=50.0,
            ofi_score=50.0,
            spread_score=50.0,
            sv_score=50.0,
            vol_score=80.0,        # → +0.6 (elevated volume = mild bullish)
        )
        si = compute_market_sub_index(sigs)
        assert si is not None
        assert abs(si.value - 53.0) < 0.01

    def test_volume_low_others_flat(self):
        """Volume ratio low (score=20 → -0.6) with everything else neutral.

        combined = 0.10*(-0.6) = -0.06
        value = 50 + 50*(-0.06) = 47.0
        """
        sigs = _full_6_signals(
            return_score=50.0,
            rsi_raw=50.0,
            ofi_score=50.0,
            spread_score=50.0,
            sv_score=50.0,
            vol_score=20.0,        # → -0.6 (low volume = mild bearish)
        )
        si = compute_market_sub_index(sigs)
        assert si is not None
        assert abs(si.value - 47.0) < 0.01

    def test_sv_plus_returns_others_missing(self):
        """Short volume + returns present, 4 others missing.

        Present: returns=0.30, short_volume=0.15 → sum=0.45
        Effective: returns=0.30/0.45=0.6667, short_volume=0.15/0.45=0.3333

        Both at +0.5:
        combined = 0.6667*0.5 + 0.3333*0.5 = 0.50
        value = 50 + 50*0.5 = 75.0
        """
        sigs = [
            _sig("return_1d",  75.0),   # → +0.5
            _sig("return_5d",  75.0),   # → +0.5
            _sig("return_20d", 75.0),   # → +0.5
            _sig("short_volume_ratio_otc", 75.0),  # → +0.5
        ]
        si = compute_market_sub_index(sigs)
        assert si is not None
        assert abs(si.value - 75.0) < 0.01

    def test_n_signals_with_all_six(self):
        """n_signals should count all 6 component signal types."""
        sigs = _full_6_signals()
        si = compute_market_sub_index(sigs)
        assert si is not None
        # 3 return_* + rsi_14 + order_flow + spread + short_volume + volume_ratio = 8
        assert si.n_signals == 8

    def test_weights_sum_to_one(self):
        """Verify the 6 component weights sum to 1.0."""
        assert abs(sum(MARKET_COMPONENT_WEIGHTS.values()) - 1.0) < 1e-9

    def test_volume_ratio_is_component_signal(self):
        """volume_ratio should now affect the sub-index value (unlike buy/sell pressure)."""
        base = _full_signals(return_score=75.0, rsi_raw=60.0, ofi_score=75.0, spread_score=75.0)
        base_si = compute_market_sub_index(base)

        with_volume = base + [_sig("volume_ratio", 80.0)]
        vol_si = compute_market_sub_index(with_volume)

        assert base_si is not None
        assert vol_si is not None
        # Adding a bullish volume signal should increase the sub-index
        assert vol_si.value > base_si.value
