"""
tests/test_macro_subindex.py — Sprint P4.4

Coverage for the dedicated `compute_macro_sub_index` aggregator
(`pipeline/scoring/subindices.py`):
  • Paper-direct weight table (vix=1.0, etf=1.5, 10y=1.0, 2y=0.75, ted=1.0)
  • NO `min(1, n/d)` shrinkage  (paper: macro signals are market-wide)
  • Weight redistribution when components are missing
  • Empty input → None (Redis fallback path handles it)
  • Non-macro signal types are silently ignored
  • Source list metadata
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipeline.scoring.subindices import (
    SubIndexResult,
    compute_macro_sub_index,
    _MACRO_SIGNAL_WEIGHTS,
)


def _ts(offset_seconds: int = 0) -> datetime:
    return datetime(2026, 5, 16, 12, 0, tzinfo=timezone.utc).replace(
        second=offset_seconds,
    )


def _sig(sig_type: str, score: float, source: str = "fred", weight: float = 1.0,
         ts: datetime | None = None) -> dict:
    """Pre-scored signal dict, mirroring what `score_macro_signals` emits."""
    return {
        "signal_type": sig_type,
        "score":       score,
        "weight":      weight,
        "source":      source,
        "timestamp":   ts or _ts(),
    }


# ===========================================================================
# 1. Weight table — paper transcription
# ===========================================================================

class TestWeightTable:

    def test_all_five_signal_types_present(self):
        assert set(_MACRO_SIGNAL_WEIGHTS.keys()) == {
            "vix", "sector_etf_return_20d",
            "treasury_yield_10y", "treasury_yield_2y", "ted_spread",
        }

    def test_paper_values_exact(self):
        """Paper §Macroeconomic Signals — weight table transcribed directly."""
        assert _MACRO_SIGNAL_WEIGHTS["vix"]                   == 1.0
        assert _MACRO_SIGNAL_WEIGHTS["sector_etf_return_20d"] == 1.5
        assert _MACRO_SIGNAL_WEIGHTS["treasury_yield_10y"]    == 1.0
        assert _MACRO_SIGNAL_WEIGHTS["treasury_yield_2y"]     == 0.75
        assert _MACRO_SIGNAL_WEIGHTS["ted_spread"]            == 1.0

    def test_weight_table_sums_to_five_and_a_quarter(self):
        assert sum(_MACRO_SIGNAL_WEIGHTS.values()) == 5.25


# ===========================================================================
# 2. All five present — exact arithmetic
# ===========================================================================

class TestAllFivePresent:

    def test_uniform_neutral_returns_fifty(self):
        sigs = [
            _sig("vix",                   50.0),
            _sig("sector_etf_return_20d", 50.0),
            _sig("treasury_yield_10y",    50.0),
            _sig("treasury_yield_2y",     50.0),
            _sig("ted_spread",            50.0),
        ]
        result = compute_macro_sub_index(sigs)
        assert result is not None
        assert result.value == pytest.approx(50.0)
        assert result.n_signals == 5

    def test_uniform_bullish_eighty(self):
        sigs = [_sig(t, 80.0) for t in _MACRO_SIGNAL_WEIGHTS]
        result = compute_macro_sub_index(sigs)
        assert result is not None
        assert result.value == pytest.approx(80.0)

    def test_uniform_bearish_twenty(self):
        sigs = [_sig(t, 20.0) for t in _MACRO_SIGNAL_WEIGHTS]
        result = compute_macro_sub_index(sigs)
        assert result is not None
        assert result.value == pytest.approx(20.0)

    def test_weighted_mix_uses_paper_weights(self):
        """
        Verify the 1.5x ETF weight has the intended effect.

        VIX=80, ETF=20, others=50. With paper weights:
          numer = 80·1.0 + 20·1.5 + 50·1.0 + 50·0.75 + 50·1.0
                = 80 + 30 + 50 + 37.5 + 50 = 247.5
          denom = 1.0 + 1.5 + 1.0 + 0.75 + 1.0 = 5.25
          value = 247.5 / 5.25 ≈ 47.14

        Under a hypothetical uniform-weight aggregator the value would be
        (80+20+50+50+50)/5 = 50.0 — so the difference is the proof the
        ETF carries 1.5x weight (pulls the score *down* here toward 20).
        """
        sigs = [
            _sig("vix",                   80.0),
            _sig("sector_etf_return_20d", 20.0),
            _sig("treasury_yield_10y",    50.0),
            _sig("treasury_yield_2y",     50.0),
            _sig("ted_spread",            50.0),
        ]
        result = compute_macro_sub_index(sigs)
        assert result is not None
        assert result.value == pytest.approx(47.1429, abs=1e-3)


# ===========================================================================
# 3. No shrinkage (key behavioural change from P4.2's stopgap)
# ===========================================================================

class TestNoShrinkage:

    def test_single_signal_preserves_raw_value(self):
        """1 signal at score=80 must yield value=80 — NOT pulled toward 50.

        This is the headline P4.4 behavioural shift. Under the P4.2-era
        `compute_sub_index(sigs, shrinkage_denominator=2)` the result
        would have been `50 + 0.5·30 = 65`. Under the paper-direct
        `compute_macro_sub_index` there is no shrinkage at all.
        """
        sigs = [_sig("vix", 80.0)]
        result = compute_macro_sub_index(sigs)
        assert result is not None
        assert result.value == pytest.approx(80.0)
        assert result.n_signals == 1

    def test_two_signals_no_shrinkage(self):
        """2 signals at 80 → 80 (matches P4.2 stopgap with denominator=2)."""
        sigs = [_sig("vix", 80.0), _sig("ted_spread", 80.0)]
        result = compute_macro_sub_index(sigs)
        assert result is not None
        assert result.value == pytest.approx(80.0)

    def test_extreme_single_signal_not_compressed(self):
        """Score=100 with 1 signal → 100, not capped/shrunk."""
        sigs = [_sig("vix", 100.0)]
        result = compute_macro_sub_index(sigs)
        assert result is not None
        assert result.value == pytest.approx(100.0)


# ===========================================================================
# 4. Missing components → weight redistribution (paper-direct)
# ===========================================================================

class TestMissingComponents:

    def test_drop_ted_redistributes_other_four(self):
        """Without TED, denom drops 5.25→4.25; remaining 4 share."""
        sigs = [
            _sig("vix",                   80.0),
            _sig("sector_etf_return_20d", 80.0),
            _sig("treasury_yield_10y",    80.0),
            _sig("treasury_yield_2y",     80.0),
        ]
        result = compute_macro_sub_index(sigs)
        assert result is not None
        # All uniform → value = 80, unchanged by the redistribution
        assert result.value == pytest.approx(80.0)
        assert result.n_signals == 4

    def test_only_vix_and_etf_uses_their_summed_weights(self):
        """
        Subset: vix(1.0) + etf(1.5) only.
          denom = 2.5
          numer = 60·1.0 + 80·1.5 = 60 + 120 = 180
          value = 180 / 2.5 = 72.0
        """
        sigs = [
            _sig("vix",                   60.0),
            _sig("sector_etf_return_20d", 80.0),
        ]
        result = compute_macro_sub_index(sigs)
        assert result is not None
        assert result.value == pytest.approx(72.0)
        assert result.n_signals == 2


# ===========================================================================
# 5. Edge cases — empty / non-macro / zero-weight
# ===========================================================================

class TestEdgeCases:

    def test_empty_input_returns_none(self):
        assert compute_macro_sub_index([]) is None

    def test_only_non_macro_signal_types_returns_none(self):
        """Rows from other layers must not contribute to the macro sub-index."""
        sigs = [
            _sig("rsi_14",          70.0, source="computed"),
            _sig("analyst_buy_pct", 80.0, source="finnhub"),
        ]
        assert compute_macro_sub_index(sigs) is None

    def test_zero_weight_signal_excluded(self):
        """A row with weight=0 is silently dropped (mirrors compute_sub_index)."""
        sigs = [
            _sig("vix",        80.0, weight=0.0),
            _sig("ted_spread", 20.0, weight=1.0),
        ]
        result = compute_macro_sub_index(sigs)
        assert result is not None
        assert result.value == pytest.approx(20.0)
        assert result.n_signals == 1

    def test_duplicate_signal_type_keeps_most_recent(self):
        """Two VIX rows in the same batch → the newer one wins."""
        sigs = [
            _sig("vix", 20.0, ts=_ts(0)),   # older
            _sig("vix", 80.0, ts=_ts(30)),  # newer
        ]
        result = compute_macro_sub_index(sigs)
        assert result is not None
        assert result.value == pytest.approx(80.0)
        assert result.n_signals == 1


# ===========================================================================
# 6. Metadata
# ===========================================================================

class TestMetadata:

    def test_sources_aggregated_across_signals(self):
        sigs = [
            _sig("vix",                   50.0, source="yfinance"),
            _sig("sector_etf_return_20d", 50.0, source="alpha_vantage"),
            _sig("treasury_yield_10y",    50.0, source="fred"),
        ]
        result = compute_macro_sub_index(sigs)
        assert result is not None
        assert result.sources == ["alpha_vantage", "fred", "yfinance"]

    def test_returns_subindexresult_instance(self):
        sigs = [_sig("vix", 50.0)]
        result = compute_macro_sub_index(sigs)
        assert isinstance(result, SubIndexResult)
        assert result.value is not None
        assert result.n_signals == 1


# ===========================================================================
# 7. Orchestrator dispatch — `_score_macro` invokes the new aggregator
# ===========================================================================

class TestOrchestratorDispatch:

    @pytest.mark.asyncio
    async def test_score_macro_calls_compute_macro_sub_index(self):
        """`_score_macro` must call compute_macro_sub_index — NOT compute_sub_index."""
        from unittest.mock import AsyncMock, patch
        from pipeline.scoring.subindices import SubIndexResult as _SR

        mock_result = _SR(value=72.5, n_signals=4, sources=["fred"])

        # Both aggregators get spies; verify only the macro one fires
        # on the macro layer call path.
        with (
            # Patch the orchestrator's own references — it imports these by
            # name, so patching the source modules would not take effect and
            # the test would hit the real database.
            patch("pipeline.orchestrator.get_signals_since",
                  new=AsyncMock(return_value=[{
                      "signal_type": "vix", "value": 22.0, "source": "yfinance",
                      "timestamp": _ts(),
                  }])),
            patch("pipeline.orchestrator.score_macro_signals",
                  new=AsyncMock(return_value=[_sig("vix", 50.0)])),
            patch("pipeline.orchestrator.compute_macro_sub_index",
                  return_value=mock_result) as mock_macro,
            patch("pipeline.orchestrator.compute_sub_index") as mock_generic,
        ):
            from pipeline.orchestrator import _score_macro
            si, _sigs, _as_of = await _score_macro(
                ticker="AAPL", sector="Information Technology",
                now=_ts(), last_state=None,
            )

        # The macro aggregator was used; the generic one was NOT called by the
        # macro path. (Other layers may still call compute_sub_index on their
        # own paths — that's fine, this test only proves the swap at this one
        # call site.)
        assert mock_macro.call_count == 1
        assert mock_generic.call_count == 0
        assert si is mock_result
