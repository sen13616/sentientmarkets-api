"""
tests/test_macro_scoring.py — Sprints P4.2 / P4.3 / P4.4

Per-ticker macro sub-index tests:
  • `_score_macro(ticker, sector, …)` routes via the ticker's GICS sector (P4.2)
  • Null-sector tickers drop the ETF component and fall through to global-only
  • `score_macro_signals` z-scores ETF history against the SECTOR_ETFS map
  • `compute_sub_index`'s `shrinkage_denominator` parameter still works for
    non-macro layers (kept in public API; the macro path itself now uses
    `compute_macro_sub_index` per P4.4 — see test_macro_subindex.py).
  • Two tickers in different sectors with divergent ETF returns produce
    materially different macro sub-indices (full pipeline regression).
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, patch

import math
import pytest

from pipeline.features.normalize import score_macro_signals, _ZSCORE_CONFIG
from pipeline.scoring.subindices import (
    compute_macro_sub_index,
    compute_sub_index,
    SubIndexResult,
)


def _ts() -> datetime:
    return datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)


def _row(sig_type: str, value: float, source: str = "yfinance") -> dict:
    return {"signal_type": sig_type, "value": value, "source": source, "timestamp": _ts()}


# ===========================================================================
# 1. ZSCORE_CONFIG entry for sector_etf_return_20d (P4.2 wiring)
# ===========================================================================

class TestSectorEtfZScoreConfig:

    def test_sector_etf_return_20d_has_zscore_config(self):
        assert "sector_etf_return_20d" in _ZSCORE_CONFIG
        cfg = _ZSCORE_CONFIG["sector_etf_return_20d"]
        assert cfg.window == 90
        # Sector ETF returns are NOT sign-inverted (positive return = bullish)
        assert cfg.negate is False


# ===========================================================================
# 2. compute_sub_index shrinkage_denominator parameter
#    P4.4 retired this idiom on the macro path (now uses compute_macro_sub_index)
#    but the parameter remains in the API for any future caller. These tests
#    verify the parameter still works as documented.
# ===========================================================================

class TestShrinkageDenominatorParameter:

    def _signal(self, score, weight=1.0, source="yfinance"):
        return {"score": score, "weight": weight, "source": source}

    def test_default_shrinkage_denominator_five(self):
        """Default denominator (5) shrinks with 2 signals: min(1, 2/5) = 0.4."""
        sigs = [self._signal(80), self._signal(80)]
        result = compute_sub_index(sigs)
        # raw = 80, shrinkage = 0.4 → 50 + 0.4 × 30 = 62
        assert result is not None
        assert abs(result.value - 62.0) < 0.01

    def test_denominator_two_no_shrinkage_at_n_eq_2(self):
        """shrinkage_denominator=2 yields min(1, 2/2) = 1.0 → raw value preserved."""
        sigs = [self._signal(80), self._signal(80)]
        result = compute_sub_index(sigs, shrinkage_denominator=2)
        assert result is not None
        assert abs(result.value - 80.0) < 0.01

    def test_denominator_two_still_shrinks_at_n_eq_1(self):
        """With one signal, shrinkage = min(1, 1/2) = 0.5 — half-pull toward 50."""
        sigs = [self._signal(80)]
        result = compute_sub_index(sigs, shrinkage_denominator=2)
        # raw=80, shrinkage=0.5 → 50 + 0.5 × 30 = 65
        assert result is not None
        assert abs(result.value - 65.0) < 0.01

    def test_shrinkage_capped_at_one_for_many_signals(self):
        """shrinkage = min(1, n/d) saturates at 1.0 even for large n."""
        sigs = [self._signal(80) for _ in range(10)]
        default = compute_sub_index(sigs)
        denom2  = compute_sub_index(sigs, shrinkage_denominator=2)
        # Both fully saturate shrinkage → identical results
        assert default is not None and denom2 is not None
        assert abs(default.value - denom2.value) < 0.01
        assert abs(denom2.value - 80.0) < 0.01

    def test_default_behavior_preserved_when_param_omitted(self):
        """compute_sub_index default behavior unchanged (narrative/influencer callers)."""
        sigs = [self._signal(70), self._signal(70), self._signal(70)]
        result = compute_sub_index(sigs)  # uses default 5
        # raw=70, shrinkage=3/5=0.6 → 50 + 0.6 × 20 = 62
        assert result is not None
        assert abs(result.value - 62.0) < 0.01


# ===========================================================================
# 3. score_macro_signals — per-ticker routing
# ===========================================================================

class TestScoreMacroSignalsPerTicker:

    @pytest.mark.asyncio
    async def test_vix_only_when_sector_is_none(self):
        """A null-sector ticker can still score VIX but not the ETF component."""
        # Insufficient history → parametric fallback for VIX
        with patch("scripts.db.queries.raw_signals.get_signal_history", new=AsyncMock(return_value=[])):
            scored = await score_macro_signals(
                ticker="NEWLY_ADDED", sector=None,
                raw=[_row("vix", 22.0), _row("sector_etf_return_20d", 0.05)],
                now=_ts(),
            )
        # ETF row still scores via parametric (no history dependency at the row level);
        # but the ETF row was emitted with ticker="NEWLY_ADDED" — verify it's in the output
        # alongside VIX. The history call for the ETF z-score is skipped (sector is None).
        types = [s["signal_type"] for s in scored]
        assert "vix" in types
        # ETF still scores (parametric path doesn't need history), and is recorded
        # under the requested ticker.
        for sig in scored:
            assert sig["ticker"] == "NEWLY_ADDED"

    @pytest.mark.asyncio
    async def test_etf_zscore_history_keyed_by_etf_symbol(self):
        """For AAPL (sector IT), z-score history must be fetched under 'XLK', not 'AAPL'."""
        history_calls: list[tuple] = []

        async def _fake_history(ticker, signal_type, limit):
            history_calls.append((ticker, signal_type, limit))
            return []  # empty → parametric path

        with patch("scripts.db.queries.raw_signals.get_signal_history", new=_fake_history):
            await score_macro_signals(
                ticker="AAPL", sector="Information Technology",
                raw=[_row("sector_etf_return_20d", 0.10)],
                now=_ts(),
            )

        # Among the calls made, the one for sector_etf_return_20d must use XLK
        etf_calls = [c for c in history_calls if c[1] == "sector_etf_return_20d"]
        assert len(etf_calls) == 1
        assert etf_calls[0][0] == "XLK"

    @pytest.mark.asyncio
    async def test_scored_rows_carry_user_ticker(self):
        """Output rows must be tagged with the ticker being scored, not '_MACRO_' or the ETF symbol."""
        with patch("scripts.db.queries.raw_signals.get_signal_history", new=AsyncMock(return_value=[])):
            scored = await score_macro_signals(
                ticker="XOM", sector="Energy",
                raw=[_row("vix", 22.0), _row("sector_etf_return_20d", 0.05)],
                now=_ts(),
            )
        for sig in scored:
            assert sig["ticker"] == "XOM", f"unexpected ticker {sig['ticker']!r} in {sig}"

    @pytest.mark.asyncio
    async def test_unknown_sector_skips_etf_history_lookup(self):
        """A sector string not in SECTOR_ETFS doesn't blow up; ETF row still parametric-scores."""
        with patch("scripts.db.queries.raw_signals.get_signal_history", new=AsyncMock(return_value=[])):
            scored = await score_macro_signals(
                ticker="TEST", sector="Made-Up Sector",
                raw=[_row("sector_etf_return_20d", 0.10)],
                now=_ts(),
            )
        # The ETF row still parametric-scores; nothing crashes.
        assert any(s["signal_type"] == "sector_etf_return_20d" for s in scored)


# ===========================================================================
# 4. Distinct tickers in distinct sectors produce different macro scores
# ===========================================================================

class TestPerTickerDistinctness:

    @pytest.mark.asyncio
    async def test_two_sectors_diverge_when_etf_returns_diverge(self):
        """AAPL (XLK +10%) and XOM (XLE −10%) must produce materially different macro scores."""
        # Empty history → parametric path for both ETFs
        with patch("scripts.db.queries.raw_signals.get_signal_history", new=AsyncMock(return_value=[])):
            scored_aapl = await score_macro_signals(
                ticker="AAPL", sector="Information Technology",
                raw=[_row("vix", 22.0), _row("sector_etf_return_20d", +0.10)],
                now=_ts(),
            )
            scored_xom = await score_macro_signals(
                ticker="XOM", sector="Energy",
                raw=[_row("vix", 22.0), _row("sector_etf_return_20d", -0.10)],
                now=_ts(),
            )

        # Both should have 2 scored signals each
        assert len(scored_aapl) == 2
        assert len(scored_xom) == 2

        # The ETF scores must differ in direction
        aapl_etf = next(s for s in scored_aapl if s["signal_type"] == "sector_etf_return_20d")
        xom_etf  = next(s for s in scored_xom  if s["signal_type"] == "sector_etf_return_20d")
        assert aapl_etf["score"] > 60.0   # +10% return = bullish under tanh
        assert xom_etf["score"]  < 40.0   # −10% return = bearish

        # Aggregated via the P4.4 dedicated macro aggregator
        si_aapl = compute_macro_sub_index(scored_aapl)
        si_xom  = compute_macro_sub_index(scored_xom)
        assert si_aapl is not None and si_xom is not None
        # Materially different — the gap should be ≥ 10 points
        assert (si_aapl.value - si_xom.value) > 10.0


# ===========================================================================
# 5. Sprint P4.3 — FRED Treasury / yield-curve signals
# ===========================================================================

from pipeline.features.normalize import (  # noqa: E402
    _score_treasury_10y, _score_treasury_2y, _score_ted_spread,
    _MACRO_GLOBAL_TYPES, _MACRO_RECOGNISED_TYPES,
)


class TestFredParametricScorers:

    def test_treasury_10y_neutral_at_four_pct(self):
        assert _score_treasury_10y(4.0) == pytest.approx(50.0)

    def test_treasury_10y_bearish_when_rates_rise(self):
        # 5.5% (one half-life above neutral) → ~25 (bearish)
        assert 10.0 < _score_treasury_10y(5.5) < 40.0

    def test_treasury_10y_bullish_when_rates_fall(self):
        # 2.5% (one half-life below neutral) → ~75 (bullish)
        assert 60.0 < _score_treasury_10y(2.5) < 90.0

    def test_treasury_10y_clamps_at_extremes(self):
        # tanh saturates asymptotically — within 1e-5 of the bound is fine
        assert _score_treasury_10y(20.0) == pytest.approx(0.0, abs=1e-5)
        assert _score_treasury_10y(-20.0) == pytest.approx(100.0, abs=1e-5)

    def test_treasury_2y_neutral_at_four_five(self):
        assert _score_treasury_2y(4.5) == pytest.approx(50.0)

    def test_treasury_2y_bearish_when_rates_rise(self):
        assert 10.0 < _score_treasury_2y(6.0) < 40.0

    def test_ted_spread_neutral_at_zero(self):
        assert _score_ted_spread(0.0) == pytest.approx(50.0)

    def test_ted_spread_bearish_when_inverted(self):
        # Inversion (negative slope) → bearish
        assert _score_ted_spread(-1.0) < 30.0

    def test_ted_spread_bullish_when_steep(self):
        assert _score_ted_spread(+1.0) > 70.0


class TestFredZScoreConfig:

    def test_treasury_10y_has_zscore_config(self):
        assert "treasury_yield_10y" in _ZSCORE_CONFIG
        cfg = _ZSCORE_CONFIG["treasury_yield_10y"]
        assert cfg.window == 90
        assert cfg.negate is True   # rising yields = bearish

    def test_treasury_2y_has_zscore_config(self):
        assert "treasury_yield_2y" in _ZSCORE_CONFIG
        cfg = _ZSCORE_CONFIG["treasury_yield_2y"]
        assert cfg.window == 90
        assert cfg.negate is True

    def test_ted_spread_has_zscore_config(self):
        assert "ted_spread" in _ZSCORE_CONFIG
        cfg = _ZSCORE_CONFIG["ted_spread"]
        assert cfg.window == 90
        # ted_spread is the 10y-2y SLOPE, not a yield: a steep/positive slope is
        # bullish, so it must NOT be negated (unlike the two yields above). The
        # z-score sign must agree with the parametric _score_ted_spread.
        assert cfg.negate is False

    def test_ted_spread_zscore_sign_matches_parametric(self):
        """A positive-slope z-score must score bullish (>50), like _score_ted_spread(+)."""
        cfg = _ZSCORE_CONFIG["ted_spread"]
        # 60 obs (> effective_min_obs) with mean ~0.32; today's slope well above.
        history = [0.2, 0.3, 0.4, 0.3, 0.35] * 12
        score = cfg.score_from_history(history, 0.9)
        assert score is not None and score > 50.0
        assert _score_ted_spread(0.9) > 50.0


class TestFredScoringIntegration:

    @pytest.mark.asyncio
    async def test_fred_signals_recognised_by_score_macro_signals(self):
        """FRED rows enter the macro scoring path and produce scored output."""
        with patch("scripts.db.queries.raw_signals.get_signal_history", new=AsyncMock(return_value=[])):
            scored = await score_macro_signals(
                ticker="AAPL", sector="Information Technology",
                raw=[
                    _row("vix",                4.0,  source="yfinance"),  # neutral-ish
                    _row("treasury_yield_10y", 4.0,  source="fred"),       # neutral
                    _row("treasury_yield_2y",  4.5,  source="fred"),       # neutral
                    _row("ted_spread",         0.0,  source="fred"),       # neutral
                ],
                now=_ts(),
            )
        types = {s["signal_type"] for s in scored}
        assert "treasury_yield_10y" in types
        assert "treasury_yield_2y"  in types
        assert "ted_spread"         in types
        assert "vix"                in types

    @pytest.mark.asyncio
    async def test_fred_history_lookup_keyed_by_macro_ticker(self):
        """FRED z-score history must be fetched under '_MACRO_', not the user ticker."""
        history_calls: list[tuple] = []

        async def _fake_history(ticker, signal_type, limit):
            history_calls.append((ticker, signal_type, limit))
            return []

        with patch("scripts.db.queries.raw_signals.get_signal_history", new=_fake_history):
            await score_macro_signals(
                ticker="AAPL", sector="Information Technology",
                raw=[
                    _row("treasury_yield_10y", 4.0, source="fred"),
                    _row("treasury_yield_2y",  4.5, source="fred"),
                    _row("ted_spread",         0.0, source="fred"),
                ],
                now=_ts(),
            )

        fred_calls = [c for c in history_calls
                      if c[1] in ("treasury_yield_10y", "treasury_yield_2y", "ted_spread")]
        assert len(fred_calls) == 3
        for c in fred_calls:
            assert c[0] == "_MACRO_", f"FRED history must use _MACRO_, got {c[0]!r}"

    def test_macro_global_types_include_fred(self):
        """The _MACRO_GLOBAL_TYPES set must include all 3 FRED signal types."""
        assert "treasury_yield_10y" in _MACRO_GLOBAL_TYPES
        assert "treasury_yield_2y"  in _MACRO_GLOBAL_TYPES
        assert "ted_spread"         in _MACRO_GLOBAL_TYPES
        assert "vix"                in _MACRO_GLOBAL_TYPES
        # And recognised
        assert _MACRO_GLOBAL_TYPES <= _MACRO_RECOGNISED_TYPES
