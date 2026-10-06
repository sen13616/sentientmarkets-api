"""
tests/test_influencer_sources.py — Sprint P3.2

Covers:
  - I8: `_analyst_target_yf` fetches `targetMeanPrice` from yfinance via
        `asyncio.to_thread`, returns None on missing / non-numeric / non-positive
        values, surfaces yfinance exceptions as None.
  - I9: `score_influencer_signals` z-scores `analyst_target_price` over the
        per-row *upside* (target - current_close) / current_close series
        (option c — historical upsides computed against the current close).
        Cold-start (insufficient history) falls back to the parametric
        `_score_analyst_target` scorer.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipeline.features import normalize as nm
from pipeline.sources import influencer as influencer_src

# ===========================================================================
# I8 — yfinance fetcher
# ===========================================================================

class TestAnalystTargetYf:

    @pytest.mark.asyncio
    async def test_returns_target_when_present(self):
        fake_ticker = MagicMock()
        fake_ticker.info = {"targetMeanPrice": 215.5}
        with patch.object(influencer_src.yf, "Ticker", return_value=fake_ticker):
            value = await influencer_src._analyst_target_yf("AAPL")
        assert value == 215.5

    @pytest.mark.asyncio
    async def test_returns_none_when_field_missing(self):
        fake_ticker = MagicMock()
        fake_ticker.info = {}
        with patch.object(influencer_src.yf, "Ticker", return_value=fake_ticker):
            value = await influencer_src._analyst_target_yf("XYZ")
        assert value is None

    @pytest.mark.asyncio
    async def test_returns_none_when_field_none(self):
        fake_ticker = MagicMock()
        fake_ticker.info = {"targetMeanPrice": None}
        with patch.object(influencer_src.yf, "Ticker", return_value=fake_ticker):
            value = await influencer_src._analyst_target_yf("XYZ")
        assert value is None

    @pytest.mark.asyncio
    async def test_returns_none_when_field_non_numeric(self):
        fake_ticker = MagicMock()
        fake_ticker.info = {"targetMeanPrice": "n/a"}
        with patch.object(influencer_src.yf, "Ticker", return_value=fake_ticker):
            value = await influencer_src._analyst_target_yf("XYZ")
        assert value is None

    @pytest.mark.asyncio
    async def test_returns_none_when_field_non_positive(self):
        fake_ticker = MagicMock()
        fake_ticker.info = {"targetMeanPrice": 0}
        with patch.object(influencer_src.yf, "Ticker", return_value=fake_ticker):
            value = await influencer_src._analyst_target_yf("XYZ")
        assert value is None

    @pytest.mark.asyncio
    async def test_swallows_yfinance_exception(self):
        def _raise(*_a, **_kw):
            raise RuntimeError("yfinance network glitch")
        with patch.object(influencer_src.yf, "Ticker", side_effect=_raise):
            value = await influencer_src._analyst_target_yf("XYZ")
        assert value is None


# ===========================================================================
# I8 — Source-label change: yfinance row is written with source='yfinance'
# ===========================================================================

class TestRunInfluencerTargetSource:

    @pytest.mark.asyncio
    async def test_target_row_uses_yfinance_source(self):
        """End-to-end: _run_influencer with mocked sub-calls writes target with source='yfinance'."""
        captured: list = []

        async def _capture_insert(rows):
            captured.extend(rows)

        async def _no_insider(*_a, **_kw):
            return []  # no insider data this tick

        async def _analyst_pct(*_a, **_kw):
            return None  # not under test

        async def _target_value(_ticker):
            return 250.0

        async def _no_eps(_ticker):
            return None  # P3.3 signal not under test here

        client = MagicMock()
        with patch.object(influencer_src, "insert_signals", new=AsyncMock(side_effect=_capture_insert)), \
             patch.object(influencer_src, "_insider_finnhub", new=AsyncMock(side_effect=_no_insider)), \
             patch.object(influencer_src, "_analyst_recommendations", new=AsyncMock(side_effect=_analyst_pct)), \
             patch.object(influencer_src, "_analyst_target_yf", new=AsyncMock(side_effect=_target_value)), \
             patch.object(influencer_src, "_earnings_estimate_yf", new=AsyncMock(side_effect=_no_eps)):
            await influencer_src._run_influencer("AAPL", client)

        # Should have exactly one row written: the analyst_target_price one
        assert len(captured) == 1
        ticker, sig_type, value, source, upload_type, _ts = captured[0]
        assert ticker == "AAPL"
        assert sig_type == "analyst_target_price"
        assert value == 250.0
        assert source == "yfinance"
        assert upload_type == "live"


# ===========================================================================
# I9 — z-score over upside (option c)
# ===========================================================================

def _row(value, *, sig_type="analyst_target_price", source="yfinance"):
    return {
        "signal_type": sig_type,
        "value": value,
        "source": source,
        "timestamp": datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc),
    }


class TestAnalystTargetPriceZScorePath:

    @pytest.mark.asyncio
    async def test_zscore_used_when_enough_history(self):
        """With ≥45 historical targets and a current_price, the upside z-score path engages."""
        # History of 60 target prices centered around 100 with small noise
        history = [100.0 + (i % 5) * 2 - 4 for i in range(60)]  # values in {96, 98, 100, 102, 104}
        current_price = 100.0
        # current_target = 110 → upside = +0.10; historical upsides centered near 0 → strongly bullish z
        current_target = 110.0

        with patch("scripts.db.queries.raw_signals.get_signal_history", new=AsyncMock(return_value=history)):
            scored = await nm.score_influencer_signals(
                "TEST", [_row(current_target)],
                now=datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc),
                current_price=current_price,
            )
        assert len(scored) == 1
        sig = scored[0]
        assert sig["signal_type"] == "analyst_target_price"
        # Strongly bullish (max z=+3 maps to 100); +10% upside vs ~0% mean is >3σ → clipped to 100
        assert sig["score"] > 90.0
        # source weight = 0.85 (P3.1), w_author × w_conf = 1 × 1, time decay ~1.0 (same ts as now)
        assert abs(sig["weight"] - 0.85) < 0.02
        assert sig["source"] == "yfinance"

    @pytest.mark.asyncio
    async def test_zscore_bearish_when_target_below_history(self):
        """Target below history → upside negative vs historical upsides → bearish score."""
        history = [120.0 + (i % 5) * 2 - 4 for i in range(60)]  # values in {116, 118, 120, 122, 124}
        current_price = 100.0
        current_target = 105.0  # +5% upside; historical upsides centered near +20% → bearish

        with patch("scripts.db.queries.raw_signals.get_signal_history", new=AsyncMock(return_value=history)):
            scored = await nm.score_influencer_signals(
                "TEST", [_row(current_target)],
                now=datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc),
                current_price=current_price,
            )
        assert len(scored) == 1
        assert scored[0]["score"] < 10.0  # strongly bearish (clipped at z=-3)

    @pytest.mark.asyncio
    async def test_parametric_fallback_when_insufficient_history(self):
        """Cold-start: < 45 obs → falls back to parametric _score_analyst_target."""
        history = [100.0] * 10  # only 10 obs — below the 45 effective_min
        current_price = 100.0
        current_target = 110.0  # +10% upside → parametric ≈ 50 + 50*tanh(0.10/0.15) ≈ 79

        with patch("scripts.db.queries.raw_signals.get_signal_history", new=AsyncMock(return_value=history)):
            scored = await nm.score_influencer_signals(
                "TEST", [_row(current_target)],
                now=datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc),
                current_price=current_price,
            )
        assert len(scored) == 1
        # Parametric score around 79
        assert 70.0 < scored[0]["score"] < 85.0

    @pytest.mark.asyncio
    async def test_parametric_fallback_when_no_current_price(self):
        """current_price=None → can't compute upside → parametric path also returns None → row skipped."""
        history = [100.0] * 60

        with patch("scripts.db.queries.raw_signals.get_signal_history", new=AsyncMock(return_value=history)):
            scored = await nm.score_influencer_signals(
                "TEST", [_row(110.0)],
                now=datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc),
                current_price=None,
            )
        assert scored == []  # parametric requires current_price; row dropped

    @pytest.mark.asyncio
    async def test_telemetry_records_zscore_path(self):
        """When z-score path succeeds, telemetry records 'zscore', not 'parametric_fallback'."""
        history = [100.0 + (i % 5) for i in range(60)]
        nm.reset_scoring_telemetry()
        with patch("scripts.db.queries.raw_signals.get_signal_history", new=AsyncMock(return_value=history)):
            await nm.score_influencer_signals(
                "TEST", [_row(110.0)],
                now=datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc),
                current_price=100.0,
            )
        counts = nm._scoring_method_counts.get("analyst_target_price", {})
        assert counts.get("zscore", 0) >= 1
        assert counts.get("parametric_fallback", 0) == 0


# ===========================================================================
# Sprint P3.3 — earnings estimate revisions
# ===========================================================================

class TestEarningsEstimateYf:

    @pytest.mark.asyncio
    async def test_returns_avg_eps_for_current_quarter(self):
        """`0q` row's `avg` column becomes the stored mean-EPS value."""
        # Mimic the (DataFrame-like) yfinance return: row.get("avg") → 1.89
        class _Row:
            def get(self, key, default=None):
                return {"avg": 1.89}.get(key, default)

        class _Est:
            empty = False
            index = ["0q", "+1q", "0y", "+1y"]
            def __contains__(self, item):
                return item in self.index
            class _Loc:
                def __getitem__(self, key):
                    if key == "0q":
                        return _Row()
                    raise KeyError(key)
            loc = _Loc()

        class _Ticker:
            def get_earnings_estimate(self):
                return _Est()

        with patch.object(influencer_src.yf, "Ticker", return_value=_Ticker()):
            value = await influencer_src._earnings_estimate_yf("AAPL")
        assert value == pytest.approx(1.89)

    @pytest.mark.asyncio
    async def test_returns_none_when_no_0q_row(self):
        class _Est:
            empty = False
            index = ["+1q", "0y", "+1y"]
            def __contains__(self, item):
                return item in self.index

        class _Ticker:
            def get_earnings_estimate(self):
                return _Est()

        with patch.object(influencer_src.yf, "Ticker", return_value=_Ticker()):
            value = await influencer_src._earnings_estimate_yf("XYZ")
        assert value is None

    @pytest.mark.asyncio
    async def test_returns_none_when_dataframe_empty(self):
        class _Est:
            empty = True

        class _Ticker:
            def get_earnings_estimate(self):
                return _Est()

        with patch.object(influencer_src.yf, "Ticker", return_value=_Ticker()):
            value = await influencer_src._earnings_estimate_yf("XYZ")
        assert value is None

    @pytest.mark.asyncio
    async def test_swallows_yfinance_exception(self):
        class _Ticker:
            def get_earnings_estimate(self):
                raise RuntimeError("yfinance schema changed")

        with patch.object(influencer_src.yf, "Ticker", return_value=_Ticker()):
            value = await influencer_src._earnings_estimate_yf("XYZ")
        assert value is None


class TestRunInfluencerEarningsRow:

    @pytest.mark.asyncio
    async def test_eps_row_uses_yfinance_source(self):
        """End-to-end: _run_influencer with mocked sub-calls writes an EPS row with source='yfinance'."""
        captured: list = []

        async def _capture_insert(rows):
            captured.extend(rows)

        async def _none(*_a, **_kw):
            return None

        async def _no_insider(*_a, **_kw):
            return []

        async def _eps_value(_ticker):
            return 1.89

        client = MagicMock()
        with patch.object(influencer_src, "insert_signals", new=AsyncMock(side_effect=_capture_insert)), \
             patch.object(influencer_src, "_insider_finnhub", new=AsyncMock(side_effect=_no_insider)), \
             patch.object(influencer_src, "_analyst_recommendations", new=AsyncMock(side_effect=_none)), \
             patch.object(influencer_src, "_analyst_target_yf", new=AsyncMock(side_effect=_none)), \
             patch.object(influencer_src, "_earnings_estimate_yf", new=AsyncMock(side_effect=_eps_value)):
            await influencer_src._run_influencer("AAPL", client)

        assert len(captured) == 1
        ticker, sig_type, value, source, upload_type, _ts = captured[0]
        assert ticker == "AAPL"
        assert sig_type == "analyst_eps_estimate_mean"
        assert value == pytest.approx(1.89)
        assert source == "yfinance"
        assert upload_type == "live"


def _eps_row(value, ts=None):
    return {
        "signal_type": "analyst_eps_estimate_mean",
        "value": value,
        "source": "yfinance",
        "timestamp": ts or datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc),
    }


class TestEarningsRevisionDeltaPath:

    def test_parametric_scorer_neutral_at_zero(self):
        assert nm._score_earnings_revision_delta(0.0) == pytest.approx(50.0)

    def test_parametric_scorer_bullish_on_positive_delta(self):
        # +5% revision → tanh(1) ≈ 0.7616 → 50 + 38.1 ≈ 88.1
        assert 80.0 < nm._score_earnings_revision_delta(0.05) < 95.0

    def test_parametric_scorer_bearish_on_negative_delta(self):
        assert 5.0 < nm._score_earnings_revision_delta(-0.05) < 20.0

    def test_parametric_scorer_clamps_at_extremes(self):
        assert nm._score_earnings_revision_delta(10.0) == pytest.approx(100.0)
        assert nm._score_earnings_revision_delta(-10.0) == pytest.approx(0.0)

    @pytest.mark.asyncio
    async def test_revision_emitted_when_history_has_two_obs(self):
        """With one prior obs in history, scoring emits earnings_estimate_revision."""
        # history is oldest-first; newest entry == current value (just written)
        history = [1.50, 1.65]  # prior=1.50, current=1.65 → delta = +10%
        with patch("scripts.db.queries.raw_signals.get_signal_history", new=AsyncMock(return_value=history)):
            scored = await nm.score_influencer_signals(
                "TEST", [_eps_row(1.65)],
                now=datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc),
                current_price=100.0,
            )
        revisions = [s for s in scored if s["signal_type"] == "earnings_estimate_revision"]
        assert len(revisions) == 1
        sig = revisions[0]
        # Stored value should be the delta itself
        assert sig["value"] == pytest.approx(0.10)
        # +10% delta is > half-scale → strongly bullish under parametric formula
        assert sig["score"] > 90.0
        # Weight = _INFLUENCER_SIGNAL_WEIGHT["earnings_estimate_revision"] = 0.80
        # × w_author=1 × w_conf=1 × w_time≈1 (same timestamp) ≈ 0.80
        assert abs(sig["weight"] - 0.80) < 0.02

    @pytest.mark.asyncio
    async def test_revision_skipped_when_history_too_short(self):
        """First-ever obs: no prior in history → revision not emitted."""
        history = [1.65]  # only the just-written value, no prior
        with patch("scripts.db.queries.raw_signals.get_signal_history", new=AsyncMock(return_value=history)):
            scored = await nm.score_influencer_signals(
                "TEST", [_eps_row(1.65)],
                now=datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc),
            )
        assert all(s["signal_type"] != "earnings_estimate_revision" for s in scored)

    @pytest.mark.asyncio
    async def test_revision_skipped_when_prior_is_zero(self):
        """Prior obs == 0 would cause div-by-zero; signal must be skipped."""
        history = [0.0, 1.50]
        with patch("scripts.db.queries.raw_signals.get_signal_history", new=AsyncMock(return_value=history)):
            scored = await nm.score_influencer_signals(
                "TEST", [_eps_row(1.50)],
                now=datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc),
            )
        assert all(s["signal_type"] != "earnings_estimate_revision" for s in scored)

    @pytest.mark.asyncio
    async def test_revision_bearish_on_downward_revision(self):
        history = [2.00, 1.80]  # delta = -10%
        with patch("scripts.db.queries.raw_signals.get_signal_history", new=AsyncMock(return_value=history)):
            scored = await nm.score_influencer_signals(
                "TEST", [_eps_row(1.80)],
                now=datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc),
            )
        revisions = [s for s in scored if s["signal_type"] == "earnings_estimate_revision"]
        assert len(revisions) == 1
        assert revisions[0]["value"] == pytest.approx(-0.10)
        assert revisions[0]["score"] < 10.0

    @pytest.mark.asyncio
    async def test_zscore_path_used_with_enough_delta_history(self):
        """≥45 deltas in history → z-score path engages; telemetry records 'zscore'."""
        # Build 50 raw obs with small consistent positive drift, then a big spike.
        # Deltas in history are ~+1%; current delta is +20% → z >> +3 → ~100 score.
        base = [1.00 + 0.01 * i for i in range(50)]  # ~+1% each step
        history = base + [base[-1] * 1.20]  # final step is +20% — the "current" obs
        nm.reset_scoring_telemetry()

        with patch("scripts.db.queries.raw_signals.get_signal_history", new=AsyncMock(return_value=history)):
            scored = await nm.score_influencer_signals(
                "TEST", [_eps_row(history[-1])],
                now=datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc),
            )

        revisions = [s for s in scored if s["signal_type"] == "earnings_estimate_revision"]
        assert len(revisions) == 1
        assert revisions[0]["score"] > 90.0
        counts = nm._scoring_method_counts.get("earnings_estimate_revision", {})
        assert counts.get("zscore", 0) == 1
        assert counts.get("parametric_fallback", 0) == 0

    @pytest.mark.asyncio
    async def test_parametric_fallback_when_delta_history_too_short(self):
        """Few prior obs → delta_history insufficient for z-score → parametric used."""
        history = [1.00, 1.10]  # only 2 obs → 0 entries in delta_history → parametric
        nm.reset_scoring_telemetry()

        with patch("scripts.db.queries.raw_signals.get_signal_history", new=AsyncMock(return_value=history)):
            await nm.score_influencer_signals(
                "TEST", [_eps_row(1.10)],
                now=datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc),
            )

        counts = nm._scoring_method_counts.get("earnings_estimate_revision", {})
        assert counts.get("parametric_fallback", 0) == 1
        assert counts.get("zscore", 0) == 0
