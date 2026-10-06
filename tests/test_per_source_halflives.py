"""
tests/test_per_source_halflives.py — Verify per-source half-life time decay.

Sprint 4 (G-S1): Replaces uniform 48h half-life with per-layer/source values.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pipeline.features.normalize import (
    _HALF_LIFE_OVERRIDE,
    _LAYER_HALF_LIFE_H,
    _build,
    _get_half_life,
    _time_weight,
)

# ===========================================================================
# _get_half_life lookup
# ===========================================================================

class TestGetHalfLife:

    def test_market_default(self):
        assert _get_half_life("market", "yfinance") == 1.0

    def test_market_polygon(self):
        assert _get_half_life("market", "polygon") == 1.0

    def test_narrative_default(self):
        assert _get_half_life("narrative", "alpha_vantage") == 12.0

    def test_narrative_finnhub(self):
        assert _get_half_life("narrative", "finnhub") == 12.0

    def test_influencer_default(self):
        """Influencer default (analyst signals) = 72h."""
        assert _get_half_life("influencer", "finnhub") == 72.0

    def test_macro_default(self):
        assert _get_half_life("macro", "computed") == 336.0

    def test_unknown_layer_fallback(self):
        """Unknown layer falls back to 48h."""
        assert _get_half_life("unknown", "yfinance") == 48.0


# ===========================================================================
# _time_weight with half-life parameter
# ===========================================================================

class TestTimeWeight:

    def test_fresh_signal_weight_one(self):
        now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
        w = _time_weight(now, now, half_life_h=1.0)
        assert abs(w - 1.0) < 1e-9

    def test_one_halflife_old_is_half(self):
        """Signal exactly one half-life old → weight = 0.5."""
        now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
        ts = now - timedelta(hours=1)
        w = _time_weight(ts, now, half_life_h=1.0)
        assert abs(w - 0.5) < 1e-6

    def test_two_halflives_old_is_quarter(self):
        now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
        ts = now - timedelta(hours=2)
        w = _time_weight(ts, now, half_life_h=1.0)
        assert abs(w - 0.25) < 1e-6

    def test_market_60min_halflife(self):
        """Market signal 60 min old → weight = 0.5."""
        now = datetime(2026, 5, 8, 15, 0, tzinfo=timezone.utc)
        ts = now - timedelta(minutes=60)
        w = _time_weight(ts, now, half_life_h=1.0)
        assert abs(w - 0.5) < 1e-6

    def test_market_30min_old(self):
        """Market signal 30 min old → weight ≈ 0.71 (one scoring tick)."""
        now = datetime(2026, 5, 8, 15, 0, tzinfo=timezone.utc)
        ts = now - timedelta(minutes=30)
        w = _time_weight(ts, now, half_life_h=1.0)
        expected = 0.5 ** 0.5  # ≈ 0.7071
        assert abs(w - expected) < 1e-4

    def test_narrative_12h_halflife(self):
        """News signal 12h old → weight = 0.5."""
        now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
        ts = now - timedelta(hours=12)
        w = _time_weight(ts, now, half_life_h=12.0)
        assert abs(w - 0.5) < 1e-6

    def test_macro_14d_halflife(self):
        """Macro signal 14 days old → weight = 0.5."""
        now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
        ts = now - timedelta(days=14)
        w = _time_weight(ts, now, half_life_h=336.0)
        assert abs(w - 0.5) < 1e-6

    def test_overnight_market_decay(self):
        """Market signal from 17h ago (overnight) → near-zero weight."""
        now = datetime(2026, 5, 8, 14, 0, tzinfo=timezone.utc)
        ts = now - timedelta(hours=17)  # previous day 21:00
        w = _time_weight(ts, now, half_life_h=1.0)
        # 0.5^17 ≈ 7.6e-6
        assert w < 0.001

    def test_default_halflife_48h(self):
        """Default half_life_h=48 maintained for backward compat."""
        now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
        ts = now - timedelta(hours=48)
        w = _time_weight(ts, now)  # uses default 48.0
        assert abs(w - 0.5) < 1e-6


# ===========================================================================
# _build uses per-source half-lives via layer parameter
# ===========================================================================

class TestBuildWithHalfLives:

    def test_market_signal_uses_1h_halflife(self):
        """Market signal 60 min old should have weight ≈ 0.5 × source_weight."""
        now = datetime(2026, 5, 8, 15, 0, tzinfo=timezone.utc)
        ts = now - timedelta(minutes=60)
        sig = _build("rsi_14", 55.0, 50.0, "yfinance", ts, "market", "AAPL", now)
        # source_weight(yfinance) = 0.9, time_weight(1h, hl=1h) = 0.5
        expected_w = 0.9 * 0.5
        assert abs(sig["weight"] - expected_w) < 0.01

    def test_narrative_signal_uses_12h_halflife(self):
        """Narrative signal 12h old → weight ≈ 0.5 × source_weight."""
        now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
        ts = now - timedelta(hours=12)
        sig = _build("finbert_sentiment", 0.3, 65.0, "alpha_vantage", ts,
                      "narrative", "AAPL", now)
        expected_w = 0.75 * 0.5  # AV source weight = 0.75 (Sprint A)
        assert abs(sig["weight"] - expected_w) < 0.01

    def test_influencer_insider_uses_168h_regardless_of_source(self):
        """Insider transaction 7 days old → weight ≈ 0.5 × channel_weight (1.00).

        Sprint P3.4 removed the `("influencer", "sec_edgar")` half-life override;
        168h now comes from the signal-type-keyed `_INFLUENCER_SIGNAL_HALF_LIFE_H`
        table (P3.1 I4) and applies for both Finnhub and SEC EDGAR insider rows.
        """
        now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
        ts = now - timedelta(days=7)
        sig = _build("insider_net_shares", 1000, 55.0, "finnhub", ts,
                      "influencer", "AAPL", now)
        # Sprint P3.1 I1: insider channel weight = 1.00.
        # w_author × w_conf = 1.0 × 1.0 (Sprint P3.1 I7/I15 scaffolds).
        expected_w = 1.0 * 0.5
        assert abs(sig["weight"] - expected_w) < 0.01

    def test_influencer_analyst_consensus_uses_paper_weight(self):
        """Sprint P3.1 I2: analyst_buy_pct uses paper channel weight 0.85 regardless of provider."""
        now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
        ts = now - timedelta(days=3)
        sig = _build("analyst_buy_pct", 0.7, 60.0, "finnhub", ts,
                      "influencer", "AAPL", now)
        # P3.1 I2: analyst consensus channel weight = 0.85 (was 0.65 via finnhub source).
        # Half-life: 72h layer default; t=3d → 0.5 decay.
        expected_w = 0.85 * 0.5
        assert abs(sig["weight"] - expected_w) < 0.01

    def test_macro_signal_uses_336h(self):
        """Macro signal 14 days old → weight ≈ 0.5 × source_weight."""
        now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
        ts = now - timedelta(days=14)
        sig = _build("vix", 22.0, 50.0, "computed", ts, "macro", "_MACRO_", now)
        expected_w = 0.85 * 0.5  # computed source weight = 0.85
        assert abs(sig["weight"] - expected_w) < 0.01


# ===========================================================================
# Layer half-life values match paper spec
# ===========================================================================

class TestHalfLifeValues:

    def test_market_is_60_min(self):
        assert _LAYER_HALF_LIFE_H["market"] == 1.0

    def test_narrative_is_12h(self):
        assert _LAYER_HALF_LIFE_H["narrative"] == 12.0

    def test_influencer_default_is_72h(self):
        assert _LAYER_HALF_LIFE_H["influencer"] == 72.0

    def test_macro_is_336h(self):
        assert _LAYER_HALF_LIFE_H["macro"] == 336.0

    def test_half_life_override_empty_after_p3_4(self):
        """Sprint P3.4: `("influencer", "sec_edgar")` override removed; dict now empty.

        The 168h insider half-life is supplied by the signal-type-keyed
        `_INFLUENCER_SIGNAL_HALF_LIFE_H` table (P3.1 I4), not by a source override.
        """
        assert _HALF_LIFE_OVERRIDE == {}


# ===========================================================================
# Sprint P3.1 — Influencer signal-channel weights, half-lives, w_author scaffold
# ===========================================================================

class TestInfluencerSignalChannelWeights:
    """Sprint P3.1 — paper §Event-Level Weighting: w_src keyed by signal channel."""

    def test_insider_channel_weight_is_100(self):
        """I1: insider transactions get w_src = 1.00 regardless of provider.

        Second assertion uses a deliberately unknown source string to confirm
        the channel-weight table wins even when the source is not in
        `_SOURCE_WEIGHTS`. (Pre-Phase-5 this was asserted with `"sec_edgar"`;
        that source was retracted when Sprint C — EDGAR 8-K — was abandoned.)
        """
        from pipeline.features.normalize import _get_signal_weight
        assert _get_signal_weight("influencer", "finnhub", "insider_net_shares") == 1.00
        assert _get_signal_weight("influencer", "unknown_provider", "insider_net_shares") == 1.00

    def test_analyst_consensus_weight_is_085(self):
        """I2: analyst consensus channel weight = 0.85."""
        from pipeline.features.normalize import _get_signal_weight
        assert _get_signal_weight("influencer", "finnhub", "analyst_buy_pct") == 0.85

    def test_analyst_target_price_weight_is_085(self):
        """I2: analyst target price channel weight = 0.85."""
        from pipeline.features.normalize import _get_signal_weight
        assert _get_signal_weight("influencer", "yfinance", "analyst_target_price") == 0.85
        assert _get_signal_weight("influencer", "finnhub", "analyst_target_price") == 0.85

    def test_earnings_revision_weight_is_080(self):
        """I3 (rolled into P3.1): earnings revision channel weight = 0.80."""
        from pipeline.features.normalize import _get_signal_weight
        assert _get_signal_weight("influencer", "yfinance", "earnings_estimate_revision") == 0.80

    def test_non_influencer_falls_through_to_source_weight(self):
        """Channel-weight override applies only to the influencer layer."""
        from pipeline.features.normalize import _SOURCE_WEIGHTS, _get_signal_weight
        # Market layer with a name that exists in influencer table: still uses source weight.
        assert _get_signal_weight("market", "yfinance", "insider_net_shares") == _SOURCE_WEIGHTS["yfinance"]
        assert _get_signal_weight("narrative", "alpha_vantage", "analyst_buy_pct") == _SOURCE_WEIGHTS["alpha_vantage"]

    def test_unknown_influencer_signal_type_falls_through(self):
        """Influencer signal not in channel table falls through to source weight."""
        from pipeline.features.normalize import _SOURCE_WEIGHTS, _get_signal_weight
        assert _get_signal_weight("influencer", "finnhub", "unknown_signal") == _SOURCE_WEIGHTS["finnhub"]


class TestInfluencerInsiderHalfLife:
    """Sprint P3.1 I4: insider transactions get 168h half-life regardless of provider."""

    def test_insider_finnhub_uses_168h(self):
        """Finnhub-sourced insider rows now get 168h, not 72h layer default."""
        assert _get_half_life("influencer", "finnhub", "insider_net_shares") == 168.0

    def test_insider_unknown_source_still_uses_168h(self):
        """Signal-type routing applies regardless of provider — insider rows
        from any source land at 168h via `_INFLUENCER_SIGNAL_HALF_LIFE_H`.

        Pre-Phase-5 this asserted the same property using `"sec_edgar"`; that
        source label was retracted when Sprint C (EDGAR 8-K) was abandoned.
        """
        assert _get_half_life("influencer", "unknown_provider", "insider_net_shares") == 168.0

    def test_analyst_buy_pct_still_72h(self):
        """Analyst consensus uses 72h layer default."""
        assert _get_half_life("influencer", "finnhub", "analyst_buy_pct") == 72.0

    def test_analyst_target_still_72h(self):
        """Analyst target price uses 72h layer default."""
        assert _get_half_life("influencer", "yfinance", "analyst_target_price") == 72.0

    def test_insider_finnhub_build_weight_uses_168h(self):
        """End-to-end: _build for Finnhub insider with 7d age → ≈ 0.5 × 1.0 channel_weight."""
        now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
        ts = now - timedelta(days=7)
        sig = _build("insider_net_shares", 1000, 55.0, "finnhub", ts,
                     "influencer", "AAPL", now)
        # Channel weight 1.00 (P3.1 I1), half-life 168h (P3.1 I4), 7d age → time decay 0.5
        expected_w = 1.0 * 0.5
        assert abs(sig["weight"] - expected_w) < 0.01


class TestInfluencerEventWeightScaffolds:
    """Sprint P3.1 I7 + I15: w_author and w_conf scaffolds wired into _build()."""

    def test_w_author_constant_is_one(self):
        """w_author = 1.00 scaffold; role-based hierarchy deferred to Future Additions."""
        from pipeline.features.normalize import _INFLUENCER_W_AUTHOR
        assert _INFLUENCER_W_AUTHOR == 1.0

    def test_w_conf_constant_is_one(self):
        """w_conf = 1.00 explicitly for non-textual influencer signals (paper)."""
        from pipeline.features.normalize import _INFLUENCER_W_CONF
        assert _INFLUENCER_W_CONF == 1.0

    def test_build_applies_scaffolds_to_influencer_layer(self):
        """w_author and w_conf are multiplied into weight for influencer layer."""
        # With both scaffolds at 1.0 the numeric weight is unchanged; this test
        # asserts the scaffolds *are present* by monkey-patching one to a non-unity
        # value and confirming the weight scales.
        import pipeline.features.normalize as nm
        now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
        ts = now  # zero age → time decay 1.0
        baseline = nm._build("analyst_buy_pct", 0.7, 60.0, "finnhub", ts,
                             "influencer", "AAPL", now)
        original = nm._INFLUENCER_W_AUTHOR
        try:
            nm._INFLUENCER_W_AUTHOR = 0.5
            scaled = nm._build("analyst_buy_pct", 0.7, 60.0, "finnhub", ts,
                               "influencer", "AAPL", now)
        finally:
            nm._INFLUENCER_W_AUTHOR = original
        assert abs(scaled["weight"] - baseline["weight"] * 0.5) < 1e-6

    def test_build_does_not_apply_scaffolds_to_other_layers(self):
        """Market/narrative/macro layers do not multiply w_author/w_conf."""
        import pipeline.features.normalize as nm
        now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
        ts = now
        baseline = nm._build("rsi_14", 55.0, 50.0, "yfinance", ts, "market", "AAPL", now)
        original = nm._INFLUENCER_W_AUTHOR
        try:
            nm._INFLUENCER_W_AUTHOR = 0.5
            unchanged = nm._build("rsi_14", 55.0, 50.0, "yfinance", ts, "market", "AAPL", now)
        finally:
            nm._INFLUENCER_W_AUTHOR = original
        assert baseline["weight"] == unchanged["weight"]
