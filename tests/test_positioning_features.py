"""Tests for positioning research features (Track B3/B4/B6).

Covers the pure z-score math, the point-in-time backfill computation, the
flag gate, the insert_row JSONB pass-through, and — critically — that
research_features can NEVER leak into an API response.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone

import pytest

from pipeline.features import positioning


# ------------------------------------------------------------- pure math --


class TestTrailingZscore:
    def test_basic_z(self):
        prior = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
        z = positioning.trailing_zscore(11.0, prior)
        mean = 5.5
        std = (sum((x - mean) ** 2 for x in prior) / 10) ** 0.5
        assert z == pytest.approx((11.0 - mean) / std, abs=1e-3)

    def test_none_below_min_obs(self):
        assert positioning.trailing_zscore(1.0, [1.0] * 9) is None

    def test_none_on_zero_variance(self):
        assert positioning.trailing_zscore(2.0, [1.0] * 15) is None


class TestShortVolZ:
    def test_uses_trailing_window_only(self):
        # 40 values; only the last WINDOW+1 should matter
        noise = [99.0] * 19          # would wreck the z if included
        series = noise + [0.5] * positioning.WINDOW + [0.6]
        z_full = positioning.short_vol_z_from_series(series)
        z_trim = positioning.short_vol_z_from_series(series[-(positioning.WINDOW + 1):])
        assert z_full == z_trim

    def test_none_on_short_series(self):
        assert positioning.short_vol_z_from_series([0.5]) is None
        assert positioning.short_vol_z_from_series([]) is None

    def test_elevated_ratio_scores_positive(self):
        series = [0.50 + 0.001 * i for i in range(positioning.WINDOW)] + [0.70]
        assert positioning.short_vol_z_from_series(series) > 0


class TestPriorWeekdays:
    def test_20_weekdays_no_weekends_excludes_as_of(self):
        days = positioning.prior_weekdays(date(2026, 7, 22), 20)  # a Wednesday
        assert len(days) == 20
        assert all(d.isoweekday() <= 5 for d in days)
        assert date(2026, 7, 22) not in days
        assert days == sorted(days)
        assert days[-1] == date(2026, 7, 21)


class TestInsiderNetZ:
    def test_none_when_no_activity_at_all(self):
        assert positioning.insider_net_z_from_daily({}, date(2026, 7, 22)) is None

    def test_filing_after_quiet_window_scores_large(self):
        # quiet 20 weekdays, then a 50k-share buy today: needs some baseline
        # variance to compute — one small prior filing provides it
        daily = {date(2026, 7, 6): 100.0, date(2026, 7, 22): 50_000.0}
        z = positioning.insider_net_z_from_daily(daily, date(2026, 7, 22))
        assert z is not None and z > 3

    def test_quiet_today_with_active_window_still_computes(self):
        daily = {date(2026, 7, 6): 10_000.0, date(2026, 7, 13): -5_000.0}
        z = positioning.insider_net_z_from_daily(daily, date(2026, 7, 22))
        assert z is not None  # 0 vs an active baseline is information, not absence


# ---------------------------------------------------------------- gating --


class TestFlagGate:
    def test_disabled_by_default(self, monkeypatch):
        monkeypatch.delenv("ENABLE_POSITIONING_FEATURES", raising=False)
        assert positioning.enabled() is False

    @pytest.mark.parametrize("val,expected", [
        ("1", True), ("true", True), ("yes", True),
        ("0", False), ("false", False), ("", False),
    ])
    def test_flag_values(self, monkeypatch, val, expected):
        monkeypatch.setenv("ENABLE_POSITIONING_FEATURES", val)
        assert positioning.enabled() is expected


# ------------------------------------------------- backfill point-in-time --


class TestBackfillPIT:
    def _ts(self, day, hour=21):
        return datetime(2026, 7, day, hour, tzinfo=timezone.utc)

    def test_signal_after_target_excluded(self):
        from scripts.eval.backfill_features import compute_features_for_ticker

        # Enough short-vol history for a z BEFORE the target...
        sv = [(self._ts(d), 0.50 + 0.001 * d) for d in range(1, 13)]
        # ...plus an extreme value AFTER the target that must not leak in
        sv_future = sv + [(self._ts(14), 99.0)]
        target = [{"id": 42, "timestamp": self._ts(12, hour=23)}]

        out_clean = compute_features_for_ticker(target, sv, [])
        out_future = compute_features_for_ticker(target, sv_future, [])
        assert out_clean == out_future  # the future row changed nothing
        assert out_clean and out_clean[0][0] == 42
        assert "short_vol_z" in out_clean[0][1]

    def test_target_before_any_history_omitted(self):
        from scripts.eval.backfill_features import compute_features_for_ticker

        sv = [(self._ts(d), 0.5) for d in range(10, 15)]
        target = [{"id": 1, "timestamp": self._ts(2)}]
        assert compute_features_for_ticker(target, sv, []) == []


# -------------------------------------------------------- persistence path --


class FakeConn:
    def __init__(self):
        self.calls = []

    async def execute(self, sql, *args):
        self.calls.append((sql, args))


async def test_insert_row_serializes_research_features():
    from scripts.db.queries.sentiment_history import insert_row

    conn = FakeConn()
    await insert_row(
        conn,
        ticker="AAPL", composite_score=55.0,
        market_index=None, narrative_index=None, influencer_index=None,
        macro_index=None, confidence_score=80, confidence_flags=[],
        top_drivers=[], divergence=None, market_as_of=None,
        narrative_as_of=None, influencer_as_of=None, macro_as_of=None,
        timestamp=datetime(2026, 7, 22, tzinfo=timezone.utc),
        research_features={"short_vol_z": 1.23},
    )
    sql, args = conn.calls[0]
    assert "research_features" in sql
    assert json.loads(args[-1]) == {"short_vol_z": 1.23}


async def test_insert_row_null_research_features_by_default():
    from scripts.db.queries.sentiment_history import insert_row

    conn = FakeConn()
    await insert_row(
        conn,
        ticker="AAPL", composite_score=55.0,
        market_index=None, narrative_index=None, influencer_index=None,
        macro_index=None, confidence_score=80, confidence_flags=[],
        top_drivers=[], divergence=None, market_as_of=None,
        narrative_as_of=None, influencer_as_of=None, macro_as_of=None,
        timestamp=datetime(2026, 7, 22, tzinfo=timezone.utc),
    )
    _, args = conn.calls[0]
    assert args[-1] is None


# ------------------------------------------------------ API leak guard (B6) --


class TestResearchFeaturesNeverServed:
    _STATE = {
        "ticker": "AAPL",
        "timestamp": datetime(2026, 7, 22, 15, 0, tzinfo=timezone.utc),
        "composite_score": 55.0,
        "composite_score_raw": 56.0,
        "score_exo": 57.0,
        "confidence": {"score": 80, "flags": []},
        "sub_indices": {"market": {"value": 50.0}, "narrative": None,
                        "influencer": None, "macro": None},
        "top_drivers": [],
        "explanation": "",
        "freshness": {},
        "divergence": "aligned",
        "research_features": {"short_vol_z": 2.5, "insider_net_z": -1.0},
        "narrative_surprise": 61.2,
    }

    def test_not_in_pro_response(self):
        from api.response.assembler import _build_pro

        dump = _build_pro(self._STATE).model_dump()
        flat = json.dumps(dump, default=str)
        assert "research_features" not in flat
        assert "short_vol_z" not in flat
        assert "narrative_surprise" not in flat

    def test_not_in_free_response(self):
        from api.response.assembler import _build_free

        flat = json.dumps(_build_free(self._STATE).model_dump(), default=str)
        assert "research_features" not in flat
        assert "short_vol_z" not in flat

    def test_schema_has_no_research_fields(self):
        from api.response.schemas import FreeTierResponse, ProTierResponse

        for model in (FreeTierResponse, ProTierResponse):
            assert "research_features" not in model.model_fields
            assert "narrative_surprise" not in model.model_fields
