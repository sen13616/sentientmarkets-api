"""
tests/test_scoring.py

Unit tests for the Block 6 composite scoring and confidence modules.
All functions are pure (no DB / async) so these are plain pytest tests.
"""

from datetime import datetime, timedelta, timezone

from pipeline.confidence.scorer import compute_confidence
from pipeline.confidence.staleness import check_staleness, stale_sources
from pipeline.scoring.composite import LAYER_WEIGHTS, compute_composite
from pipeline.scoring.divergence import compute_divergence

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _si(value: float):
    """Lightweight stand-in for SubIndexResult with a .value attribute."""

    class _R:
        def __init__(self, v):
            self.value = v

    return _R(value)


# ===========================================================================
# composite.py
# ===========================================================================


class TestComputeComposite:
    def test_all_four_layers_present(self):
        sub = {
            "market": _si(70.0),
            "narrative": _si(60.0),
            "influencer": _si(65.0),
            "macro": _si(55.0),
        }
        r = compute_composite(sub)
        expected = 0.35 * 70 + 0.30 * 60 + 0.25 * 65 + 0.10 * 55  # 64.25
        assert abs(r.score - expected) < 1e-4
        assert r.missing_layers == []
        assert abs(sum(r.weights_used.values()) - 1.0) < 1e-9

    def test_weights_sum_to_one_with_all_layers(self):
        sub = {k: _si(50.0) for k in LAYER_WEIGHTS}
        r = compute_composite(sub)
        assert abs(sum(r.weights_used.values()) - 1.0) < 1e-9
        # All neutral → composite = 50
        assert abs(r.score - 50.0) < 1e-4

    def test_one_layer_missing_macro(self):
        sub = {
            "market": _si(70.0),
            "narrative": _si(60.0),
            "influencer": _si(65.0),
            "macro": None,
        }
        r = compute_composite(sub)
        assert r.missing_layers == ["macro"]
        # Effective weights: /0.90
        total_present = 0.35 + 0.30 + 0.25
        expected = (0.35 * 70 + 0.30 * 60 + 0.25 * 65) / total_present
        assert abs(r.score - expected) < 1e-4
        assert abs(sum(r.weights_used.values()) - 1.0) < 1e-9

    def test_one_layer_missing_market(self):
        sub = {
            "market": None,
            "narrative": _si(60.0),
            "influencer": _si(65.0),
            "macro": _si(55.0),
        }
        r = compute_composite(sub)
        assert r.missing_layers == ["market"]
        total_present = 0.30 + 0.25 + 0.10
        expected = (0.30 * 60 + 0.25 * 65 + 0.10 * 55) / total_present
        assert abs(r.score - expected) < 1e-4
        assert abs(sum(r.weights_used.values()) - 1.0) < 1e-9

    def test_two_layers_missing(self):
        sub = {
            "market": _si(70.0),
            "narrative": _si(60.0),
            "influencer": None,
            "macro": None,
        }
        r = compute_composite(sub)
        assert set(r.missing_layers) == {"influencer", "macro"}
        total_present = 0.35 + 0.30
        expected = (0.35 * 70 + 0.30 * 60) / total_present
        assert abs(r.score - expected) < 1e-4
        assert abs(sum(r.weights_used.values()) - 1.0) < 1e-9

    def test_three_layers_missing(self):
        sub = {
            "market": _si(80.0),
            "narrative": None,
            "influencer": None,
            "macro": None,
        }
        r = compute_composite(sub)
        assert len(r.missing_layers) == 3
        # Only market present → weight = 1.0 → score = 80
        assert abs(r.score - 80.0) < 1e-4

    def test_all_layers_missing_returns_neutral(self):
        sub = {k: None for k in LAYER_WEIGHTS}
        r = compute_composite(sub)
        assert r.score == 50.0
        assert r.weights_used == {}

    def test_accepts_plain_float_values(self):
        """compute_composite must also work with raw float values (not just SubIndexResult)."""
        sub = {"market": 70.0, "narrative": 60.0, "influencer": None, "macro": None}
        r = compute_composite(sub)
        total = 0.35 + 0.30
        expected = (0.35 * 70 + 0.30 * 60) / total
        assert abs(r.score - expected) < 1e-4


# ===========================================================================
# divergence.py
# ===========================================================================


class TestComputeDivergence:
    def test_aligned_small_spread(self):
        # spread = 65 − 58 = 7  →  "aligned"
        result, score = compute_divergence(
            {"market": 60, "narrative": 65, "influencer": 62, "macro": 58},
            composite=61.0,
        )
        assert result.flag == "aligned"
        assert result.spread == 7.0
        assert not result.cap_applied
        assert score == 61.0

    def test_moderate_divergence(self):
        # spread = 70 − 45 = 25  →  "moderate_divergence"
        result, score = compute_divergence(
            {"market": 70, "narrative": 45, "influencer": 60, "macro": 55},
            composite=58.0,
        )
        assert result.flag == "moderate_divergence"
        assert result.spread == 25.0
        assert not result.cap_applied
        assert score == 58.0  # no cap

    def test_high_divergence_no_cap(self):
        # spread = 85 − 40 = 45  →  "high_divergence"
        # cap check: any > 85? 85 is NOT > 85  →  no cap
        result, score = compute_divergence(
            {"market": 85, "narrative": 40, "influencer": 50, "macro": 45},
            composite=57.0,
        )
        assert result.flag == "high_divergence"
        assert result.spread == 45.0
        assert not result.cap_applied
        assert score == 57.0

    def test_cap_applied_when_one_above_85_one_below_30(self):
        # 90 > 85 ✓  and  25 < 30 ✓  →  cap at 75
        result, score = compute_divergence(
            {"market": 90, "narrative": 25, "influencer": 60, "macro": 50},
            composite=80.0,
        )
        assert result.cap_applied
        assert score == 75.0

    def test_cap_not_applied_when_no_layer_below_30(self):
        # 90 > 85 ✓  but  35 is NOT < 30  →  no cap
        result, score = compute_divergence(
            {"market": 90, "narrative": 35, "influencer": 60},
            composite=65.0,
        )
        assert not result.cap_applied
        assert score == 65.0

    def test_cap_not_applied_when_no_layer_above_85(self):
        # 20 < 30 ✓  but  75 is NOT > 85  →  no cap
        result, score = compute_divergence(
            {"market": 75, "narrative": 20},
            composite=50.0,
        )
        assert not result.cap_applied
        assert score == 50.0

    def test_cap_already_below_75_stays_untouched(self):
        # Cap condition met, but composite < 75 → stays the same
        result, score = compute_divergence(
            {"market": 90, "narrative": 20},
            composite=55.0,
        )
        assert result.cap_applied
        assert score == 55.0  # min(55, 75) = 55

    def test_single_layer_no_spread(self):
        result, score = compute_divergence({"market": 70.0}, composite=70.0)
        assert result.spread == 0.0
        assert result.flag == "aligned"
        assert not result.cap_applied

    def test_spread_boundary_exactly_20(self):
        # spread == 20 is NOT > 20  →  "aligned"
        result, _ = compute_divergence({"market": 70, "narrative": 50}, composite=60.0)
        assert result.spread == 20.0
        assert result.flag == "aligned"

    def test_spread_boundary_exactly_40(self):
        # spread == 40 is NOT > 40  →  "moderate_divergence"
        result, _ = compute_divergence({"market": 80, "narrative": 40}, composite=60.0)
        assert result.spread == 40.0
        assert result.flag == "moderate_divergence"


# ===========================================================================
# staleness.py
# ===========================================================================


class TestCheckStaleness:
    def _now(self):
        # Friday 16:00 UTC — firmly within US market hours (14:30–21:00)
        # so the 90-minute intraday threshold is active for market staleness.
        return datetime(2026, 4, 24, 16, 0, 0, tzinfo=timezone.utc)

    def test_all_fresh(self):
        now = self._now()
        as_of = {
            "market": now - timedelta(minutes=30),
            "news": now - timedelta(hours=2),
            "analyst": now - timedelta(days=1),
            "insider": now - timedelta(days=10),
            "macro": now - timedelta(hours=12),
        }
        result = check_staleness(as_of, now=now)
        assert not any(result.values())

    def test_market_stale_at_91_minutes(self):
        now = self._now()
        as_of = {
            "market": now - timedelta(minutes=91),  # > 90 min threshold
            "news": now - timedelta(hours=2),
            "analyst": now - timedelta(days=1),
            "insider": now - timedelta(days=10),
            "macro": now - timedelta(hours=12),
        }
        result = check_staleness(as_of, now=now)
        assert result["market"] is True
        assert result["news"] is False

    def test_news_stale_at_7_hours(self):
        now = self._now()
        as_of = {
            "market": now - timedelta(minutes=30),
            "news": now - timedelta(hours=7),  # > 6 hr threshold
            "analyst": now - timedelta(days=1),
            "insider": now - timedelta(days=10),
            "macro": now - timedelta(hours=12),
        }
        result = check_staleness(as_of, now=now)
        assert result["news"] is True
        assert result["market"] is False

    def test_insider_stale_at_31_days(self):
        now = self._now()
        as_of = {
            "market": now - timedelta(minutes=10),
            "news": now - timedelta(hours=1),
            "analyst": now - timedelta(days=1),
            "insider": now - timedelta(days=31),  # > 30 day threshold
            "macro": now - timedelta(hours=1),
        }
        result = check_staleness(as_of, now=now)
        assert result["insider"] is True
        assert result["analyst"] is False

    def test_none_timestamp_is_stale(self):
        now = self._now()
        result = check_staleness({"market": None, "news": now - timedelta(hours=1)}, now=now)
        assert result["market"] is True
        assert result["news"] is False

    def test_missing_key_not_in_result(self):
        # Only keys present in STALENESS_THRESHOLDS are returned
        now = self._now()
        result = check_staleness({"market": now - timedelta(minutes=1)}, now=now)
        assert "market" in result
        # Missing keys treated as stale
        assert result.get("news", True) is True

    def test_stale_sources_helper_returns_list(self):
        now = self._now()
        as_of = {
            "market": now - timedelta(minutes=91),  # stale
            "news": now - timedelta(hours=1),  # fresh
            "analyst": now - timedelta(days=4),  # stale (> 3 days)
            "insider": now - timedelta(days=5),  # fresh
            "macro": now - timedelta(hours=1),  # fresh
        }
        result = stale_sources(as_of, now=now)
        assert "market" in result
        assert "analyst" in result
        assert "news" not in result

    def test_exact_threshold_is_not_stale(self):
        # Boundary: exactly at threshold is NOT stale (> not >=)
        now = self._now()
        as_of = {
            "market": now - timedelta(minutes=90),  # == threshold, not stale
        }
        result = check_staleness(as_of, now=now)
        assert result["market"] is False


# ===========================================================================
# scorer.py
# ===========================================================================


class TestComputeConfidence:
    def test_perfect_score(self):
        r = compute_confidence(
            missing_layers=[], stale_sources=[], n_signals=10, divergence_flag="aligned"
        )
        assert r.score == 100
        assert r.flags == []

    def test_one_missing_layer(self):
        r = compute_confidence(
            missing_layers=["macro"], stale_sources=[], n_signals=10, divergence_flag="aligned"
        )
        assert r.score == 85  # 100 − 15
        assert "missing_layer:macro" in r.flags

    def test_two_missing_layers(self):
        r = compute_confidence(
            missing_layers=["macro", "narrative"],
            stale_sources=[],
            n_signals=10,
            divergence_flag="aligned",
        )
        assert r.score == 70  # 100 − 15 − 15
        assert "missing_layer:macro" in r.flags
        assert "missing_layer:narrative" in r.flags

    def test_stale_data_penalty(self):
        r = compute_confidence(
            missing_layers=[], stale_sources=["market"], n_signals=10, divergence_flag="aligned"
        )
        assert r.score == 90  # 100 − 10
        assert "stale:market" in r.flags

    def test_two_stale_sources(self):
        r = compute_confidence(
            missing_layers=[],
            stale_sources=["market", "news"],
            n_signals=10,
            divergence_flag="aligned",
        )
        assert r.score == 80  # 100 − 10 − 10

    def test_low_signal_volume(self):
        r = compute_confidence(
            missing_layers=[], stale_sources=[], n_signals=3, divergence_flag="aligned"
        )
        assert r.score == 80  # 100 − 20
        assert "low_signal_volume" in r.flags

    def test_exactly_5_signals_no_penalty(self):
        r = compute_confidence(
            missing_layers=[], stale_sources=[], n_signals=5, divergence_flag="aligned"
        )
        assert r.score == 100
        assert "low_signal_volume" not in r.flags

    def test_4_signals_triggers_penalty(self):
        r = compute_confidence(
            missing_layers=[], stale_sources=[], n_signals=4, divergence_flag="aligned"
        )
        assert r.score == 80  # 100 − 20

    def test_high_divergence_penalty(self):
        r = compute_confidence(
            missing_layers=[], stale_sources=[], n_signals=10, divergence_flag="high_divergence"
        )
        assert r.score == 85  # 100 − 15
        assert "high_divergence" in r.flags

    def test_moderate_divergence_no_penalty(self):
        r = compute_confidence(
            missing_layers=[], stale_sources=[], n_signals=10, divergence_flag="moderate_divergence"
        )
        assert r.score == 100
        assert "high_divergence" not in r.flags

    def test_penalties_accumulate(self):
        r = compute_confidence(
            missing_layers=["macro", "influencer"],  # −15 × 2 = −30
            stale_sources=["market", "news"],  # −10 × 2 = −20
            n_signals=2,  # −20
            divergence_flag="high_divergence",  # −15
        )
        assert r.score == 15  # 100 − 30 − 20 − 20 − 15

    def test_clips_at_zero(self):
        r = compute_confidence(
            missing_layers=["market", "narrative", "influencer", "macro"],  # −60
            stale_sources=["market", "news", "analyst", "insider", "macro"],  # −50
            n_signals=0,  # −20
            divergence_flag="high_divergence",  # −15
        )
        # 100 − 60 − 50 − 20 − 15 = −45  →  clipped to 0
        assert r.score == 0

    def test_score_is_integer(self):
        r = compute_confidence(
            missing_layers=["macro"], stale_sources=[], n_signals=10, divergence_flag="aligned"
        )
        assert isinstance(r.score, int)

    def test_flags_are_strings(self):
        r = compute_confidence(
            missing_layers=["macro"],
            stale_sources=["market"],
            n_signals=2,
            divergence_flag="high_divergence",
        )
        assert all(isinstance(f, str) for f in r.flags)
