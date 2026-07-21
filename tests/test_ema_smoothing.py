"""
tests/test_ema_smoothing.py

Unit tests for the EMA smoothing function (Sprint 5a, G-C3).

Validates:
- Cold-start behavior (prev_smoothed=None → returns raw)
- Normal EMA update with correct α computation
- Half-life property (after dt = T½, gap closes by exactly 50%)
- Large dt behavior (α → 1, smoothed ≈ raw)
- Zero dt behavior (smoothed unchanged)
- Negative dt clamping (treated as 0)
"""
from __future__ import annotations

import math

import pytest

from pipeline.scoring.ema import compute_ema


# ---------------------------------------------------------------------------
# Cold-start
# ---------------------------------------------------------------------------

class TestEMAColdStart:

    def test_cold_start_returns_raw(self):
        """When prev_smoothed is None, return raw_t unchanged."""
        assert compute_ema(65.0, None, dt_hours=0.5) == 65.0

    def test_cold_start_any_dt(self):
        """Cold-start ignores dt entirely."""
        assert compute_ema(42.0, None, dt_hours=0.0) == 42.0
        assert compute_ema(42.0, None, dt_hours=100.0) == 42.0

    def test_cold_start_any_half_life(self):
        """Cold-start ignores half_life_hours."""
        assert compute_ema(75.0, None, dt_hours=1.0, half_life_hours=24.0) == 75.0


# ---------------------------------------------------------------------------
# Normal update
# ---------------------------------------------------------------------------

class TestEMANormalUpdate:

    def test_basic_update(self):
        """Verify the α formula produces the correct weighted average."""
        raw = 70.0
        prev = 50.0
        dt = 2.0  # hours
        half_life = 4.0

        alpha = 1.0 - 0.5 ** (dt / half_life)
        expected = alpha * raw + (1.0 - alpha) * prev

        result = compute_ema(raw, prev, dt_hours=dt, half_life_hours=half_life)
        assert abs(result - expected) < 1e-10

    def test_30min_scoring_tick(self):
        """Standard 30-min scoring tick: α ≈ 0.0830."""
        raw = 60.0
        prev = 50.0
        dt = 0.5  # 30 min

        alpha = 1.0 - 0.5 ** (0.5 / 4.0)
        expected = alpha * raw + (1.0 - alpha) * prev

        result = compute_ema(raw, prev, dt_hours=dt)
        assert abs(result - expected) < 1e-10
        # α ≈ 0.083, so result ≈ 50.83
        assert 50.0 < result < 51.0

    def test_smoothed_moves_toward_raw(self):
        """Smoothed value always moves in the direction of raw."""
        # raw > prev → smoothed increases
        result = compute_ema(80.0, 50.0, dt_hours=1.0)
        assert 50.0 < result < 80.0

        # raw < prev → smoothed decreases
        result = compute_ema(20.0, 50.0, dt_hours=1.0)
        assert 20.0 < result < 50.0

    def test_raw_equals_prev(self):
        """When raw equals previous smoothed, result is unchanged."""
        result = compute_ema(50.0, 50.0, dt_hours=1.0)
        assert abs(result - 50.0) < 1e-10


# ---------------------------------------------------------------------------
# Half-life property
# ---------------------------------------------------------------------------

class TestEMAHalfLife:

    def test_half_life_closes_50_percent(self):
        """After exactly one half-life, the gap between prev and raw closes by 50%."""
        raw = 100.0
        prev = 0.0
        half_life = 4.0

        result = compute_ema(raw, prev, dt_hours=half_life, half_life_hours=half_life)

        # Gap was 100. After one half-life, 50% closes → result = 50.0
        assert abs(result - 50.0) < 1e-10

    def test_two_half_lives_close_75_percent(self):
        """After 2 half-lives (8h), 75% of the gap closes."""
        raw = 100.0
        prev = 0.0

        result = compute_ema(raw, prev, dt_hours=8.0, half_life_hours=4.0)
        # α = 1 - 0.5^2 = 0.75
        assert abs(result - 75.0) < 1e-10

    def test_three_half_lives_close_875_percent(self):
        """After 3 half-lives (12h), 87.5% of the gap closes."""
        raw = 100.0
        prev = 0.0

        result = compute_ema(raw, prev, dt_hours=12.0, half_life_hours=4.0)
        # α = 1 - 0.5^3 = 0.875
        assert abs(result - 87.5) < 1e-10

    def test_custom_half_life(self):
        """Half-life property holds for non-default half-lives."""
        raw = 80.0
        prev = 40.0
        gap = raw - prev  # 40

        result = compute_ema(raw, prev, dt_hours=2.0, half_life_hours=2.0)
        # One half-life: 50% of gap
        expected = prev + 0.5 * gap
        assert abs(result - expected) < 1e-10


# ---------------------------------------------------------------------------
# Large dt (gap behavior)
# ---------------------------------------------------------------------------

class TestEMALargeDt:

    def test_24h_gap_nearly_resets(self):
        """After 24h (6 half-lives), α ≈ 0.984 — smoothed ≈ raw."""
        raw = 70.0
        prev = 30.0

        result = compute_ema(raw, prev, dt_hours=24.0, half_life_hours=4.0)
        # α = 1 - 0.5^6 = 0.984375
        assert abs(result - raw) < 1.0

    def test_very_large_dt(self):
        """After 100h, smoothed is essentially equal to raw."""
        result = compute_ema(55.0, 10.0, dt_hours=100.0)
        assert abs(result - 55.0) < 1e-4

    def test_gap_never_overshoots(self):
        """Smoothed never overshoots raw, regardless of dt."""
        for dt in [0.1, 0.5, 1.0, 4.0, 8.0, 24.0, 100.0, 1000.0]:
            result = compute_ema(80.0, 40.0, dt_hours=dt)
            assert 40.0 <= result <= 80.0, f"Overshoot at dt={dt}: {result}"


# ---------------------------------------------------------------------------
# Zero dt
# ---------------------------------------------------------------------------

class TestEMAZeroDt:

    def test_zero_dt_returns_prev(self):
        """dt = 0 → α = 0 → smoothed unchanged (returns prev_smoothed)."""
        result = compute_ema(100.0, 50.0, dt_hours=0.0)
        assert result == 50.0

    def test_zero_dt_cold_start(self):
        """dt = 0 with cold-start still returns raw."""
        result = compute_ema(65.0, None, dt_hours=0.0)
        assert result == 65.0


# ---------------------------------------------------------------------------
# Negative dt clamping
# ---------------------------------------------------------------------------

class TestEMANegativeDt:

    def test_negative_dt_clamped_to_zero(self):
        """Negative dt is treated as dt = 0 → smoothed unchanged."""
        result = compute_ema(100.0, 50.0, dt_hours=-1.0)
        assert result == 50.0

    def test_negative_dt_cold_start(self):
        """Negative dt with cold-start still returns raw."""
        result = compute_ema(65.0, None, dt_hours=-5.0)
        assert result == 65.0


class TestHalfLifeEnvOverride:
    """The default half-life is env-tunable (EMA_HALF_LIFE_HOURS) but must
    stay 4.0 unless explicitly overridden — the production default only
    changes via the eval-gated experiment (nowcasting plan, Phase 4)."""

    def test_default_is_four_hours(self):
        import pipeline.scoring.ema as ema
        assert ema._HALF_LIFE_HOURS == 4.0

    def test_env_override_binds_at_import(self, monkeypatch):
        import importlib
        import pipeline.scoring.ema as ema
        monkeypatch.setenv("EMA_HALF_LIFE_HOURS", "2.0")
        try:
            importlib.reload(ema)
            assert ema._HALF_LIFE_HOURS == 2.0
            # α after one half-life must now close 50% at dt=2h
            assert ema.compute_ema(100.0, 0.0, 2.0) == pytest.approx(50.0)
        finally:
            monkeypatch.delenv("EMA_HALF_LIFE_HOURS")
            importlib.reload(ema)
            assert ema._HALF_LIFE_HOURS == 4.0
