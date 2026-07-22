"""Tests for the options snapshot source (parsing/derivation math, mocked
chains — no network, no yfinance)."""
from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pytest

from pipeline.sources.options import (
    OPTIONS_SIGNAL_TYPES,
    derive_options_signals,
    pick_expiry,
)

NOW = datetime(2026, 7, 22, 21, 20, tzinfo=timezone.utc)


def _chain(strikes, ivs, volumes=None, ois=None):
    n = len(strikes)
    return pd.DataFrame({
        "strike": strikes,
        "impliedVolatility": ivs,
        "volume": volumes if volumes is not None else [100] * n,
        "openInterest": ois if ois is not None else [1000] * n,
    })


# Typical smile around spot=100: puts rich on the downside
SPOT = 100.0
CALLS = _chain([90, 95, 100, 105, 110], [0.32, 0.28, 0.25, 0.23, 0.22],
               volumes=[50, 100, 400, 300, 150], ois=[500, 800, 4000, 3000, 1200])
PUTS = _chain([90, 95, 100, 105, 110], [0.38, 0.33, 0.26, 0.24, 0.23],
              volumes=[300, 500, 350, 80, 20], ois=[3500, 5200, 3800, 700, 150])


class TestPickExpiry:
    def test_nearest_30d_wins(self):
        exps = ["2026-07-25", "2026-08-21", "2026-09-18"]  # 3, 30, 58 dte
        assert pick_expiry(exps, NOW) == "2026-08-21"

    def test_past_and_today_excluded(self):
        assert pick_expiry(["2026-07-22", "2026-07-10"], NOW) is None

    def test_empty_and_malformed(self):
        assert pick_expiry([], NOW) is None
        assert pick_expiry(["not-a-date"], NOW) is None

    def test_tie_and_single(self):
        assert pick_expiry(["2026-08-14"], NOW) == "2026-08-14"  # 23 dte, only option


class TestDerivation:
    def test_pcr_volume(self):
        s = derive_options_signals(CALLS, PUTS, SPOT)
        assert s["pcr_volume"] == pytest.approx(1250 / 1000, abs=1e-4)

    def test_pcr_oi(self):
        s = derive_options_signals(CALLS, PUTS, SPOT)
        assert s["pcr_oi"] == pytest.approx(13350 / 9500, abs=1e-4)

    def test_atm_iv_mean_of_both_legs_at_spot_strike(self):
        s = derive_options_signals(CALLS, PUTS, SPOT)
        assert s["atm_iv_30d"] == pytest.approx((0.25 + 0.26) / 2, abs=1e-4)

    def test_skew_put95_minus_call105_positive_for_normal_smile(self):
        s = derive_options_signals(CALLS, PUTS, SPOT)
        assert s["iv_skew_25d"] == pytest.approx(0.33 - 0.23, abs=1e-4)
        assert s["iv_skew_25d"] > 0

    def test_zero_call_volume_omits_pcr_never_zero_fills(self):
        calls = _chain([95, 100], [0.28, 0.25], volumes=[0, 0], ois=[0, 0])
        puts = _chain([100], [0.26], volumes=[500], ois=[100])
        s = derive_options_signals(calls, puts, SPOT)
        assert "pcr_volume" not in s
        assert "pcr_oi" not in s
        assert "atm_iv_30d" in s  # IV legs still fine (3 distinct sane IVs)

    def test_nan_volumes_treated_as_zero(self):
        calls = _chain([100, 105], [0.25, 0.23], volumes=[np.nan, 200])
        puts = _chain([95, 100], [0.33, 0.26], volumes=[np.nan, 100])
        s = derive_options_signals(calls, puts, SPOT)
        assert s["pcr_volume"] == pytest.approx(100 / 200)

    def test_insane_iv_rejected(self):
        calls = _chain([100], [0.0])       # below floor
        puts = _chain([100], [7.5])        # above cap
        s = derive_options_signals(calls, puts, SPOT)
        assert "atm_iv_30d" not in s
        assert "iv_skew_25d" not in s

    def test_atm_single_sane_leg_ok(self):
        # put side has no usable IVs; ATM comes from the call leg alone
        # (call chain itself shows real dispersion)
        calls = _chain([95, 100, 105], [0.28, 0.25, 0.23])
        puts = _chain([100], [np.nan])
        s = derive_options_signals(calls, puts, SPOT)
        assert s["atm_iv_30d"] == pytest.approx(0.25)

    def test_sparse_chain_far_strikes_omit_skew(self):
        # nearest strikes to the 95/105 targets are 140/60 — past 10% tolerance
        calls = _chain([140, 150], [0.25, 0.22])
        puts = _chain([50, 60], [0.45, 0.40])
        s = derive_options_signals(calls, puts, SPOT)
        assert "iv_skew_25d" not in s

    def test_flat_placeholder_iv_surface_rejected(self):
        # After-hours yfinance serves flat IVs (observed: all puts 0.250007).
        # A flat surface must yield NO IV signals, even though each value is
        # individually "sane".
        calls = _chain([95, 100, 105], [0.250007, 0.250007, 0.250007])
        puts = _chain([95, 100, 105], [0.250007, 0.250007, 0.250007])
        s = derive_options_signals(calls, puts, SPOT)
        assert "atm_iv_30d" not in s
        assert "iv_skew_25d" not in s
        assert "pcr_volume" in s  # volume side unaffected

    def test_one_sided_zero_oi_omits_pcr_oi(self):
        calls = _chain([100], [0.25], ois=[5000])
        puts = _chain([100], [0.30], ois=[0])   # after-hours artifact
        s = derive_options_signals(calls, puts, SPOT)
        assert "pcr_oi" not in s

    def test_second_placeholder_iv_level_rejected(self):
        # observed after-hours placeholder ~0.0156 — below the 5% floor
        calls = _chain([95, 100, 105], [0.0156, 0.0157, 0.0158])
        puts = _chain([95, 100, 105], [0.0156, 0.0157, 0.0158])
        s = derive_options_signals(calls, puts, SPOT)
        assert "atm_iv_30d" not in s
        assert "iv_skew_25d" not in s

    def test_thin_depth_omits_ratios(self):
        # 10 contracts a side is noise, not flow — observed pcr artifacts
        calls = _chain([100], [0.25], volumes=[10], ois=[40])
        puts = _chain([100], [0.30], volumes=[10], ois=[40])
        s = derive_options_signals(calls, puts, SPOT)
        assert "pcr_volume" not in s
        assert "pcr_oi" not in s

    def test_bad_spot_returns_nothing(self):
        assert derive_options_signals(CALLS, PUTS, 0.0) == {}
        assert derive_options_signals(CALLS, PUTS, None) == {}

    def test_empty_chains_return_nothing(self):
        empty = pd.DataFrame()
        assert derive_options_signals(empty, empty, SPOT) == {}


class TestRegistration:
    def test_signal_types_research_retained(self):
        from scripts.db.queries.raw_signals import RESEARCH_RETAIN_SIGNAL_TYPES

        assert set(OPTIONS_SIGNAL_TYPES) <= set(RESEARCH_RETAIN_SIGNAL_TYPES)

    def test_job_registered_2120_weekdays(self):
        from pipeline.scheduler import scheduler

        job = scheduler.get_job("options")
        assert job is not None
        fields = {f.name: str(f) for f in job.trigger.fields}
        assert fields["hour"] == "21"
        assert fields["minute"] == "20"
        assert fields["day_of_week"] == "mon-fri"
