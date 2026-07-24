"""
tests/test_signal_staleness.py

Unit tests for per-signal-type staleness rules.

All tests are pure and synchronous — no DB, Redis, or async required.
Dates use April 2026 where:
    Fri 2026-04-24 | Sat 2026-04-25 | Sun 2026-04-26
    Mon 2026-04-27 | Tue 2026-04-28 | Wed 2026-04-29
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from pipeline.confidence.staleness import (
    SignalStalenessRule,
    _DEFAULT_SIGNAL_RULE,
    filter_stale_signals,
    is_market_hours,
    market_lookback_since,
    signal_is_stale,
)


def _utc(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


# ──────────────────────────────────────────────────────────────────────────────
# Required test cases (a)–(e)
# ──────────────────────────────────────────────────────────────────────────────

class TestRequiredCases:
    """Test cases (a)–(e) specified in the requirements."""

    def test_a_sunday_afternoon_friday_close_is_fresh(self):
        """(a) Sunday afternoon: Friday EOD yf_close is fresh."""
        now = _utc(2026, 4, 26, 18, 0)       # Sunday 18:00 UTC
        assert now.isoweekday() == 7
        ts = _utc(2026, 4, 24, 21, 15)        # Friday 21:15 UTC (EOD job)
        assert signal_is_stale("yf_close", ts, now) is False

    def test_b_market_hours_recent_signal_fresh(self):
        """(b) Tuesday 15:00 UTC (market open): yf_close from 14:35 is fresh."""
        now = _utc(2026, 4, 28, 15, 0)        # Tuesday 15:00 UTC
        assert is_market_hours(now) is True
        ts = _utc(2026, 4, 28, 14, 35)        # 25 min ago — within 30-min threshold
        assert signal_is_stale("yf_close", ts, now) is False

    def test_c_market_hours_prior_day_signal_stale(self):
        """(c) Tuesday 15:00 UTC: yf_close from Monday 20:55 is stale."""
        now = _utc(2026, 4, 28, 15, 0)        # Tuesday 15:00 UTC
        assert is_market_hours(now) is True
        ts = _utc(2026, 4, 27, 20, 55)        # Monday 20:55 UTC (~18 h ago)
        assert signal_is_stale("yf_close", ts, now) is True

    def test_d_bid_ask_always_stale_outside_hours(self):
        """(d) Saturday: bid_ask_spread_bps from Friday 20:59 is stale (special-cased)."""
        now = _utc(2026, 4, 25, 10, 0)        # Saturday 10:00 UTC
        ts = _utc(2026, 4, 24, 20, 59)        # Friday 20:59 UTC (just before close)
        assert signal_is_stale("bid_ask_spread_bps", ts, now) is True

    def test_e_holiday_prior_close_is_fresh(self):
        """(e) Wednesday before market open (simulates a holiday): prior close is fresh.

        Holiday detection requires a calendar which is out of scope.  This test
        uses a pre-open time (08:00 UTC) where is_market_hours returns False,
        which exercises the identical off-hours freshness logic.
        """
        now = _utc(2026, 4, 29, 8, 0)         # Wednesday 08:00 UTC (pre-open)
        assert is_market_hours(now) is False
        ts = _utc(2026, 4, 28, 21, 10)        # Tuesday 21:10 UTC (post-close)
        assert signal_is_stale("yf_close", ts, now) is False


# ──────────────────────────────────────────────────────────────────────────────
# signal_is_stale — additional coverage
# ──────────────────────────────────────────────────────────────────────────────

class TestSignalIsStaleDuringMarketHours:
    """During market hours, per-type max_age_minutes applies."""

    def test_yf_close_30_min_threshold(self):
        now = _utc(2026, 4, 28, 16, 0)  # Tuesday 16:00 UTC
        assert signal_is_stale("yf_close", now - timedelta(minutes=29), now) is False
        assert signal_is_stale("yf_close", now - timedelta(minutes=31), now) is True

    def test_rsi_14_60_min_threshold(self):
        now = _utc(2026, 4, 28, 16, 0)
        assert signal_is_stale("rsi_14", now - timedelta(minutes=59), now) is False
        assert signal_is_stale("rsi_14", now - timedelta(minutes=61), now) is True

    def test_order_flow_30_min_threshold(self):
        now = _utc(2026, 4, 28, 16, 0)
        assert signal_is_stale("order_flow_imbalance", now - timedelta(minutes=29), now) is False
        assert signal_is_stale("order_flow_imbalance", now - timedelta(minutes=31), now) is True

    def test_buy_pressure_30_min_threshold(self):
        now = _utc(2026, 4, 28, 16, 0)
        assert signal_is_stale("buy_pressure", now - timedelta(minutes=29), now) is False
        assert signal_is_stale("buy_pressure", now - timedelta(minutes=31), now) is True

    def test_sell_pressure_30_min_threshold(self):
        now = _utc(2026, 4, 28, 16, 0)
        assert signal_is_stale("sell_pressure", now - timedelta(minutes=29), now) is False
        assert signal_is_stale("sell_pressure", now - timedelta(minutes=31), now) is True

    def test_bid_ask_spread_bps_30_min_threshold(self):
        """During market hours, bid_ask_spread_bps uses the 30-min threshold
        (always_stale_outside only kicks in outside hours)."""
        now = _utc(2026, 4, 28, 16, 0)
        assert signal_is_stale("bid_ask_spread_bps", now - timedelta(minutes=29), now) is False
        assert signal_is_stale("bid_ask_spread_bps", now - timedelta(minutes=31), now) is True

    def test_default_rule_for_unlisted_type(self):
        """Unlisted types (e.g. return_1d) use the 90-min default."""
        now = _utc(2026, 4, 28, 16, 0)
        assert signal_is_stale("return_1d", now - timedelta(minutes=89), now) is False
        assert signal_is_stale("return_1d", now - timedelta(minutes=91), now) is True


class TestSignalIsStaleOutsideMarketHours:
    """Outside market hours, market-hours-aware signals check last-session freshness."""

    def test_ohlcv_eod_fresh_on_weekend(self):
        """OHLCV signal from Friday EOD is fresh on Saturday."""
        now = _utc(2026, 4, 25, 12, 0)   # Saturday noon
        ts = _utc(2026, 4, 24, 21, 0)    # Friday 21:00 UTC (at close)
        assert signal_is_stale("ohlcv_close", ts, now) is False

    def test_ohlcv_midday_stale_on_weekend(self):
        """OHLCV signal from Friday midday (well before close) is stale on Saturday."""
        now = _utc(2026, 4, 25, 12, 0)   # Saturday noon
        ts = _utc(2026, 4, 24, 17, 0)    # Friday 17:00 UTC (4 h before close)
        # freshness_cutoff = Friday 21:00 - 30min = Friday 20:30
        # Friday 17:00 < Friday 20:30 → stale
        assert signal_is_stale("ohlcv_close", ts, now) is True

    def test_rsi_eod_fresh_on_weekend(self):
        now = _utc(2026, 4, 25, 12, 0)   # Saturday noon
        ts = _utc(2026, 4, 24, 21, 15)   # Friday 21:15 UTC (EOD job)
        assert signal_is_stale("rsi_14", ts, now) is False

    def test_order_flow_eod_fresh_on_weekend(self):
        now = _utc(2026, 4, 25, 12, 0)
        ts = _utc(2026, 4, 24, 21, 15)
        assert signal_is_stale("order_flow_imbalance", ts, now) is False

    def test_bid_ask_always_stale_on_weekend(self):
        """Bid-ask signals are always stale outside hours, even if recently produced."""
        now = _utc(2026, 4, 25, 12, 0)
        ts = _utc(2026, 4, 24, 21, 15)   # would pass the session check
        assert signal_is_stale("bid_ask_spread_bps", ts, now) is True

    def test_bid_ask_always_stale_overnight(self):
        """Bid-ask spread stale after close on a weeknight too."""
        now = _utc(2026, 4, 28, 22, 0)   # Tuesday 22:00 UTC (after close)
        ts = _utc(2026, 4, 28, 20, 55)   # Tuesday 20:55 UTC (just before close)
        assert is_market_hours(now) is False
        assert signal_is_stale("bid_ask_spread_bps", ts, now) is True

    def test_none_tzinfo_gets_utc(self):
        """Naive datetimes are treated as UTC."""
        now = datetime(2026, 4, 25, 12, 0)  # naive, Saturday
        ts = datetime(2026, 4, 24, 21, 15)  # naive, Friday EOD
        assert signal_is_stale("yf_close", ts, now) is False


# ──────────────────────────────────────────────────────────────────────────────
# filter_stale_signals
# ──────────────────────────────────────────────────────────────────────────────

class TestFilterStaleSignals:

    def test_removes_stale_keeps_fresh(self):
        now = _utc(2026, 4, 28, 16, 0)  # Tuesday market hours
        rows = [
            {"signal_type": "yf_close",  "timestamp": now - timedelta(minutes=10)},
            {"signal_type": "yf_close",  "timestamp": now - timedelta(minutes=60)},  # stale (>30)
            {"signal_type": "rsi_14",    "timestamp": now - timedelta(minutes=50)},   # fresh (<60)
            {"signal_type": "rsi_14",    "timestamp": now - timedelta(minutes=90)},   # stale (>60)
        ]
        fresh = filter_stale_signals(rows, now)
        assert len(fresh) == 2
        assert fresh[0]["signal_type"] == "yf_close"
        assert fresh[1]["signal_type"] == "rsi_14"

    def test_removes_bid_ask_outside_hours(self):
        now = _utc(2026, 4, 25, 10, 0)  # Saturday
        ts = _utc(2026, 4, 24, 21, 15)  # Friday EOD
        rows = [
            {"signal_type": "yf_close",        "timestamp": ts},   # fresh (session)
            {"signal_type": "bid_ask_spread_bps", "timestamp": ts},   # stale (always)
            {"signal_type": "order_flow_imbalance", "timestamp": ts},  # fresh (session)
        ]
        fresh = filter_stale_signals(rows, now)
        types = [r["signal_type"] for r in fresh]
        assert "yf_close" in types
        assert "order_flow_imbalance" in types
        assert "bid_ask_spread_bps" not in types

    def test_empty_input_returns_empty(self):
        assert filter_stale_signals([], _utc(2026, 4, 28, 16, 0)) == []

    def test_all_stale_returns_empty(self):
        now = _utc(2026, 4, 28, 16, 0)
        rows = [
            {"signal_type": "yf_close", "timestamp": now - timedelta(hours=5)},
        ]
        assert filter_stale_signals(rows, now) == []


# ──────────────────────────────────────────────────────────────────────────────
# market_lookback_since
# ──────────────────────────────────────────────────────────────────────────────

class TestMarketLookbackSince:

    def test_during_market_hours_returns_90min_ago(self):
        now = _utc(2026, 4, 28, 16, 0)  # Tuesday 16:00 UTC
        assert is_market_hours(now) is True
        since = market_lookback_since(now)
        assert since == now - timedelta(minutes=90)

    def test_saturday_returns_friday_open(self):
        now = _utc(2026, 4, 25, 10, 0)  # Saturday 10:00 UTC
        since = market_lookback_since(now)
        # April = EDT: Friday close = 20:00 UTC (16:00 ET), open = 13:30 UTC (9:30 ET)
        expected = _utc(2026, 4, 24, 13, 30)
        assert since == expected

    def test_sunday_returns_friday_open(self):
        now = _utc(2026, 4, 26, 18, 0)  # Sunday 18:00 UTC
        since = market_lookback_since(now)
        expected = _utc(2026, 4, 24, 13, 30)   # Friday 9:30 ET (EDT)
        assert since == expected

    def test_monday_preopen_returns_friday_open(self):
        now = _utc(2026, 4, 27, 8, 0)   # Monday 08:00 UTC (before open)
        since = market_lookback_since(now)
        # Monday 08:00 UTC is before Monday's open → walk back to Friday's session;
        # last open = Friday 9:30 ET = 13:30 UTC (EDT)
        expected = _utc(2026, 4, 24, 13, 30)
        assert since == expected

    def test_weeknight_after_close_returns_same_day_open(self):
        now = _utc(2026, 4, 28, 22, 0)  # Tuesday 22:00 UTC (after close)
        since = market_lookback_since(now)
        # 22:00 UTC ≥ Tuesday close (20:00 UTC EDT) → same-day session;
        # last open = Tuesday 9:30 ET = 13:30 UTC
        expected = _utc(2026, 4, 28, 13, 30)
        assert since == expected


# ──────────────────────────────────────────────────────────────────────────────
# FINRA short volume staleness
# ──────────────────────────────────────────────────────────────────────────────

class TestShortVolumeStaleness:
    """
    FINRA short volume signals are daily-cadence (published ~21:30 UTC).

    Calendar reference (April/May 2026):
        Fri 2026-04-24 | Sat 2026-04-25 | Sun 2026-04-26
        Mon 2026-04-27 | Tue 2026-04-28 | Wed 2026-04-29
        Thu 2026-04-30 | Fri 2026-05-01
    """

    def test_sunday_afternoon_friday_file_is_fresh(self):
        """Sunday 18:00 UTC: Friday's short_volume_ratio_otc is fresh.

        ref_date: Sunday 18:00 < publish_cutoff (Sun 00:00) is False
        → actually Sun 18:00 >= Sun 00:00 → ref_date = Sunday
        → walk back weekends → Friday.
        Expected close = Fri 21:00; freshness_cutoff = Fri 20:30.
        Friday 21:00 signal ≥ Fri 20:30 → fresh.
        """
        now = _utc(2026, 4, 26, 18, 0)       # Sunday 18:00 UTC
        ts = _utc(2026, 4, 24, 21, 0)        # Friday 21:00 UTC (at close)
        for sig in ("short_volume_otc", "short_volume_total_otc", "short_volume_ratio_otc"):
            assert signal_is_stale(sig, ts, now) is False

    def test_tuesday_2330_tuesday_file_present_is_fresh(self):
        """Tuesday 23:30 UTC: Tuesday's file (ts=21:30) is fresh.

        publish_cutoff = Tue 22:00 + 2h = Wed 00:00.
        Tue 23:30 < Wed 00:00 → ref_date = Monday.
        But wait — Monday's close is Mon 21:00 and Tuesday's signal at
        21:30 is after Mon 20:30 → fresh.

        Actually: the signal timestamped Tue 21:30 is ALSO after Mon 20:30,
        so it's fresh regardless.  The key point is no false positive.
        """
        now = _utc(2026, 4, 28, 23, 30)      # Tuesday 23:30 UTC
        ts = _utc(2026, 4, 28, 21, 30)       # Tuesday 21:30 UTC (today's file)
        assert signal_is_stale("short_volume_ratio_otc", ts, now) is False

    def test_tuesday_2330_monday_file_within_grace_is_fresh(self):
        """Tuesday 23:30 UTC: Monday's file is still fresh (within grace).

        publish_cutoff = Tue 22:00 + 2h = Wed 00:00.
        Tue 23:30 < Wed 00:00 → ref_date = Mon 2026-04-27 (weekday).
        expected_close = Mon 21:00; freshness_cutoff = Mon 20:30.
        Monday's signal at 21:30 ≥ Mon 20:30 → fresh.
        """
        now = _utc(2026, 4, 28, 23, 30)      # Tuesday 23:30 UTC
        ts = _utc(2026, 4, 27, 21, 30)       # Monday 21:30 UTC
        assert signal_is_stale("short_volume_ratio_otc", ts, now) is False

    def test_wednesday_0030_monday_file_stale(self):
        """Wednesday 00:30 UTC: Tuesday's file should be available; Monday's is stale.

        publish_cutoff = Wed 22:00 + 2h = Thu 00:00.
        Wed 00:30 < Thu 00:00 → ref_date = Tuesday 2026-04-28 (weekday).
        expected_close = Tue 21:00; freshness_cutoff = Tue 20:30.
        Monday's signal at 21:30 < Tue 20:30 → stale.
        """
        now = _utc(2026, 4, 29, 0, 30)       # Wednesday 00:30 UTC
        ts = _utc(2026, 4, 27, 21, 30)       # Monday 21:30 UTC
        assert signal_is_stale("short_volume_ratio_otc", ts, now) is True

    def test_wednesday_market_holiday_tuesday_file_fresh(self):
        """Wednesday on a market holiday: Tuesday's file is fresh.

        We don't have a holiday calendar, but if the scoring job doesn't
        run on Wednesday (holiday), no new file is expected.  The staleness
        check doesn't know about holidays — it just checks whether the
        signal is from the expected trading day.

        At Wednesday 08:00 UTC (before markets would open):
        publish_cutoff = Wed 22:00 + 2h = Thu 00:00.
        Wed 08:00 < Thu 00:00 → ref_date = Tuesday 2026-04-28.
        expected_close = Tue 21:00; freshness_cutoff = Tue 20:30.
        Tuesday's signal at 21:30 ≥ Tue 20:30 → fresh.
        """
        now = _utc(2026, 4, 29, 8, 0)        # Wednesday 08:00 UTC
        ts = _utc(2026, 4, 28, 21, 30)       # Tuesday 21:30 UTC
        assert signal_is_stale("short_volume_ratio_otc", ts, now) is False

    def test_all_three_types_use_same_logic(self):
        """All 3 short volume types use the same staleness rule."""
        now = _utc(2026, 4, 26, 18, 0)       # Sunday 18:00 UTC
        ts_fresh = _utc(2026, 4, 24, 21, 0)  # Friday 21:00
        ts_stale = _utc(2026, 4, 23, 21, 0)  # Thursday 21:00
        for sig in ("short_volume_otc", "short_volume_total_otc", "short_volume_ratio_otc"):
            assert signal_is_stale(sig, ts_fresh, now) is False
            assert signal_is_stale(sig, ts_stale, now) is True

    def test_during_market_hours_still_uses_daily_logic(self):
        """Even during market hours, short volume uses daily-cadence logic.

        Tuesday 16:00 UTC (market open): Monday's file from 21:30 is fresh
        because the publish_cutoff (Tue 00:00) hasn't been reached by the
        reference check — wait, 16:00 > 00:00.

        publish_cutoff = Tue 22:00 + 2h = Wed 00:00.
        Tue 16:00 < Wed 00:00 → ref_date = Monday.
        expected_close = Mon 21:00; freshness_cutoff = Mon 20:30.
        Monday 21:30 ≥ Mon 20:30 → fresh.
        """
        now = _utc(2026, 4, 28, 16, 0)       # Tuesday 16:00 UTC (market hours)
        assert is_market_hours(now) is True
        ts = _utc(2026, 4, 27, 21, 30)       # Monday 21:30 UTC
        assert signal_is_stale("short_volume_ratio_otc", ts, now) is False

    def test_saturday_morning_friday_file_fresh(self):
        """Saturday 06:00 UTC: Friday's file is fresh."""
        now = _utc(2026, 4, 25, 6, 0)        # Saturday 06:00 UTC
        ts = _utc(2026, 4, 24, 21, 30)       # Friday 21:30 UTC
        assert signal_is_stale("short_volume_ratio_otc", ts, now) is False

    def test_monday_preopen_friday_file_fresh(self):
        """Monday 08:00 UTC (before markets open): Friday's file is fresh.

        publish_cutoff = Mon 22:00 + 2h = Tue 00:00.
        Mon 08:00 < Tue 00:00 → ref_date = Sunday → walk back → Friday.
        expected_close = Fri 21:00; freshness_cutoff = Fri 20:30.
        Friday 21:30 ≥ Fri 20:30 → fresh.
        """
        now = _utc(2026, 4, 27, 8, 0)        # Monday 08:00 UTC
        ts = _utc(2026, 4, 24, 21, 30)       # Friday 21:30 UTC
        assert signal_is_stale("short_volume_ratio_otc", ts, now) is False

    def test_filter_stale_signals_keeps_fresh_sv(self):
        """filter_stale_signals preserves fresh short volume signals."""
        now = _utc(2026, 4, 26, 18, 0)       # Sunday 18:00 UTC
        ts = _utc(2026, 4, 24, 21, 30)       # Friday 21:30
        rows = [
            {"signal_type": "short_volume_ratio_otc", "timestamp": ts},
            {"signal_type": "yf_close",               "timestamp": ts},
        ]
        fresh = filter_stale_signals(rows, now)
        types = [r["signal_type"] for r in fresh]
        assert "short_volume_ratio_otc" in types
        assert "yf_close" in types
