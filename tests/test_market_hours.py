"""
tests/test_market_hours.py

Unit tests for the market-hours-aware staleness logic introduced in Task 2.

All tests are pure and synchronous — no DB, Redis, or async required.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pipeline.confidence.staleness import check_staleness, is_market_hours


def _utc(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


# ──────────────────────────────────────────────────────────────────────────────
# is_market_hours()
# ──────────────────────────────────────────────────────────────────────────────


class TestIsMarketHours:
    def test_weekday_during_hours_returns_true(self):
        # Monday 2026-04-27 at 15:00 UTC — well within market hours
        now = _utc(2026, 4, 27, 15, 0)
        assert is_market_hours(now) is True

    def test_weekday_after_close_returns_false(self):
        # Friday 2026-04-25 at 22:00 UTC — after 21:00 close
        now = _utc(2026, 4, 25, 22, 0)
        assert is_market_hours(now) is False

    def test_saturday_during_normal_hours_returns_false(self):
        # Saturday 2026-04-26 at 15:00 UTC
        now = _utc(2026, 4, 26, 15, 0)
        assert is_market_hours(now) is False

    def test_weekday_before_open_returns_false(self):
        # Monday at 13:00 UTC — before 14:30 open
        now = _utc(2026, 4, 27, 13, 0)
        assert is_market_hours(now) is False

    def test_exactly_at_open_returns_true_edt(self):
        # April = EDT: 9:30 ET open == 13:30 UTC.
        assert is_market_hours(_utc(2026, 4, 27, 13, 30)) is True
        assert is_market_hours(_utc(2026, 4, 27, 13, 29)) is False  # one min before open

    def test_exactly_at_close_returns_false_edt(self):
        # April = EDT: 16:00 ET close == 20:00 UTC; close is exclusive.
        assert is_market_hours(_utc(2026, 4, 27, 20, 0)) is False
        assert is_market_hours(_utc(2026, 4, 27, 19, 59)) is True  # one min before close

    def test_session_window_shifts_with_dst_est(self):
        # January = EST: 9:30 ET open == 14:30 UTC, 16:00 ET close == 21:00 UTC.
        # Proves the window tracks DST (this same 14:30 UTC is *inside* the EDT
        # session but *at open* in EST — the fixed-UTC window got this wrong).
        assert is_market_hours(_utc(2026, 1, 5, 14, 30)) is True  # Mon 9:30 EST
        assert is_market_hours(_utc(2026, 1, 5, 14, 29)) is False  # before EST open
        assert is_market_hours(_utc(2026, 1, 5, 21, 0)) is False  # EST close (exclusive)
        assert is_market_hours(_utc(2026, 1, 5, 20, 59)) is True

    def test_sunday_returns_false(self):
        now = _utc(2026, 4, 26, 18, 0)  # Sunday
        # 2026-04-26 is a Sunday
        assert now.isoweekday() == 7
        assert is_market_hours(now) is False


# ──────────────────────────────────────────────────────────────────────────────
# check_staleness() — market-hours-aware cases
# ──────────────────────────────────────────────────────────────────────────────


class TestMarketStaleness:
    # ── Test 4: EOD score on Friday stays fresh over the weekend ─────────────

    def test_eod_score_is_not_stale_on_saturday(self):
        """
        Friday 21:15 UTC EOD score should NOT be flagged stale on Saturday.
        The data is fresh — markets just haven't opened yet.
        """
        # Saturday morning UTC
        now = _utc(2026, 4, 25, 10, 0)  # Saturday 10:00 UTC
        market_as_of = _utc(2026, 4, 24, 21, 15)  # Friday  21:15 UTC (EOD job)

        # 2026-04-25 is a Saturday
        assert now.isoweekday() == 6
        assert is_market_hours(now) is False

        result = check_staleness({"market": market_as_of}, now=now)
        assert result["market"] is False, (
            "EOD score from Friday 21:15 should not be stale on Saturday"
        )

    # ── Test 5: Mid-morning Friday score IS stale on Saturday ────────────────

    def test_missed_eod_score_is_stale_on_saturday(self):
        """
        A score timestamped Friday 10:00 UTC (before close, EOD job missed)
        SHOULD be flagged stale on Saturday — it predates the last market close.
        """
        now = _utc(2026, 4, 25, 10, 0)  # Saturday 10:00 UTC
        market_as_of = _utc(2026, 4, 24, 10, 0)  # Friday  10:00 UTC (mid-morning)

        result = check_staleness({"market": market_as_of}, now=now)
        assert result["market"] is True, (
            "Score from Friday 10:00 should be stale on Saturday (missed EOD)"
        )

    # ── Test 6: Stale during market hours when data is 2 h old ───────────────

    def test_stale_during_market_hours_when_data_is_2h_old(self):
        """
        During market hours, threshold is 90 minutes.  Data that is 2 hours
        old must be flagged stale.
        """
        now = _utc(2026, 4, 28, 16, 0)  # Monday 16:00 UTC (market open)
        market_as_of = now - timedelta(hours=2)

        assert is_market_hours(now) is True

        result = check_staleness({"market": market_as_of}, now=now)
        assert result["market"] is True, (
            "Data that is 2 h old should be stale during market hours (threshold=90 min)"
        )

    # ── Fresh during market hours when data is recent ────────────────────────

    def test_fresh_during_market_hours_when_recent(self):
        now = _utc(2026, 4, 28, 16, 0)  # Monday 16:00 UTC
        market_as_of = now - timedelta(minutes=30)

        assert is_market_hours(now) is True

        result = check_staleness({"market": market_as_of}, now=now)
        assert result["market"] is False

    # ── None timestamp is always stale ───────────────────────────────────────

    def test_none_timestamp_is_always_stale(self):
        now = _utc(2026, 4, 25, 10, 0)  # Saturday — outside hours
        result = check_staleness({"market": None}, now=now)
        assert result["market"] is True

    # ── Non-market sources are unaffected ────────────────────────────────────

    def test_non_market_sources_use_fixed_threshold(self):
        """news threshold is 6 h — unaffected by market hours logic."""
        now = _utc(2026, 4, 25, 10, 0)  # Saturday
        news_ts = now - timedelta(hours=5)  # 5 h old → fresh
        result = check_staleness({"news": news_ts}, now=now)
        assert result["news"] is False

        old_news = now - timedelta(hours=7)  # 7 h old → stale
        result2 = check_staleness({"news": old_news}, now=now)
        assert result2["news"] is True


# ──────────────────────────────────────────────────────────────────────────────
# Session boundaries served as `market_hours` (api/response/assembler.py)
# ──────────────────────────────────────────────────────────────────────────────


class TestSessionBoundaries:
    def test_summer_boundaries_use_edt(self):
        from pipeline.confidence.staleness import last_session_close, next_session_open

        # Wednesday 2026-07-15 12:00 UTC (08:00 EDT), before the open.
        now = _utc(2026, 7, 15, 12, 0)
        assert next_session_open(now) == _utc(2026, 7, 15, 13, 30)
        assert last_session_close(now) == _utc(2026, 7, 14, 20, 0)

    def test_winter_boundaries_use_est(self):
        from pipeline.confidence.staleness import last_session_close, next_session_open

        # Wednesday 2026-01-14 22:00 UTC (17:00 EST), after the close.
        now = _utc(2026, 1, 14, 22, 0)
        assert next_session_open(now) == _utc(2026, 1, 15, 14, 30)
        assert last_session_close(now) == _utc(2026, 1, 14, 21, 0)

    def test_weekend_rolls_to_monday_across_dst_change(self):
        from pipeline.confidence.staleness import last_session_close, next_session_open

        # Saturday 2026-10-31; DST ends Sunday 2026-11-01.
        now = _utc(2026, 10, 31, 15, 0)
        assert last_session_close(now) == _utc(2026, 10, 30, 20, 0)  # Friday, EDT
        assert next_session_open(now) == _utc(2026, 11, 2, 14, 30)  # Monday, EST

    def test_assembler_payload_matches_is_open(self):
        from api.response.assembler import _market_hours_info

        # 13:45 UTC is inside the EDT session but before the EST open.
        summer = _market_hours_info(_utc(2026, 7, 15, 13, 45))
        assert summer.is_open is True
        assert summer.next_open == _utc(2026, 7, 16, 13, 30)
        winter = _market_hours_info(_utc(2026, 1, 15, 13, 45))
        assert winter.is_open is False
        assert winter.next_open == _utc(2026, 1, 15, 14, 30)
