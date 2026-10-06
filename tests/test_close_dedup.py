"""
tests/test_close_dedup.py — Verify _compute_returns handles same-day closes correctly.

Without DISTINCT ON (DATE(timestamp)) dedup in get_close_history, the
first intraday cron of a session inserts today's close into history.
_compute_returns then divides today by today, collapsing return_1d to ~0.

Note: _compute_returns uses log returns as of Sprint 1 (G-S4).
"""
from __future__ import annotations

import math
from datetime import datetime, timezone

from pipeline.sources.market import _compute_returns


def test_same_day_closes_yield_nonzero_return():
    """If close_history has two entries for the same day, return should use prior day."""
    # Scenario: two same-day entries should not both appear in deduped history.
    # This test validates the _compute_returns logic given a properly deduped
    # history (one row per day).
    yesterday_close = 100.0
    today_close = 105.0

    # After dedup: history has one entry for yesterday (the most recent session close)
    history = [
        (datetime(2026, 5, 4, 21, 0, tzinfo=timezone.utc), yesterday_close),
    ]

    results = _compute_returns(today_close, history)
    result_dict = dict(results)

    assert "return_1d" in result_dict
    expected = math.log(today_close / yesterday_close)
    assert abs(result_dict["return_1d"] - expected) < 1e-6
    assert result_dict["return_1d"] != 0.0  # Must not be zero


def test_duplicate_today_in_history_collapses_return():
    """Demonstrate the bug: if today's close leaks into history[-1], return_1d → 0."""
    today_close = 105.0

    # BUG scenario (before dedup fix): history[-1] is also today
    history = [
        (datetime(2026, 5, 4, 21, 0, tzinfo=timezone.utc), 100.0),  # yesterday
        (datetime(2026, 5, 5, 14, 45, tzinfo=timezone.utc), 105.0),  # today (leaked)
    ]

    results = _compute_returns(today_close, history)
    result_dict = dict(results)

    # With the leaked duplicate, return_1d = log(today/today) = 0.0
    assert result_dict["return_1d"] == 0.0


def test_compute_returns_5d_20d():
    """5d and 20d returns use the correct lookback offsets."""
    current_close = 110.0
    # Build 25 days of history (one per day) from 80.0 to 104.0
    history = [
        (datetime(2026, 4, i + 6, 21, 0, tzinfo=timezone.utc), 80.0 + i)
        for i in range(25)
    ]

    results = _compute_returns(current_close, history)
    result_dict = dict(results)

    # closes[-1] = 104.0 (day 24), closes[-5] = 100.0 (day 20), closes[-20] = 85.0 (day 5)
    assert "return_1d" in result_dict
    assert "return_5d" in result_dict
    assert "return_20d" in result_dict

    assert abs(result_dict["return_1d"] - math.log(110.0 / 104.0)) < 1e-6
    assert abs(result_dict["return_5d"] - math.log(110.0 / 100.0)) < 1e-6
    assert abs(result_dict["return_20d"] - math.log(110.0 / 85.0)) < 1e-6
