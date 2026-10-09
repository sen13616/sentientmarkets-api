"""
pipeline/market_calendar.py

NYSE trading calendar: full-day closures and 1:00 pm ET early closes.

Layered on top of the weekday-only, DST-aware session helpers in
``pipeline.confidence.staleness`` (which scoring uses and which stay
unchanged). Used by the public ``/health/pipeline`` endpoint to decide
which market-calendar jobs are expected to have run, and by the
short-volume backfill to skip non-trading days.

Source: NYSE Group, "NYSE Group Announces 2026, 2027 and 2028 Holiday and
Early Closings Calendar" (2025-12-23); 2025 from the prior year's release.
Extend ``NYSE_HOLIDAYS`` / ``NYSE_EARLY_CLOSES`` before the last covered
year ends — outside the covered years the helpers fall back to weekdays
only and log a warning.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta, timezone

from pipeline.confidence.staleness import _EASTERN, _SESSION_CLOSE_ET, _SESSION_OPEN_ET, _as_utc

_log = logging.getLogger(__name__)

NYSE_HOLIDAYS: frozenset[date] = frozenset(
    {
        # 2025
        date(2025, 1, 1),  # New Year's Day
        date(2025, 1, 9),  # National Day of Mourning (President Carter)
        date(2025, 1, 20),  # MLK Day
        date(2025, 2, 17),  # Washington's Birthday
        date(2025, 4, 18),  # Good Friday
        date(2025, 5, 26),  # Memorial Day
        date(2025, 6, 19),  # Juneteenth
        date(2025, 7, 4),  # Independence Day
        date(2025, 9, 1),  # Labor Day
        date(2025, 11, 27),  # Thanksgiving
        date(2025, 12, 25),  # Christmas
        # 2026
        date(2026, 1, 1),  # New Year's Day
        date(2026, 1, 19),  # MLK Day
        date(2026, 2, 16),  # Washington's Birthday
        date(2026, 4, 3),  # Good Friday
        date(2026, 5, 25),  # Memorial Day
        date(2026, 6, 19),  # Juneteenth
        date(2026, 7, 3),  # Independence Day (observed)
        date(2026, 9, 7),  # Labor Day
        date(2026, 11, 26),  # Thanksgiving
        date(2026, 12, 25),  # Christmas
        # 2027
        date(2027, 1, 1),  # New Year's Day
        date(2027, 1, 18),  # MLK Day
        date(2027, 2, 15),  # Washington's Birthday
        date(2027, 3, 26),  # Good Friday
        date(2027, 5, 31),  # Memorial Day
        date(2027, 6, 18),  # Juneteenth (observed)
        date(2027, 7, 5),  # Independence Day (observed)
        date(2027, 9, 6),  # Labor Day
        date(2027, 11, 25),  # Thanksgiving
        date(2027, 12, 24),  # Christmas (observed)
    }
)

# 1:00 pm ET closes.
NYSE_EARLY_CLOSES: frozenset[date] = frozenset(
    {
        date(2025, 7, 3),
        date(2025, 11, 28),
        date(2025, 12, 24),
        date(2026, 11, 27),
        date(2026, 12, 24),
        date(2027, 11, 26),
    }
)

_EARLY_CLOSE_ET = (13, 0)

COVERED_YEARS: frozenset[int] = frozenset(d.year for d in NYSE_HOLIDAYS)

_warned_years: set[int] = set()


def _check_coverage(d: date) -> None:
    if d.year not in COVERED_YEARS and d.year not in _warned_years:
        _warned_years.add(d.year)
        _log.warning(
            "market_calendar: no NYSE holiday data for %d — treating every weekday as a session",
            d.year,
        )


def is_session_day(d: date) -> bool:
    """True if *d* (an Eastern calendar date) is an NYSE trading day."""
    if d.isoweekday() > 5:
        return False
    _check_coverage(d)
    return d not in NYSE_HOLIDAYS


def _session_bounds_utc(d: date) -> tuple[datetime, datetime]:
    """(open, close) UTC instants of trading day *d*, honouring early closes."""
    close_hm = _EARLY_CLOSE_ET if d in NYSE_EARLY_CLOSES else _SESSION_CLOSE_ET
    open_et = datetime.combine(d, time(*_SESSION_OPEN_ET), tzinfo=_EASTERN)
    close_et = datetime.combine(d, time(*close_hm), tzinfo=_EASTERN)
    return open_et.astimezone(timezone.utc), close_et.astimezone(timezone.utc)


def in_session(now: datetime) -> bool:
    """True if *now* falls inside an NYSE regular session (holiday- and early-close-aware)."""
    now = _as_utc(now)
    d = now.astimezone(_EASTERN).date()
    if not is_session_day(d):
        return False
    open_utc, close_utc = _session_bounds_utc(d)
    return open_utc <= now < close_utc


def last_session_close(now: datetime) -> datetime:
    """UTC instant of the most recent NYSE session close at or before *now*."""
    now = _as_utc(now)
    d = now.astimezone(_EASTERN).date()
    for _ in range(15):  # longest closure run is far shorter
        if is_session_day(d):
            _, close_utc = _session_bounds_utc(d)
            if close_utc <= now:
                return close_utc
        d -= timedelta(days=1)
    raise RuntimeError("no NYSE session close found in the last 15 days")
