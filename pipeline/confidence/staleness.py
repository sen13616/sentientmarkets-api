"""
pipeline/confidence/staleness.py

Checks whether each data source's most-recent timestamp is within the
spec-defined freshness threshold.

Thresholds (spec §Layer 09)
----------------------------
    market   : 90 minutes during market hours; market-hours-aware outside
    news     : 6 hours  (lower bound of the 6–12 hour range)
    analyst  : 3 days   (lower bound of the 3–5 day range)
    insider  : 30 days  (lower bound of the 30–45 day range)
    macro    : 72 hours (covers weekend gap when macro_intraday is paused)

A source is stale if:
  - its as_of timestamp is None (no data received), OR
  - (now − as_of) > threshold

Market staleness is market-hours-aware.  A score produced at Friday
21:15 UTC (end-of-day run) remains fresh all weekend and is not penalised
simply because markets are closed.  The 90-minute threshold only applies
while markets are actually open.

Usage
-----
    from pipeline.confidence.staleness import check_staleness

    stale = check_staleness({
        "market":  datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
        "news":    None,  # missing → stale
        ...
    })
    # stale == {"market": False, "news": True, ...}
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# US equity regular session in *Eastern* time. Using a DST-aware timezone
# (not a fixed UTC window) keeps the session correct year-round: 9:30 am –
# 4:00 pm ET is 14:30 – 21:00 UTC in winter (EST) but 13:30 – 20:00 UTC in
# summer (EDT). The previous hardcoded UTC window was a full hour off for the
# ~8 months of EDT each year.
_EASTERN = ZoneInfo("America/New_York")
_SESSION_OPEN_ET  = (9, 30)    # (hour, minute) Eastern
_SESSION_CLOSE_ET = (16, 0)    # (hour, minute) Eastern

# How long after the most-recent market close the end-of-day score is still fresh.
# Set to 30 minutes so the EOD job at 21:15 UTC has plenty of margin.
_EOD_GRACE = timedelta(minutes=30)


def _as_utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def _session_close_utc(now: datetime) -> datetime:
    """UTC instant of the most-recent weekday 16:00-ET close at or before *now*."""
    et = _as_utc(now).astimezone(_EASTERN)
    close_et = et.replace(hour=_SESSION_CLOSE_ET[0], minute=_SESSION_CLOSE_ET[1],
                          second=0, microsecond=0)
    cand = et if et >= close_et else et - timedelta(days=1)
    while cand.isoweekday() > 5:                       # walk back to a weekday
        cand -= timedelta(days=1)
    close_et = cand.replace(hour=_SESSION_CLOSE_ET[0], minute=_SESSION_CLOSE_ET[1],
                            second=0, microsecond=0)
    return close_et.astimezone(timezone.utc)


def _session_open_utc(close_utc: datetime) -> datetime:
    """UTC instant of the 9:30-ET open for the trading day of *close_utc*."""
    et = close_utc.astimezone(_EASTERN)
    open_et = et.replace(hour=_SESSION_OPEN_ET[0], minute=_SESSION_OPEN_ET[1],
                         second=0, microsecond=0)
    return open_et.astimezone(timezone.utc)

STALENESS_THRESHOLDS: dict[str, timedelta] = {
    "market":  timedelta(minutes=90),
    "news":    timedelta(hours=6),
    "analyst": timedelta(days=3),
    "insider": timedelta(days=30),
    "macro":   timedelta(hours=72),
}


def is_market_hours(now: datetime) -> bool:
    """
    Return True if *now* falls within US equity regular market hours.

    Session: weekdays 9:30 am – 4:00 pm US/Eastern (DST-aware).
    """
    et = _as_utc(now).astimezone(_EASTERN)
    # isoweekday: Monday=1 … Friday=5, Saturday=6, Sunday=7
    if et.isoweekday() > 5:
        return False
    open_minutes  = _SESSION_OPEN_ET[0]  * 60 + _SESSION_OPEN_ET[1]
    close_minutes = _SESSION_CLOSE_ET[0] * 60 + _SESSION_CLOSE_ET[1]
    now_minutes   = et.hour * 60 + et.minute
    return open_minutes <= now_minutes < close_minutes


def _last_market_close(now: datetime) -> datetime:
    """
    Return the UTC datetime of the most-recent weekday market close (16:00 ET).

    If markets have already closed today (a weekday), that is today's close;
    otherwise walk back to the previous weekday's close. DST-aware.
    """
    return _session_close_utc(now)


def _market_stale(ts: datetime, now: datetime) -> bool:
    """
    Market-hours-aware staleness check for the "market" source.

    During market hours    : stale if (now − ts) > 90 minutes.
    Outside market hours   : stale if ts is older than the most-recent market
                             close plus EOD_GRACE (30 min). This means an
                             end-of-day score produced at 21:15 UTC stays
                             fresh through the entire overnight / weekend
                             period.
    """
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    if is_market_hours(now):
        return (now - ts) > STALENESS_THRESHOLDS["market"]

    # Outside market hours: compare ts against the last close
    last_close = _last_market_close(now)
    # Score is fresh if it was produced at or after the last close
    # (with a 30-minute grace window so the EOD job has time to run)
    freshness_cutoff = last_close - _EOD_GRACE
    return ts < freshness_cutoff


def check_staleness(
    as_of: dict[str, datetime | None],
    now: datetime | None = None,
) -> dict[str, bool]:
    """
    Return a dict of {source_name: is_stale} for each key in
    STALENESS_THRESHOLDS that appears in `as_of`.

    Sources present in STALENESS_THRESHOLDS but absent from `as_of`
    are treated as stale (no data = stale).

    Parameters
    ----------
    as_of : dict mapping source name → last-fetched datetime (UTC) or None.
    now   : reference time (defaults to datetime.now(UTC)).

    Returns
    -------
    dict[str, bool]  — True = stale, False = fresh.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    result: dict[str, bool] = {}
    for source, threshold in STALENESS_THRESHOLDS.items():
        ts = as_of.get(source)
        if ts is None:
            result[source] = True
            continue

        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        if source == "market":
            result[source] = _market_stale(ts, now)
        else:
            result[source] = (now - ts) > threshold

    return result


def stale_sources(
    as_of: dict[str, datetime | None],
    now: datetime | None = None,
) -> list[str]:
    """
    Convenience wrapper — returns only the names of stale sources.

    Useful for passing directly to pipeline.confidence.scorer.compute_confidence.
    """
    return [src for src, is_stale in check_staleness(as_of, now=now).items() if is_stale]


# ---------------------------------------------------------------------------
# Per-signal-type staleness rules
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SignalStalenessRule:
    """
    Staleness rule for an individual signal type.

    Parameters
    ----------
    max_age_minutes      : Maximum age (in minutes) during market hours before
                           the signal is considered stale.
    market_hours_aware   : When True, outside market hours the signal is fresh
                           if it came from the most recent trading session
                           (same logic as the layer-level ``_market_stale``).
    always_stale_outside : When True, the signal is unconditionally stale
                           outside market hours regardless of when it was
                           produced.  Used for bid/ask quotes which are
                           not meaningful after the close.
    """
    max_age_minutes: int
    market_hours_aware: bool = True
    always_stale_outside: bool = False


SIGNAL_STALENESS_RULES: dict[str, SignalStalenessRule] = {
    # ── OHLCV (yfinance primary, polygon fallback) ────────────────────────
    "yf_open":              SignalStalenessRule(30),
    "yf_high":              SignalStalenessRule(30),
    "yf_low":               SignalStalenessRule(30),
    "yf_close":             SignalStalenessRule(30),
    "yf_volume":            SignalStalenessRule(30),
    "ohlcv_open":           SignalStalenessRule(30),
    "ohlcv_high":           SignalStalenessRule(30),
    "ohlcv_low":            SignalStalenessRule(30),
    "ohlcv_close":          SignalStalenessRule(30),
    "ohlcv_volume":         SignalStalenessRule(30),
    # ── RSI ───────────────────────────────────────────────────────────────
    "rsi_14":               SignalStalenessRule(60),
    # ── Order flow (derived from OHLCV bar) ───────────────────────────────
    "order_flow_imbalance": SignalStalenessRule(30),
    "buy_pressure":         SignalStalenessRule(30),
    "sell_pressure":        SignalStalenessRule(30),
    # ── Bid-ask spread (only meaningful during market hours) ──────────────
    # Only bid_ask_spread_bps is persisted; raw bid/ask/bid_ask_spread stopped
    # being written 2026-07-20.
    "bid_ask_spread_bps":   SignalStalenessRule(30, always_stale_outside=True),
    # ── FINRA short volume (daily cadence, published ~21:30 UTC) ─────────
    # market_hours_aware=False: these use a custom checker (_short_volume_stale)
    # that accounts for the daily publication schedule and weekends.
    "short_volume_otc":       SignalStalenessRule(0, market_hours_aware=False),
    "short_volume_total_otc": SignalStalenessRule(0, market_hours_aware=False),
    "short_volume_ratio_otc": SignalStalenessRule(0, market_hours_aware=False),
}

# Unlisted market signal types (return_*, volume_ratio, legacy options) use
# the existing 90-minute layer-level threshold with market-hours awareness.
_DEFAULT_SIGNAL_RULE = SignalStalenessRule(90)


# FINRA short volume signal types — use dedicated staleness logic
_SHORT_VOLUME_TYPES = frozenset({
    "short_volume_otc",
    "short_volume_total_otc",
    "short_volume_ratio_otc",
})

# FINRA publishes daily short volume around 21:30 UTC.  The ingest job
# runs at 21:30 UTC on weekdays.  We allow a 2-hour grace window past
# the expected publication time (22:00 UTC) before flagging stale.
_SV_EXPECTED_PUBLISH_UTC = (22, 0)   # hour, minute — when we expect data
_SV_GRACE = timedelta(hours=2)       # grace window past expected publish
# Nominal daily close used to bound short-volume freshness. Kept as a fixed
# UTC time (not the DST-aware ET session close): FINRA's daily file is a
# once-per-day artifact and the 2-hour publish grace absorbs the ≤1-hour DST
# shift, so a fixed nominal keeps this daily-cadence check simple and stable.
_SV_NOMINAL_CLOSE_UTC = (21, 0)      # hour, minute


def _short_volume_stale(ts: datetime, now: datetime) -> bool:
    """
    Staleness check for FINRA daily short-volume signals.

    These are daily-cadence signals published once per trading day (~21:30
    UTC).  The staleness logic accounts for the publication schedule:

    - On a trading day after the expected publish time (22:00 UTC) plus
      the grace window (2 h → 00:00 next day): today's file should be
      available; stale if ts predates today's market close.
    - On a trading day before the publish time: yesterday's file is the
      freshest; stale if ts predates the *previous* trading day's close.
    - On weekends: Friday's file remains valid through Monday morning;
      stale if ts predates Friday's close.

    The grace window prevents false-positive staleness when FINRA's file
    is delayed by up to 2 hours past the expected time.
    """
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    # Walk back to find the most recent trading day whose file should be
    # available by *now*.
    #
    # The publication deadline for a trading day D is D at 22:00 UTC + 2h
    # grace = D+1 at 00:00 UTC.  So if now >= D at 24:00 (midnight after
    # D), we expect D's file.  Otherwise we expect the *previous* trading
    # day's file.

    # Step 1: find "today" in terms of the expected-publish schedule.
    # If it's past the publish+grace time, the current calendar date is the
    # reference trading day.  Otherwise, the previous calendar date is.
    publish_cutoff = now.replace(
        hour=_SV_EXPECTED_PUBLISH_UTC[0],
        minute=_SV_EXPECTED_PUBLISH_UTC[1],
        second=0, microsecond=0,
    ) + _SV_GRACE  # e.g. 00:00 next day

    if now >= publish_cutoff:
        ref_date = now
    else:
        ref_date = now - timedelta(days=1)

    # Step 2: walk ref_date back to the most recent weekday (trading day).
    while ref_date.isoweekday() > 5:
        ref_date -= timedelta(days=1)

    # The expected file is from ref_date's trading session.  The signal
    # timestamp should be at or after ref_date's market close (21:00 UTC)
    # minus a grace window (the EOD job may write signals a bit before close).
    expected_close = ref_date.replace(
        hour=_SV_NOMINAL_CLOSE_UTC[0], minute=_SV_NOMINAL_CLOSE_UTC[1],
        second=0, microsecond=0,
    )
    freshness_cutoff = expected_close - _EOD_GRACE
    return ts < freshness_cutoff


def signal_is_stale(signal_type: str, ts: datetime, now: datetime) -> bool:
    """
    Per-signal-type staleness check.

    During market hours the per-type ``max_age_minutes`` threshold applies.
    Outside market hours:
      - ``always_stale_outside`` signals are unconditionally stale.
      - Short-volume signals use dedicated daily-cadence logic.
      - Other ``market_hours_aware`` signals are fresh if produced during
        or after the most recent trading session (same grace-window logic
        as the layer-level ``_market_stale``).
    """
    # Short volume signals use dedicated daily-cadence logic at all times
    if signal_type in _SHORT_VOLUME_TYPES:
        return _short_volume_stale(ts, now)

    rule = SIGNAL_STALENESS_RULES.get(signal_type, _DEFAULT_SIGNAL_RULE)

    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    # During market hours: simple age check
    if is_market_hours(now):
        return (now - ts) > timedelta(minutes=rule.max_age_minutes)

    # Outside market hours
    if rule.always_stale_outside:
        return True

    if not rule.market_hours_aware:
        return (now - ts) > timedelta(minutes=rule.max_age_minutes)

    # Market-hours-aware: fresh if from the most recent trading session
    last_close = _last_market_close(now)
    freshness_cutoff = last_close - _EOD_GRACE
    return ts < freshness_cutoff


def filter_stale_signals(raw: list[dict], now: datetime) -> list[dict]:
    """
    Filter a list of raw signal rows, removing individually stale signals.

    Each row must have ``signal_type`` (str) and ``timestamp`` (datetime) keys.
    """
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return [
        row for row in raw
        if not signal_is_stale(row["signal_type"], row["timestamp"], now)
    ]


def market_lookback_since(now: datetime) -> datetime:
    """
    Compute the DB query start time for fetching market signals.

    During market hours : 90 minutes back (covers the longest per-signal
                          threshold with margin).
    Outside hours       : back to the open of the most recent trading
                          session so that session data is available for
                          scoring and per-signal filtering.
    """
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    if is_market_hours(now):
        return now - timedelta(minutes=90)
    # Outside hours: need signals from the entire last trading session
    last_close = _last_market_close(now)
    return _session_open_utc(last_close)
