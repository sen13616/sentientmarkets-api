"""
pipeline/features/positioning.py

Positioning research features (Track B3, 2026-07-22 — eval-gated, flag-off by
default; modeled on pipeline/features/surprise.py).

Two z-score features over positioning raw material:

    short_vol_z   — z-score of the latest short_volume_ratio_otc vs the
                    ticker's own trailing ~20 sessions (higher = today's
                    off-exchange short share is unusually high for this name).
    insider_net_z — z-score of the current UTC day's summed insider_net_shares
                    vs the daily-net distribution over the prior 20 weekdays,
                    zero-filled for weekdays with no filings (a quiet tape IS
                    the baseline; a filing after silence should score large).

Both return None rather than a fabricated neutral when history is too thin
(min observations) or has no variance (sigma floor). Raw z values are stored
unclamped and unmapped — the eval harness ranks cross-sectionally, so scale
does not matter; do NOT confuse these with the 0–100 mapped scores in
normalize.py.

Written into the `research_features` JSONB (migration 012) on Redis state and
sentiment_history ONLY when ENABLE_POSITIONING_FEATURES=1. Never enters the
composite, the API schema, or any served score. Evaluation is experiment E003
(own pre-registration) — do not run these through the harness before that.

Insider timestamp basis (Track B3 finding, 2026-07-22)
------------------------------------------------------
`insider_net_shares` rows are stamped with Finnhub's **transactionDate**
(fallback: filingDate) parsed to midnight UTC — verified against production:
12,907/12,907 rows are midnight-UTC date rows; they are NOT fetch-time stamped
(pipeline/sources/influencer.py `_insider_finnhub`). Caveat for point-in-time
research: the transaction date precedes the public Form-4 filing by up to ~2
business days, so rows can be timestamped slightly EARLIER than the market
could have known about them — a mild look-ahead in insider_net_z of up to ~2
days. This is deliberately left unchanged: re-stamping to filingDate would
(a) change served scores through the time-decay weights and (b) duplicate the
trailing 30-day re-fetch window under the raw_signals natural-key dedup
(ticker, signal_type, timestamp, value, source). Any evaluation of
insider-based features must budget for this ~2-day timestamp optimism.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta

_log = logging.getLogger(__name__)

#: Trailing window (sessions / weekdays) for both features.
WINDOW = 20
#: Minimum prior observations before a z-score is emitted.
MIN_OBS = 10
#: Below this standard deviation the baseline is considered degenerate.
SIGMA_FLOOR = 1e-9

_INSIDER_LOOKBACK_DAYS = 45  # calendar days; covers 20 weekdays + margin


def enabled() -> bool:
    """Feature flag, read per call so tests (and restarts) can toggle it."""
    return os.getenv("ENABLE_POSITIONING_FEATURES", "").lower() in ("1", "true", "yes")


# ---------------------------------------------------------------------------
# Pure math — shared by the live tick path and scripts/eval/backfill_features.py
# ---------------------------------------------------------------------------

def trailing_zscore(
    current: float,
    prior: list[float],
    min_obs: int = MIN_OBS,
    sigma_floor: float = SIGMA_FLOOR,
) -> float | None:
    """z = (current − mean(prior)) / std(prior); None on thin/degenerate history."""
    if len(prior) < min_obs:
        return None
    n = len(prior)
    mean = sum(prior) / n
    var = sum((x - mean) ** 2 for x in prior) / n
    std = var ** 0.5
    if std < sigma_floor:
        return None
    return round((current - mean) / std, 4)


def short_vol_z_from_series(values_oldest_first: list[float]) -> float | None:
    """Latest short_volume_ratio_otc vs the up-to-WINDOW prior sessions."""
    if len(values_oldest_first) < 2:
        return None
    *prior, current = values_oldest_first[-(WINDOW + 1):]
    return trailing_zscore(current, prior)


def prior_weekdays(as_of_day: date, n: int = WINDOW) -> list[date]:
    """The n weekdays strictly before as_of_day, oldest first."""
    days: list[date] = []
    d = as_of_day
    while len(days) < n:
        d -= timedelta(days=1)
        if d.isoweekday() <= 5:
            days.append(d)
    return list(reversed(days))


def insider_net_z_from_daily(
    daily_net: dict[date, float],
    as_of_day: date,
) -> float | None:
    """Current day's net shares vs the prior-20-weekday daily-net baseline.

    daily_net maps UTC date -> summed insider_net_shares for that date; days
    absent from the map count as 0.0 (no filings). If the entire baseline AND
    the current day are zero the feature is None (no information), never a
    fabricated neutral.
    """
    window = prior_weekdays(as_of_day, WINDOW)
    prior = [daily_net.get(d, 0.0) for d in window]
    current = daily_net.get(as_of_day, 0.0)
    if current == 0.0 and all(v == 0.0 for v in prior):
        return None
    return trailing_zscore(current, prior)


# ---------------------------------------------------------------------------
# Live tick path (orchestrator, flag-gated)
# ---------------------------------------------------------------------------

async def compute_positioning_features(ticker: str, now: datetime) -> dict:
    """Return {feature_name: z} with only the features that computed.

    Empty dict when nothing computed — the orchestrator then omits the
    research_features key entirely (no fabricated values in state).
    """
    from scripts.db.queries.raw_signals import get_signal_history, get_signals_since

    features: dict[str, float] = {}

    try:
        series = await get_signal_history(
            ticker, "short_volume_ratio_otc", limit=WINDOW + 1
        )
        z = short_vol_z_from_series(series)
        if z is not None:
            features["short_vol_z"] = z
    except Exception as exc:
        _log.debug("[%s] short_vol_z failed: %s", ticker, exc)

    try:
        since = now - timedelta(days=_INSIDER_LOOKBACK_DAYS)
        rows = await get_signals_since(ticker, since, ["insider_net_shares"])
        daily: dict[date, float] = {}
        for r in rows:
            d = r["timestamp"].date()
            daily[d] = daily.get(d, 0.0) + float(r["value"])
        z = insider_net_z_from_daily(daily, now.date())
        if z is not None:
            features["insider_net_z"] = z
    except Exception as exc:
        _log.debug("[%s] insider_net_z failed: %s", ticker, exc)

    return features
