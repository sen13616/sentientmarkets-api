"""
pipeline/scoring/market_overview.py

Per-tick universe-level statistics, computed once at the end of the global
scoring pass from values already in memory (no extra DB reads):

    • universe percentile per ticker (share of universe scoring strictly
      below, 0–100)
    • market overview blob: average score, breadth, top/bottom movers by
      1-day change, per-sector averages and within-sector ranks

Both are cached in Redis by the scheduler and served read-only by the API
(`universe_percentile` on the pro sentiment response; `/v1/market/overview`).

Gap safety: these functions only ever see tickers scored in the CURRENT
tick, and 1-day changes arrive pre-computed against a bounded 24–48h-old
baseline (see get_baseline_scores) — a ticker with no baseline has
change=None and is simply excluded from movers/breadth-improving. Nothing
here interpolates across missing ticks.
"""
from __future__ import annotations

from datetime import datetime

#: Redis keys written each tick by the scheduler and read by the API.
PERCENTILE_KEY = "pipeline:universe_percentile"
OVERVIEW_KEY = "pipeline:market_overview"

_N_MOVERS = 10


def compute_percentiles(scores: dict[str, float]) -> dict[str, float]:
    """
    Percentile of each ticker's score within the universe scored this tick.

    percentile = 100 × (# tickers strictly below) / (n − 1), so the lowest
    ticker scores 0.0 and the highest 100.0. A single-ticker universe maps
    to 50.0. Ties share the same percentile.
    """
    n = len(scores)
    if n == 0:
        return {}
    if n == 1:
        return {t: 50.0 for t in scores}

    ordered = sorted(scores.values())
    # For each distinct value, count of values strictly below it.
    below: dict[float, int] = {}
    for i, v in enumerate(ordered):
        if v not in below:
            below[v] = i
    return {
        t: round(100.0 * below[v] / (n - 1), 1)
        for t, v in scores.items()
    }


def build_overview(
    scores: dict[str, float],
    changes: dict[str, float | None],
    change_pcts: dict[str, float | None],
    sector_map: dict[str, str],
    timestamp: datetime,
) -> dict:
    """
    Build the market-overview blob for one scoring tick.

    Parameters
    ----------
    scores      : ticker → smoothed composite score, ONLY tickers scored
                  this tick.
    changes     : ticker → score_change_1d (None when no 24–48h baseline
                  exists — new ticker or data gap).
    change_pcts : ticker → score_change_1d_pct (None likewise).
    sector_map  : ticker → GICS sector; tickers absent from the map are
                  included in universe stats but not in the sector section.
    timestamp   : the scoring-tick time the blob was computed at.
    """
    n = len(scores)
    if n == 0:
        return {
            "timestamp": timestamp.isoformat(),
            "universe_scored": 0,
            "average_score": None,
            "breadth_above_50_pct": None,
            "breadth_improving_pct": None,
            "top_movers": [],
            "bottom_movers": [],
            "sectors": [],
        }

    average = sum(scores.values()) / n
    above_50 = sum(1 for v in scores.values() if v > 50.0)

    with_change = [t for t, c in changes.items() if c is not None and t in scores]
    improving_pct: float | None = None
    if with_change:
        improving = sum(1 for t in with_change if changes[t] > 0)
        improving_pct = round(100.0 * improving / len(with_change), 1)

    def _mover(t: str) -> dict:
        return {
            "ticker": t,
            "score": round(scores[t], 2),
            "score_change_1d": changes[t],
            "score_change_1d_pct": change_pcts.get(t),
        }

    ranked_by_change = sorted(with_change, key=lambda t: changes[t], reverse=True)
    top_movers = [_mover(t) for t in ranked_by_change[:_N_MOVERS]]
    bottom_movers = [_mover(t) for t in ranked_by_change[-_N_MOVERS:]][::-1]

    # Per-sector averages and within-sector rank (1 = highest score).
    by_sector: dict[str, list[str]] = {}
    for t in scores:
        sector = sector_map.get(t)
        if sector:
            by_sector.setdefault(sector, []).append(t)

    sectors = []
    for sector in sorted(by_sector):
        members = sorted(by_sector[sector], key=lambda t: scores[t], reverse=True)
        size = len(members)
        sectors.append({
            "sector": sector,
            "average_score": round(sum(scores[t] for t in members) / size, 2),
            "size": size,
            "tickers": [
                {"ticker": t, "score": round(scores[t], 2), "rank": i + 1}
                for i, t in enumerate(members)
            ],
        })

    return {
        "timestamp": timestamp.isoformat(),
        "universe_scored": n,
        "average_score": round(average, 2),
        "breadth_above_50_pct": round(100.0 * above_50 / n, 1),
        "breadth_improving_pct": improving_pct,
        "top_movers": top_movers,
        "bottom_movers": bottom_movers,
        "sectors": sectors,
    }
