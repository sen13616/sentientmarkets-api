"""
pipeline/features/surprise.py

Narrative surprise (nowcasting plan, Phase 5b — eval-gated, flag-off by default).

Markets price *new* information: persistent positive coverage is already
priced, so the narrative LEVEL carries no forward signal (see
docs/SUMMARYOFTESTING.md). This feature scores the DEVIATION of the current
coverage window from the ticker's own trailing coverage baseline instead.

    current  = relevance-weighted mean finbert_score over the last 24h
    baseline = daily relevance-weighted means over the trailing 14 days,
               EXCLUDING the current window
    surprise = z-score of current vs baseline, mapped to [0, 100]
               (RollingZScorer: clamp ±3, 50 = in line with own baseline)

Written to Redis state + sentiment_history.narrative_surprise ONLY when
ENABLE_NARRATIVE_SURPRISE=1. Never enters the composite, the API schema, or
any served score until it beats the narrative_index baseline on the eval
scorecard (scripts/eval/) after >= 3-4 weeks of accumulation.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from pipeline.features.normalize import RollingZScorer
from scripts.db.queries.raw_articles import get_article_scores_between

_log = logging.getLogger(__name__)

CURRENT_WINDOW_HOURS = 24
BASELINE_DAYS = 14
#: Minimum articles across the whole baseline window; below this the ticker's
#: coverage is too thin for a meaningful baseline → None.
MIN_BASELINE_ARTICLES = 10

#: window=BASELINE_DAYS daily buckets; effective minimum = 7 populated days.
_scorer = RollingZScorer(window=BASELINE_DAYS, min_obs=5, sigma_floor=1e-3)


def enabled() -> bool:
    """Feature flag, read per call so tests (and restarts) can toggle it."""
    return os.getenv("ENABLE_NARRATIVE_SURPRISE", "").lower() in ("1", "true", "yes")


def _weighted_mean(rows: list[dict]) -> float | None:
    """Relevance-weighted mean of finbert_score; None with no usable rows."""
    num = den = 0.0
    for r in rows:
        score = r.get("finbert_score")
        rel = r.get("relevance_score")
        if score is None or rel is None or rel <= 0:
            continue
        num += float(score) * float(rel)
        den += float(rel)
    return num / den if den > 0 else None


def _daily_means(rows: list[dict]) -> list[float]:
    """Bucket rows by UTC date and return per-day weighted means, oldest first."""
    by_day: dict = {}
    for r in rows:
        published = r.get("published_at")
        if published is None:
            continue
        by_day.setdefault(published.date(), []).append(r)
    means = []
    for day in sorted(by_day):
        m = _weighted_mean(by_day[day])
        if m is not None:
            means.append(m)
    return means


async def compute_narrative_surprise(ticker: str, now: datetime) -> float | None:
    """
    Surprise score in [0, 100] (50 = coverage tone in line with the ticker's
    own trailing baseline), or None when the current window has no scored
    coverage or the baseline is too thin (cold start, sparse names).
    """
    current_start = now - timedelta(hours=CURRENT_WINDOW_HOURS)
    baseline_start = current_start - timedelta(days=BASELINE_DAYS)

    current_rows = await get_article_scores_between(ticker, current_start, now)
    current = _weighted_mean(current_rows)
    if current is None:
        return None

    baseline_rows = await get_article_scores_between(
        ticker, baseline_start, current_start
    )
    if len(baseline_rows) < MIN_BASELINE_ARTICLES:
        return None

    history = _daily_means(baseline_rows)
    score = _scorer.score_from_history(history, current)
    return round(score, 2) if score is not None else None
