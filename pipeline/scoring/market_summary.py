"""
pipeline/scoring/market_summary.py

Rule-based market sentiment summary (pro tier) — the universe-wide analog of
the per-ticker explanation in pipeline/explanation/templates.py.

Contract
--------
- Input  : a market-overview blob as produced by
           pipeline.scoring.market_overview.build_overview().
- Output : a plain-English summary string (2–4 short sentences).
- Constraint : only references figures present in the blob. Never invents
               numbers or reasons not backed by the tick's aggregates.

Assembly
--------
1. Mood + headline breadth from average_score / breadth_above_50_pct
   (+ an improving/weakening clause when breadth_improving_pct is present).
2. Sector lead/lag from the per-sector averages.
3. The single largest up- and down-mover by 1-day change.

Every clause is guarded — missing or null inputs are simply dropped rather
than filled in. An empty tick yields a fixed fallback string.

This module deliberately imports nothing from market_overview (so the two can
be wired together without a cycle) and nothing from the api/ layer (System A
must not depend on System B). The mood bands mirror the per-ticker label
mapping in api/response/labels.py in spirit, but are finer near 50: a
universe *average* compresses toward the middle, so the neutral band is
narrowed and "mildly" tiers are added to keep the summary informative.
"""
from __future__ import annotations

_EMPTY = "No market sentiment data available for this tick."


def _mood(avg: float) -> str:
    """Market mood word for a universe-average score (0–100)."""
    if avg >= 65.0:
        return "strongly bullish"
    if avg >= 58.0:
        return "bullish"
    if avg >= 53.0:
        return "mildly bullish"
    if avg > 47.0:
        return "neutral"
    if avg > 42.0:
        return "mildly bearish"
    if avg > 35.0:
        return "bearish"
    return "strongly bearish"


def _signed(x: float) -> str:
    """Format a 1-day change with an explicit sign, one decimal (e.g. '+8.2')."""
    return f"{x:+.1f}"


def _headline(overview: dict) -> str:
    avg = overview["average_score"]
    parts = [f"avg {avg:.0f}"]

    breadth = overview.get("breadth_above_50_pct")
    if breadth is not None:
        clause = f"{breadth:.0f}% of names above neutral"
        improving = overview.get("breadth_improving_pct")
        if improving is not None:
            if improving >= 55.0:
                clause += " and improving"
            elif improving <= 45.0:
                clause += " but weakening"
        parts.append(clause)

    return f"Market sentiment is {_mood(avg)} ({', '.join(parts)})."


def _sectors_sentence(overview: dict) -> str | None:
    sectors = overview.get("sectors") or []
    if len(sectors) < 2:
        return None
    ranked = sorted(sectors, key=lambda s: s["average_score"], reverse=True)
    laggard = ranked[-1]["sector"]
    # Two leaders when there are enough distinct sectors, else one.
    if len(ranked) >= 4:
        leaders = f"{ranked[0]['sector']} and {ranked[1]['sector']} lead"
    else:
        leaders = f"{ranked[0]['sector']} leads"
    return f"{leaders}; {laggard} lags."


def _movers_sentence(overview: dict) -> str | None:
    top = overview.get("top_movers") or []
    bottom = overview.get("bottom_movers") or []
    bits = []
    if top:
        bits.append(f"{top[0]['ticker']} ({_signed(top[0]['score_change_1d'])})")
    # Avoid naming the same ticker twice when the universe is tiny.
    if bottom and (not top or bottom[0]["ticker"] != top[0]["ticker"]):
        bits.append(f"{bottom[0]['ticker']} ({_signed(bottom[0]['score_change_1d'])})")
    if not bits:
        return None
    return f"Biggest movers: {', '.join(bits)}."


def build_summary(overview: dict) -> str:
    """
    Build a plain-English market sentiment summary from an overview blob.

    Parameters
    ----------
    overview : dict
        A blob as returned by pipeline.scoring.market_overview.build_overview().

    Returns
    -------
    str
        A 2–4 sentence summary, or a fixed fallback when the tick is empty.
    """
    if not overview.get("universe_scored") or overview.get("average_score") is None:
        return _EMPTY

    sentences = [_headline(overview)]
    for clause in (_sectors_sentence(overview), _movers_sentence(overview)):
        if clause:
            sentences.append(clause)
    return " ".join(sentences)
