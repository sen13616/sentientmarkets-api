"""
api/response/labels.py

Maps a numeric sentiment score to its human-readable label.

Label thresholds (per spec)
---------------------------
    0–20   → Strongly Bearish
    21–40  → Bearish
    41–60  → Neutral
    61–80  → Bullish
    81–100 → Strongly Bullish
"""

from __future__ import annotations


def score_to_label(score: int | float) -> str:
    """Return the sentiment label for a composite score in [0, 100]."""
    s = int(score)
    if s <= 20:
        return "Strongly Bearish"
    if s <= 40:
        return "Bearish"
    if s <= 60:
        return "Neutral"
    if s <= 80:
        return "Bullish"
    return "Strongly Bullish"
