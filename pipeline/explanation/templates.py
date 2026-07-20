"""
pipeline/explanation/templates.py

Rule-based explanation generator (Phase 1 / free tier).

Contract
--------
- Input  : ranked list[DriverRecord] from pipeline.scoring.drivers.extract_drivers()
- Output : plain-English explanation string (1–3 sentences)
- Constraint : only references signals present in the driver list.
               Never invents reasons not backed by data.

Template map
------------
Keyed on DriverRecord.signal (human-readable label produced by drivers._label()).
Each entry has three variants: "bullish", "bearish", "neutral".

Assembly strategy
-----------------
1. Take the top-ranked drivers (up to 3).
2. Split into primary-direction drivers and counter-direction drivers.
3. Build a lead sentence from the top 1–2 same-direction drivers.
4. If a notable counter signal exists (magnitude ≥ 0.3), append a second sentence.
"""
from __future__ import annotations

from pipeline.scoring.drivers import DriverRecord

# ---------------------------------------------------------------------------
# Template phrases — keyed by signal label, then direction
# ---------------------------------------------------------------------------

_PHRASES: dict[str, dict[str, str]] = {
    # Phrases are bare noun/verb phrases — no leading articles, no strength adjectives.
    # The qualifier ("strong", "moderate", "mild") is prepended by _qualifier().
    "RSI(14)": {
        "bullish": "RSI recovery from oversold territory",
        "bearish": "overbought RSI pressure",
        "neutral": "neutral RSI momentum",
    },
    "Price momentum (1d)": {
        "bullish": "single-day price gain",
        "bearish": "single-day price decline",
        "neutral": "flat daily price action",
    },
    "Price momentum (5d)": {
        "bullish": "5-day price momentum",
        "bearish": "5-day price weakness",
        "neutral": "flat 5-day price action",
    },
    "Price momentum (20d)": {
        "bullish": "20-day upward momentum",
        "bearish": "20-day price weakness",
        "neutral": "sideways 20-day price action",
    },
    "Volume surge": {
        "bullish": "buying volume surge",
        "bearish": "selling volume surge",
        "neutral": "normal volume levels",
    },
    "Insider transaction": {
        "bullish": "insider conviction",
        "bearish": "insider selling",
        "neutral": "mixed insider activity",
    },
    "Analyst consensus": {
        "bullish": "analyst buy ratings",
        "bearish": "analyst rating weakness",
        "neutral": "mixed analyst consensus",
    },
    "Analyst price target": {
        "bullish": "upside analyst price targets",
        "bearish": "below-market analyst price targets",
        "neutral": "in-line analyst price targets",
    },
    "VIX": {
        "bullish": "falling VIX (market fear receding)",
        "bearish": "rising VIX (growing market fear)",
        "neutral": "stable market volatility",
    },
    "Sector trend (20d)": {
        "bullish": "positive sector momentum",
        "bearish": "negative sector momentum",
        "neutral": "flat sector trend",
    },
    "Order flow": {
        "bullish": "buy-side order flow imbalance",
        "bearish": "sell-side order flow imbalance",
        "neutral": "balanced order flow",
    },
    "Buy pressure": {
        "bullish": "elevated buy pressure",
        "bearish": "weak buy pressure",
        "neutral": "neutral buy pressure",
    },
    "Sell pressure": {
        "bullish": "receding sell pressure",
        "bearish": "elevated sell pressure",
        "neutral": "neutral sell pressure",
    },
    "Bid-ask spread": {
        "bullish": "tight bid-ask spreads (strong liquidity)",
        "bearish": "wide bid-ask spreads (thin liquidity)",
        "neutral": "normal bid-ask spreads",
    },
    "Short volume ratio": {
        "bullish": "declining short volume activity",
        "bearish": "elevated short volume pressure",
        "neutral": "normal short volume levels",
    },
    "News sentiment": {
        "bullish": "positive news sentiment",
        "bearish": "negative news sentiment",
        "neutral": "mixed news sentiment",
    },
}

_DEFAULT_PHRASE: dict[str, str] = {
    "bullish": "a positive signal",
    "bearish": "a negative signal",
    "neutral": "a neutral signal",
}

# Layer-specific framing for counter-signal sentences: (subject, verb)
_LAYER_CONTEXT: dict[str, tuple[str, str]] = {
    "market":     ("Near-term technical conditions", "show"),
    "narrative":  ("Market commentary",              "shows"),
    "influencer": ("Insider and analyst activity",   "shows"),
    "macro":      ("The macro environment",          "shows"),
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _phrase(driver: DriverRecord) -> str:
    """Return the template phrase fragment for this driver."""
    variants = _PHRASES.get(driver.signal, _DEFAULT_PHRASE)
    return variants.get(driver.direction, _DEFAULT_PHRASE.get(driver.direction, "a mixed signal"))


def _qualifier(magnitude: float) -> str:
    """Return a strength qualifier based on magnitude (0–1)."""
    if magnitude >= 0.70:
        return "strong "
    if magnitude >= 0.40:
        return "moderate "
    return "mild "


def _lead_sentence(drivers: list[DriverRecord]) -> str:
    """Build the primary 'Sentiment is driven by …' sentence from 1–2 drivers."""
    if len(drivers) == 1:
        d = drivers[0]
        return f"Sentiment is driven by {_qualifier(d.magnitude)}{_phrase(d)}."
    d1, d2 = drivers[0], drivers[1]
    return (
        f"Sentiment is primarily driven by {_qualifier(d1.magnitude)}{_phrase(d1)}"
        f" and {_qualifier(d2.magnitude)}{_phrase(d2)}."
    )


def _counter_sentence(driver: DriverRecord) -> str:
    """Build the counter-signal sentence using layer-specific framing."""
    default = ("An offsetting factor", "shows")
    subject, verb = _LAYER_CONTEXT.get(driver.source_layer, default)
    return f"{subject} {verb} {_qualifier(driver.magnitude)}{_phrase(driver)}."


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_explanation(drivers: list[DriverRecord]) -> str:
    """
    Generate a 1–3 sentence plain-English explanation from ranked drivers.

    Only signals present in `drivers` are referenced — no invented reasons.

    Parameters
    ----------
    drivers : list[DriverRecord]
        Importance-ranked list from extract_drivers(). May be empty.

    Returns
    -------
    str
        Plain-English explanation suitable for the API response field.
    """
    if not drivers:
        return "Insufficient signal data to generate an explanation."

    # Separate non-neutral drivers
    active = [d for d in drivers if d.direction != "neutral"]

    if not active:
        # All signals near-neutral
        d = drivers[0]
        return (
            f"Sentiment is near neutral. "
            f"The primary input is {_qualifier(d.magnitude)}{_phrase(d)}."
        )

    primary_dir = active[0].direction

    # Drivers aligned with the primary direction (capped at 2 for lead sentence)
    aligned = [d for d in active if d.direction == primary_dir][:2]

    # First opposing driver with meaningful magnitude
    opposing = next(
        (d for d in active if d.direction != primary_dir and d.magnitude >= 0.3),
        None,
    )

    lead = _lead_sentence(aligned)

    if opposing is None:
        return lead

    return f"{lead} {_counter_sentence(opposing)}"
