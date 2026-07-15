"""
pipeline/scoring/drivers.py

After sub-index computation, extract the top N signals across all layers
ranked by importance and format them as structured driver records.

Driver schema (spec §Layer 08)
-------------------------------
{
    "signal":       str,   # human-readable signal name
    "description":  str,   # one-line plain-English description of the value
    "direction":    str,   # "bullish" | "bearish" | "neutral"
    "magnitude":    float, # 0–1  (0 = indifferent, 1 = extreme)
    "source_layer": str,   # "market" | "narrative" | "influencer" | "macro"
    "confidence":   float, # 0–1, derived from combined signal weight
}

Importance ranking
------------------
    importance_i = weight_i × |score_i − 50| / 50

A high-weight signal that scores near 50 (neutral) does not drive the
composite — only signals with both high weight AND strong directional
conviction appear at the top.

Expected input per signal dict
-------------------------------
    signal_type : str   — e.g. "rsi_14", "insider_net_shares"
    value       : float — raw (un-normalized) value for description generation
    score       : float — 0–100 scaled, direction-corrected
    weight      : float — combined w_source × w_confidence × w_time  (> 0)
    source      : str   — data source label
    layer       : str   — "market" | "narrative" | "influencer" | "macro"
    ticker      : str   — (optional) ticker symbol, used in descriptions
"""
from __future__ import annotations

from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Signal metadata — human-readable labels
# ---------------------------------------------------------------------------

_SIGNAL_LABELS: dict[str, str] = {
    "rsi_14":                "RSI(14)",
    "ohlcv_close":           "Price",
    "ohlcv_volume":          "Volume",
    "yf_close":              "Price",
    "yf_volume":             "Volume",
    "return_1d":             "Price momentum (1d)",
    "return_5d":             "Price momentum (5d)",
    "return_20d":            "Price momentum (20d)",
    "volume_ratio":          "Volume surge",
    # Removed Stage 1 — put_call_ratio, short_interest_ratio, implied_volatility
    # excluded from paper's implemented methodology. See docs/SIGNAL_CATALOG.md.
    "insider_net_shares":    "Insider transaction",
    "analyst_buy_pct":       "Analyst consensus",
    "analyst_target_price":  "Analyst price target",
    "vix":                   "VIX",
    "sector_etf_return_20d": "Sector trend (20d)",
    "order_flow_imbalance":  "Order flow",
    "buy_pressure":          "Buy pressure",
    "sell_pressure":         "Sell pressure",
    "bid_ask_spread_bps":    "Bid-ask spread",
    "bid_ask_spread":        "Bid-ask spread (raw)",
    "bid":                   "Bid price",
    "ask":                   "Ask price",
    "short_volume_ratio_otc": "Short volume ratio",
}


def _label(signal_type: str) -> str:
    return _SIGNAL_LABELS.get(signal_type, signal_type.replace("_", " ").title())


def _describe(signal_type: str, value: float, ticker: str = "") -> str:
    """Generate a plain-English one-line description from signal_type + raw value."""
    t = f" for {ticker}" if ticker else ""

    if signal_type == "rsi_14":
        zone = "overbought" if value > 70 else "oversold" if value < 30 else "neutral"
        return f"RSI(14){t} = {value:.1f} ({zone})"

    if signal_type == "return_1d":
        return f"1-day price return{t}: {value:+.2%}"

    if signal_type == "return_5d":
        return f"5-day price return{t}: {value:+.2%}"

    if signal_type == "return_20d":
        return f"20-day price return{t}: {value:+.2%}"

    if signal_type == "volume_ratio":
        return f"Volume{t} is {value:.1f}× the 20-day average"

    if signal_type == "insider_net_shares":
        action = "purchased" if value > 0 else "sold"
        return f"Insider {action} {abs(int(value)):,} shares{t}"

    if signal_type == "analyst_buy_pct":
        return f"Analyst consensus{t}: {value:.0%} buy/outperform ratings"

    if signal_type == "analyst_target_price":
        return f"Analyst consensus price target{t}: ${value:,.2f}"

    if signal_type == "vix":
        regime = "elevated fear" if value > 25 else "low volatility" if value < 15 else "normal"
        return f"VIX at {value:.1f} ({regime})"

    if signal_type == "sector_etf_return_20d":
        return f"Sector 20-day return: {value:+.2%}"

    if signal_type == "order_flow_imbalance":
        side = "buy-side" if value > 0 else "sell-side" if value < 0 else "balanced"
        return f"Order flow imbalance{t}: {value:+.2f} CLV ({side})"

    if signal_type == "buy_pressure":
        return f"Buy pressure{t}: {value:.2f} ({value * 100:.1f}%)"

    if signal_type == "sell_pressure":
        return f"Sell pressure{t}: {value:.2f} ({value * 100:.1f}%)"

    if signal_type == "bid_ask_spread_bps":
        width = "tight" if value < 10 else "wide" if value > 50 else "moderate"
        return f"Bid-ask spread{t}: {value:.1f} bps ({width})"

    if signal_type == "short_volume_ratio_otc":
        level = "elevated" if value > 0.55 else "low" if value < 0.35 else "normal"
        return f"Short volume ratio (OTC){t}: {value:.1%} ({level})"

    # Generic fallback
    return f"{_label(signal_type)}{t}: {value:.4g}"


# ---------------------------------------------------------------------------
# Driver record
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DriverRecord:
    signal:       str
    description:  str
    direction:    str    # "bullish" | "bearish" | "neutral"
    magnitude:    float  # 0–1
    source_layer: str
    confidence:   float  # 0–1

    def to_dict(self) -> dict:
        return {
            "signal":       self.signal,
            "description":  self.description,
            "direction":    self.direction,
            "magnitude":    self.magnitude,
            "source_layer": self.source_layer,
            "confidence":   self.confidence,
        }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_drivers(
    signals: list[dict],
    top_n: int = 5,
) -> list[DriverRecord]:
    """
    Extract the top `top_n` signals ranked by importance and return them
    as structured DriverRecord objects.

    Importance formula
    ------------------
        importance = weight × |score − 50| / 50

    Parameters
    ----------
    signals : list[dict]
        Each dict must contain:
          - signal_type (str)
          - value       (float)  — raw un-normalized value
          - score       (float)  — 0–100, direction-corrected
          - weight      (float)  — combined w, must be > 0
          - source      (str)
          - layer       (str)    — "market"|"narrative"|"influencer"|"macro"
          - ticker      (str, optional)

    top_n : int
        Maximum number of drivers to return. Defaults to 5.

    Returns
    -------
    list[DriverRecord] sorted by importance descending, length ≤ top_n.
    """
    # Compute importance for every signal
    scored: list[tuple[float, dict]] = []
    for sig in signals:
        weight = sig.get("weight") or 0
        score  = sig.get("score", 50.0)
        if weight <= 0:
            continue
        importance = weight * abs(score - 50.0) / 50.0
        scored.append((importance, sig))

    scored.sort(key=lambda x: x[0], reverse=True)

    # Deduplicate: keep only the highest-importance instance per signal_type
    seen_types: set[str] = set()
    deduped: list[tuple[float, dict]] = []
    for importance, sig in scored:
        sig_type = sig.get("signal_type", "unknown")
        if sig_type in seen_types:
            continue
        seen_types.add(sig_type)
        deduped.append((importance, sig))

    drivers: list[DriverRecord] = []
    for importance, sig in deduped[:top_n]:
        score   = sig.get("score", 50.0)
        weight  = sig.get("weight", 0.0)
        sig_type = sig.get("signal_type", "unknown")
        value   = sig.get("value", 0.0)
        layer   = sig.get("layer", "")
        ticker  = sig.get("ticker", "")

        if score > 52:
            direction = "bullish"
        elif score < 48:
            direction = "bearish"
        else:
            direction = "neutral"

        magnitude  = round(abs(score - 50.0) / 50.0, 4)
        confidence = round(min(1.0, weight), 4)

        drivers.append(DriverRecord(
            signal       = _label(sig_type),
            description  = _describe(sig_type, value, ticker),
            direction    = direction,
            magnitude    = magnitude,
            source_layer = layer,
            confidence   = confidence,
        ))

    return drivers
