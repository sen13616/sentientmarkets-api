"""
pipeline/scoring/subindices.py

Computes a single sub-index value (0–100) from a collection of pre-scored,
pre-weighted signals belonging to one data layer.

Generic formula (spec §Mathematics)
------------------------------------
    raw   = Σ(w_i × score_i) / Σ(w_i)         weighted average
    value = 50 + min(1, n/5) × (raw − 50)      volume shrinkage toward neutral

The shrinkage factor `min(1, n/5)` pulls the result toward 50 when fewer
than 5 signals are present, guarding against single-signal overconfidence.

Market sub-index (structured 6-component aggregation)
------------------------------------------------------
    The market layer uses a dedicated ``compute_market_sub_index()`` that
    combines six component groups with explicit weights:
        returns (0.30), momentum (0.15), order_flow (0.20),
        liquidity (0.10), short_volume (0.15), volume (0.10).
    Missing components have their weight redistributed proportionally.

Expected input per signal dict
-------------------------------
    score  : float in [0, 100]  — z-scaled, direction-corrected (50 = neutral)
    weight : float > 0          — combined w_source × w_confidence × w_time
    source : str                — data source label
    signal_type : str           — e.g. "rsi_14", "return_1d" (used by market sub-index)
    value  : float              — raw un-normalized value (used for RSI momentum conversion)

Returns None (missing layer) when the signal list is empty or all weights
are zero.  The orchestration layer redistributes composite weight accordingly.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

_log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SubIndexResult:
    value: float  # shrinkage-adjusted sub-index, 0–100
    n_signals: int  # number of valid contributing signals
    sources: list[str] = field(default_factory=list, compare=False)


def compute_sub_index(
    signals: list[dict],
    shrinkage_denominator: float = 5.0,
) -> SubIndexResult | None:
    """
    Compute a layer sub-index from pre-scored, pre-weighted signal dicts.

    Parameters
    ----------
    signals : list[dict]
        Each dict must contain:
          - 'score'  (float) : 0–100, direction-corrected, 50 = neutral
          - 'weight' (float) : combined w_source × w_confidence × w_time
          - 'source' (str)   : data source label
    shrinkage_denominator : float, default 5.0
        Controls the volume-shrinkage curve: ``shrinkage = min(1, n / d)``.
        Narrative / influencer use the default (5) per existing convention.
        Macro layer overrides to 2 (Sprint P4.2 Decision 7 Option C) so that
        2 present signals — the realistic floor before P4.3 adds FRED data —
        already produce no shrinkage. P4.4 will replace this whole codepath
        for macro with the dedicated ``compute_macro_sub_index``.

    Returns
    -------
    SubIndexResult or None when no valid signals exist (missing layer).
    """
    valid = [s for s in signals if (s.get("weight") or 0) > 0]
    if not valid:
        return None

    total_w = sum(s["weight"] for s in valid)
    if total_w == 0:
        return None

    raw = sum(s["score"] * s["weight"] for s in valid) / total_w

    n = len(valid)
    shrinkage = min(1.0, n / shrinkage_denominator)
    value = 50.0 + shrinkage * (raw - 50.0)

    sources = sorted({s["source"] for s in valid})
    return SubIndexResult(
        value=round(value, 4),
        n_signals=n,
        sources=sources,
    )


# ---------------------------------------------------------------------------
# Market sub-index — structured 6-component aggregation
# ---------------------------------------------------------------------------

# Tunable component weights — will be revisited during empirical validation
# in the research paper Phase C.  Must sum to 1.0.
# Sprint 2 (G-S3): volume added as 6th component at 0.10; returns reduced
# from 0.35→0.30 and order_flow from 0.25→0.20 to compensate.
MARKET_COMPONENT_WEIGHTS: dict[str, float] = {
    "returns": 0.30,
    "momentum": 0.15,
    "order_flow": 0.20,
    "liquidity": 0.10,
    "short_volume": 0.15,
    "volume": 0.10,
}

# Signal types that feed each component
_RETURNS_TYPES = frozenset({"return_1d", "return_5d", "return_20d"})
_COMPONENT_TYPES = _RETURNS_TYPES | {
    "rsi_14",
    "order_flow_imbalance",
    "bid_ask_spread_bps",
    "short_volume_ratio_otc",
    "volume_ratio",
}


def compute_market_sub_index(signals: list[dict]) -> SubIndexResult | None:
    """
    Compute the market sub-index using a structured 6-component weighted
    average.

    Components (all converted to [-1, +1] before weighting)
    ---------------------------------------------------------
        returns      (0.30) — mean of return_1d / return_5d / return_20d scores
        momentum     (0.15) — rsi_14 as a *momentum* indicator (NOT contrarian).
                              RSI 70 → +1 (strong upward momentum = bullish).
                              This is intentional: the sub-index treats RSI as a
                              trend-following signal.  The contrarian interpretation
                              (overbought = bearish) is surfaced in the driver
                              description instead.
        order_flow   (0.20) — order_flow_imbalance (CLV); close near high = +1
        liquidity    (0.10) — bid_ask_spread_bps; wider spread = -1 (bearish)
        short_volume (0.15) — short_volume_ratio_otc z-score; higher recent
                              short volume relative to 20d history = bearish.
                              Already negated by the normalizer: score > 50 = bullish.
        volume       (0.10) — volume_ratio (current / 20d avg); elevated volume =
                              mild bullish (market interest); score already in [20, 80].

    Missing-signal handling
    -----------------------
    If a component has no valid signals, its weight is redistributed
    proportionally across present components (avoids biasing toward neutral).
    If 5 of 6 components are missing the layer is considered missing entirely
    and None is returned.

    Parameters
    ----------
    signals : list[dict]
        Pre-scored signal dicts from ``score_market_signals()``.  Only those
        with signal_type in the six component groups contribute to the
        sub-index; others (buy_pressure, sell_pressure, etc.) are silently
        ignored here but still available for driver extraction.

    Returns
    -------
    SubIndexResult or None when fewer than 2 components are present.
    """
    # ── Group valid signals by signal_type ────────────────────────────────
    by_type: dict[str, list[dict]] = {}
    for s in signals:
        if (s.get("weight") or 0) > 0:
            by_type.setdefault(s["signal_type"], []).append(s)

    # ── Build per-component values in [-1, +1] ───────────────────────────
    components: dict[str, float] = {}

    # Returns — average of available return periods, each already in [0,100]
    ret_vals: list[float] = []
    for rt in ("return_1d", "return_5d", "return_20d"):
        if rt in by_type:
            best = by_type[rt][0]  # most recent (DESC from DB)
            ret_vals.append((best["score"] - 50.0) / 50.0)
    if ret_vals:
        components["returns"] = sum(ret_vals) / len(ret_vals)

    # Momentum — raw RSI → [-1, +1] via momentum mapping
    # RSI 70 → +1, RSI 50 → 0, RSI 30 → -1
    if "rsi_14" in by_type:
        rsi_raw = float(by_type["rsi_14"][0]["value"])
        components["momentum"] = max(-1.0, min(1.0, (rsi_raw - 50.0) / 20.0))

    # Order flow — CLV already direction-correct in scorer (> 50 = bullish)
    if "order_flow_imbalance" in by_type:
        s = by_type["order_flow_imbalance"][0]
        components["order_flow"] = (s["score"] - 50.0) / 50.0

    # Liquidity — wider spread = lower score (bearish) in scorer
    if "bid_ask_spread_bps" in by_type:
        s = by_type["bid_ask_spread_bps"][0]
        components["liquidity"] = (s["score"] - 50.0) / 50.0

    # Short volume — z-score already direction-corrected by normalizer
    # (high short volume = bearish → low score; low short volume = bullish → high score)
    if "short_volume_ratio_otc" in by_type:
        s = by_type["short_volume_ratio_otc"][0]
        components["short_volume"] = (s["score"] - 50.0) / 50.0

    # Volume — volume_ratio score already in [20, 80] from normalizer
    if "volume_ratio" in by_type:
        s = by_type["volume_ratio"][0]
        components["volume"] = (s["score"] - 50.0) / 50.0

    # ── 5 of 6 missing → layer missing ───────────────────────────────────
    if len(components) <= 1:
        return None

    # ── Redistribute weights across present components ────────────────────
    present_w = sum(MARKET_COMPONENT_WEIGHTS[c] for c in components)
    effective = {c: MARKET_COMPONENT_WEIGHTS[c] / present_w for c in components}

    # ── Weighted average in [-1, +1] space ────────────────────────────────
    combined = sum(effective[c] * components[c] for c in components)

    # ── Convert to [0, 100] ───────────────────────────────────────────────
    value = 50.0 + 50.0 * max(-1.0, min(1.0, combined))

    # ── Collect metadata ──────────────────────────────────────────────────
    valid = [
        s
        for s in signals
        if (s.get("weight") or 0) > 0 and s.get("signal_type") in _COMPONENT_TYPES
    ]
    sources = sorted({s["source"] for s in valid})

    return SubIndexResult(
        value=round(value, 4),
        n_signals=len(valid),
        sources=sources,
    )


# ---------------------------------------------------------------------------
# Macro sub-index — paper-direct weighted average, no shrinkage (Sprint P4.4)
# ---------------------------------------------------------------------------

#: Paper §Macroeconomic Signals weight table (Research Paper v6, transcribed
#: directly from the §Macroeconomic Signals → Aggregation subsection). Sum =
#: 5.25; the aggregator normalises by the sum of present weights so that
#: missing signals are absorbed without biasing toward 50. The per-signal
#: weights are NOT equal because the paper assigns higher influence to the
#: sector-level signal (`sector_etf_return_20d`, w=1.5) — this is the only
#: per-ticker macro input and the paper argues it carries the largest fraction
#: of the macro information per ticker. All other inputs are market-wide.
#:
#: `ted_spread` here is the `T10Y2Y` substitute introduced by Sprint P4.3
#: (pipeline/sources/fred.py). The original 3M-LIBOR–3M-T-bill TED spread was
#: discontinued in 2022 with LIBOR; the paper text needs to be updated to
#: reflect the substitution and is queued for the next paper-edit cycle.
_MACRO_SIGNAL_WEIGHTS: dict[str, float] = {
    "vix": 1.0,  # Paper §Macroeconomic Signals — risk gauge
    "sector_etf_return_20d": 1.5,  # Paper §Macroeconomic Signals — sector trend; only per-ticker macro input
    "treasury_yield_10y": 1.0,  # Paper §Macroeconomic Signals — long-end rates
    "treasury_yield_2y": 0.75,  # Paper §Macroeconomic Signals — short-end rates (lower weight: high-correlation with 10y)
    "ted_spread": 1.0,  # Paper §Macroeconomic Signals — yield-curve / credit-stress proxy (T10Y2Y substitute)
}


def compute_macro_sub_index(signals: list[dict]) -> SubIndexResult | None:
    """
    Macro layer aggregator (paper §Macroeconomic Signals).

    Differences from the generic ``compute_sub_index``:
      • Per-signal-type weights from `_MACRO_SIGNAL_WEIGHTS` (paper table)
      • **No `min(1, n/d)` shrinkage** — paper: "macro signals are market-wide
        state variables that are available at every scoring tick without
        exception, and the epistemic-caution logic that motivates shrinkage
        in event-driven channels does not apply here."
      • Missing components: weight redistributed proportionally over present
        ones (denominator = sum of present-component weights). Mathematically
        equivalent to imputing missing signals at the weighted-average of
        the present ones — sane default that preserves direction.

    Sprint P4.4 replaces the P4.2-era ``compute_sub_index(sigs,
    shrinkage_denominator=2)`` call site in ``_score_macro``. The
    ``shrinkage_denominator`` parameter on ``compute_sub_index`` is retained
    in the public API for narrative/influencer/event-driven layers; only the
    macro path stops using it.

    Returns
    -------
    SubIndexResult with ``value`` in [0, 100] and ``n_signals`` = number of
    present components, or ``None`` if no recognised macro signals are
    present (caller falls back to Redis cache via ``_fallback_subindex``).
    """
    # Group valid signals by signal_type; if a signal_type appears more than
    # once, prefer the row whose underlying timestamp is most recent (mirrors
    # the convention in compute_market_sub_index).
    grouped: dict[str, dict] = {}
    for sig in signals:
        sig_type = sig.get("signal_type")
        if sig_type not in _MACRO_SIGNAL_WEIGHTS:
            continue
        if (sig.get("weight") or 0) <= 0:
            continue
        existing = grouped.get(sig_type)
        if existing is None:
            grouped[sig_type] = sig
            continue
        # Pick the newer of the two; defaults to keeping `existing` on a tie
        # since the input ordering already encodes recency (DESC from DB).
        sig_ts = sig.get("timestamp")
        existing_ts = existing.get("timestamp")
        if sig_ts is not None and existing_ts is not None and sig_ts > existing_ts:
            grouped[sig_type] = sig

    if not grouped:
        return None

    present_w = sum(_MACRO_SIGNAL_WEIGHTS[t] for t in grouped)
    effective = {t: _MACRO_SIGNAL_WEIGHTS[t] / present_w for t in grouped}

    value = sum(effective[t] * grouped[t]["score"] for t in grouped)

    _log.debug(
        "compute_macro_sub_index: components=%s effective_weights=%s value=%.2f",
        sorted(grouped),
        {k: round(v, 3) for k, v in effective.items()},
        value,
    )

    sources = sorted({s.get("source", "unknown") for s in grouped.values()})
    return SubIndexResult(
        value=round(value, 4),
        n_signals=len(grouped),
        sources=sources,
    )
