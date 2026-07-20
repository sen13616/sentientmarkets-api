"""
pipeline/features/normalize.py  — Layer 07

Converts raw signal values fetched from the database into scored, weighted
signal dicts that can be passed to pipeline.scoring.subindices.compute_sub_index()
and pipeline.scoring.drivers.extract_drivers().

Per-signal weight formula (paper §Event-Level Weighting)
--------------------------------------------------------
The canonical formula stated by the paper is:

    w_i  =  w_src · w_rel · w_conf · w_author · exp(−λ · Δt_i)

Term-by-term: where it lives in this module, paper anchor, current state.

    w_src     Source-credibility weight. Source-keyed by default via
              `_SOURCE_WEIGHTS`. Influencer channel keys by SIGNAL CHANNEL
              instead (insider / analyst-consensus / analyst-target /
              earnings-revisions) via `_INFLUENCER_SIGNAL_WEIGHT`; the
              dispatcher is `_get_signal_weight`. Paper §Event-Level Weighting
              + paper Data-Collection source-hierarchy table.

    w_rel     Relevance weight. Narrative channel only. Set per-article from
              `raw_articles.relevance_score`. Articles below the inclusion
              threshold (0.60, Sprint D) are excluded entirely before this
              function is called. Paper Stage 2.

    w_conf    Model-confidence weight. Narrative channel only. Computed from
              FinBERT class probabilities as
                  w_conf = 1 − (−Σ P_i ln P_i) / ln 3
              by `_compute_w_conf`. Set to 1.00 for all non-textual signals
              per paper: "Model confidence does not apply to non-textual
              signals and is set to 1.00 throughout." Paper §Event-Level
              Weighting (Sprint A).

    w_author  Author-credibility weight. Influencer channel only;
              `_INFLUENCER_W_AUTHOR = 1.00` uniformly today. Scaffold for the
              role-based hierarchy (CEO → CFO → Director …) that the paper
              explicitly defers to Future Additions. Narrative-channel
              `w_author` is folded into `w_src` per paper §Event-Level
              Weighting and is not a standalone term in the narrative weight.

    Δt_i      Age of the signal at scoring time = `now − timestamp`.

    λ         ln(2) / half_life. Half-life lookup:
                - `_LAYER_HALF_LIFE_H` (layer default, paper §Historical
                  Data),
                - `_HALF_LIFE_OVERRIDE` (currently empty; reserved for
                  future (layer, source) overrides),
                - `_INFLUENCER_SIGNAL_HALF_LIFE_H` (signal-channel override
                  for the influencer layer; today only insider rows at
                  168 h, paper §Event-Level Weighting).
              The dispatcher is `_get_half_life`.

The formula is applied per layer in `_build` (non-narrative path) and in
`score_narrative_articles` (narrative path). The narrative path is the only
place where w_rel and w_conf are populated; everywhere else they are
implicitly 1.0 and drop out of the product.

Output dict schema (per signal)
--------------------------------
    signal_type : str    — e.g. "rsi_14"
    value       : float  — raw value (preserved for driver descriptions)
    score       : float  — direction-corrected [0, 100], 50 = neutral
    weight      : float  — combined w_src · w_rel · w_conf · w_author · exp(−λΔt)
    source      : str    — data source label
    layer       : str    — "market" | "narrative" | "influencer" | "macro"
    ticker      : str    — ticker symbol

Scoring conventions
-------------------
- All scores are direction-corrected: > 50 = bullish, < 50 = bearish.
- Time decay half-lives: market 1h, news 12h, analyst 3d, insider 7d,
  macro 14d (Sprint 4, G-S1; paper §Historical Data).
- Source weights: exchange-grade data (yfinance, Polygon, FINRA) = 0.85–0.90;
  third-party news aggregators (AV, Finnhub) = 0.65–0.75. See
  `_SOURCE_WEIGHTS` below.
- Z-score normalization (Sprint 4, G-C1; paper §Normalization): signals with
  sufficient history (≥ `fill_threshold · window` observations, default
  50%) use adaptive z-score via `RollingZScorer`; others fall back to fixed
  parametric scorers.
"""
from __future__ import annotations

import logging
import math
from datetime import datetime, timezone

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source weights
# ---------------------------------------------------------------------------

_SOURCE_WEIGHTS: dict[str, float] = {
    "alpha_vantage": 0.75,   # Paper §Event-Level Weighting (Sprint A)
    "polygon":       0.9,
    "finnhub":       0.65,   # Paper §Event-Level Weighting (Sprint A)
    "computed":      0.85,
    # Removed Stage 1 — "newsapi" excluded from paper's implemented methodology. See docs/SIGNAL_CATALOG.md.
    # Removed Phase 5 retraction — "sec_edgar" entry dropped 2026-05-16; Sprint C (EDGAR 8-K narrative source)
    # never merged to main. 8-K filings move to Future Additions; the source label is no longer written by
    # any active ingester (Form 4 was removed in P3.4; insider data now comes solely from Finnhub).
    "yfinance":      0.9,
    "finra_regsho":  0.9,
}

# ---------------------------------------------------------------------------
# Per-source half-lives (Sprint 4, G-S1)
#
# Research paper §Historical Data specifies per-category decay rates.
# Layer-level defaults with source-specific overrides where the same source
# appears in multiple layers with different decay characteristics.
# ---------------------------------------------------------------------------

_LAYER_HALF_LIFE_H: dict[str, float] = {
    "market":     1.0,      # 60 min
    "narrative":  12.0,     # 12 hours
    "influencer": 72.0,     # 3 days (analyst default)
    "macro":      336.0,    # 14 days
}

# Overrides for specific (layer, source) combinations.
# Currently empty. The historical `("influencer", "sec_edgar"): 168.0` override was
# removed in Sprint P3.4 when the EDGAR Form 4 primary path was deleted (insider data
# now comes solely from Finnhub); the 168h insider half-life is applied via
# `_INFLUENCER_SIGNAL_HALF_LIFE_H["insider_net_shares"]` below (P3.1, I4). Phase 5
# retraction (2026-05-16) confirmed that `sec_edgar` is no longer a wired source —
# Sprint C (EDGAR 8-K narrative) was drafted but never merged and has been moved to
# Future Additions. Kept as an empty dict so future (layer, source) overrides have a home.
_HALF_LIFE_OVERRIDE: dict[tuple[str, str], float] = {}

# Influencer-layer signal-type overrides (Sprint P3.1, paper §Event-Level Weighting).
# Paper specifies half-life by *signal channel*, not data provider. Takes precedence
# over the (layer, source) table above.
_INFLUENCER_SIGNAL_HALF_LIFE_H: dict[str, float] = {
    "insider_net_shares": 168.0,   # Paper: insider transactions = 7 days regardless of provider
    # analyst_buy_pct, analyst_target_price, earnings_estimate_revision use 72h layer default
}


def _get_half_life(layer: str, source: str, signal_type: str | None = None) -> float:
    """Look up half-life. Priority: (layer, signal_type) → (layer, source) → layer default."""
    if signal_type is not None and layer == "influencer":
        sig_hl = _INFLUENCER_SIGNAL_HALF_LIFE_H.get(signal_type)
        if sig_hl is not None:
            return sig_hl
    return _HALF_LIFE_OVERRIDE.get(
        (layer, source.lower()),
        _LAYER_HALF_LIFE_H.get(layer, 48.0),
    )


def _time_weight(ts: datetime, now: datetime, half_life_h: float = 48.0) -> float:
    """Return exponential decay weight.  Fresh signal = 1.0; half_life_h old = 0.5."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    age_h = max(0.0, (now - ts).total_seconds() / 3600)
    lam = math.log(2) / half_life_h
    return math.exp(-lam * age_h)


def _source_weight(source: str) -> float:
    return _SOURCE_WEIGHTS.get(source.lower(), 0.70)


# Influencer-layer signal-type weight overrides (Sprint P3.1, paper §Event-Level Weighting).
# Paper specifies w_src by signal channel (insider / analyst-consensus / target-price /
# earnings-revisions), not by data provider. Takes precedence over `_SOURCE_WEIGHTS`
# when layer == "influencer".
_INFLUENCER_SIGNAL_WEIGHT: dict[str, float] = {
    "insider_net_shares":         1.00,   # Paper: insider transactions
    "analyst_buy_pct":            0.85,   # Paper: analyst consensus
    "analyst_target_price":       0.85,   # Paper: analyst target price
    "earnings_estimate_revision": 0.80,   # Paper: earnings revisions (signal lands in P3.3)
}

# Influencer event-weight scaffolds (Sprint P3.1, paper §Event-Level Weighting).
# w_author = 1.00 uniformly today; scaffold for the deferred role-based hierarchy
# (CEO → CFO → Director …) which the paper places in Future Additions.
# w_conf  = 1.00 explicitly for non-textual influencer signals — paper: "Model
# confidence w_conf does not apply to non-textual signals and is set to 1.00
# throughout".
_INFLUENCER_W_AUTHOR: float = 1.0
_INFLUENCER_W_CONF:   float = 1.0


def _get_signal_weight(layer: str, source: str, signal_type: str) -> float:
    """Return w_src for a signal.

    For influencer layer, paper specifies weights keyed by signal channel rather
    than provider; the channel weight takes precedence. Other layers fall through
    to the per-source table.
    """
    if layer == "influencer":
        sig_w = _INFLUENCER_SIGNAL_WEIGHT.get(signal_type)
        if sig_w is not None:
            return sig_w
    return _source_weight(source)


# ---------------------------------------------------------------------------
# Rolling z-score normalizer (Sprint 4, G-C1)
# ---------------------------------------------------------------------------

class RollingZScorer:
    """
    Adaptive z-score normalizer.

    Computes z = (x - μ) / max(σ, σ_floor) from a historical window,
    clamps to ±3, and maps to [0, 100] (50 = neutral).

    Parameters
    ----------
    window         : Number of historical observations to fetch.
    min_obs        : Absolute minimum observations required; returns None if fewer.
    sigma_floor    : Minimum standard deviation to avoid division by near-zero.
    negate         : If True, negate z before mapping (for bearish-when-high
                     signals like VIX, RSI contrarian, bid-ask spread, short volume).
    fill_threshold : Fraction of ``window`` that must be present before z-score
                     activates.  Effective minimum = max(min_obs, ceil(window *
                     fill_threshold)).  Default 0.5 ensures the z-score baseline
                     is computed against a meaningful history span.
    """

    __slots__ = ("window", "min_obs", "sigma_floor", "negate", "fill_threshold")

    def __init__(
        self,
        window: int,
        min_obs: int = 30,
        sigma_floor: float = 1e-4,
        negate: bool = False,
        fill_threshold: float = 0.5,
    ) -> None:
        self.window = window
        self.min_obs = min_obs
        self.sigma_floor = sigma_floor
        self.negate = negate
        self.fill_threshold = fill_threshold

    @property
    def effective_min_obs(self) -> int:
        """Minimum observations considering both min_obs and fill_threshold."""
        import math
        return max(self.min_obs, math.ceil(self.window * self.fill_threshold))

    def score_from_history(
        self, history: list[float], current_value: float
    ) -> float | None:
        """
        Pure z-score computation from a pre-fetched history list.

        Parameters
        ----------
        history       : Historical values (oldest first), up to ``window``.
        current_value : The value to normalize.

        Returns
        -------
        float in [0, 100] or None if insufficient data / variance below floor.
        """
        if len(history) < self.effective_min_obs:
            return None

        n = len(history)
        mean = sum(history) / n
        variance = sum((x - mean) ** 2 for x in history) / n
        std = variance ** 0.5

        if std < self.sigma_floor:
            return None

        z = (current_value - mean) / std
        z = max(-3.0, min(3.0, z))

        if self.negate:
            z = -z

        return 50.0 + 50.0 * (z / 3.0)


# Per-signal-type z-score configuration.
# Signals not listed here use parametric scorers only.
_ZSCORE_CONFIG: dict[str, RollingZScorer] = {
    # Intra-day market signals: window=500 observations
    "rsi_14":               RollingZScorer(window=500, negate=True),
    "return_1d":            RollingZScorer(window=500),
    "return_5d":            RollingZScorer(window=500),
    "return_20d":           RollingZScorer(window=500),
    "volume_ratio":         RollingZScorer(window=500),
    "order_flow_imbalance": RollingZScorer(window=500),
    "bid_ask_spread_bps":   RollingZScorer(window=500, negate=True),
    # Daily-cadence signals: window=90 observations
    "short_volume_ratio_otc": RollingZScorer(window=90, negate=True),
    "insider_net_shares":     RollingZScorer(window=90),
    "analyst_buy_pct":        RollingZScorer(window=90),
    "analyst_target_price":   RollingZScorer(window=90, fill_threshold=0.5),
    "earnings_estimate_revision": RollingZScorer(window=90),
    "vix":                    RollingZScorer(window=90, negate=True),
    # Sprint P4.2: per-ETF rolling z-score; history is keyed under the ETF
    # symbol (XLK, XLE, …), not the ticker being scored. ~22 daily obs per
    # ETF today → parametric fallback runs for ~3 more weeks.
    "sector_etf_return_20d":  RollingZScorer(window=90),
    # Sprint P4.3: FRED Treasury / yield-curve signals. Stored under `_MACRO_`
    # alongside VIX, so history lookup uses the same ticker. The two YIELDS are
    # sign-inverted (rising yields are bearish for equities). `ted_spread` is
    # the 10y-2y SLOPE, NOT a yield: a positive/steep slope is bullish and an
    # inversion is bearish, so it is NOT negated — matching _score_ted_spread.
    "treasury_yield_10y":     RollingZScorer(window=90, negate=True),
    "treasury_yield_2y":      RollingZScorer(window=90, negate=True),
    "ted_spread":             RollingZScorer(window=90),
}


# ---------------------------------------------------------------------------
# Telemetry — z-score vs parametric fallback counts (Sprint 4)
# ---------------------------------------------------------------------------

_scoring_method_counts: dict[str, dict[str, int]] = {}


def _record_method(signal_type: str, method: str) -> None:
    """Record that a signal was scored via 'zscore' or 'parametric_fallback'."""
    if signal_type not in _scoring_method_counts:
        _scoring_method_counts[signal_type] = {"zscore": 0, "parametric_fallback": 0}
    _scoring_method_counts[signal_type][method] += 1


def reset_scoring_telemetry() -> None:
    """Clear telemetry counters.  Called at the start of each scoring tick."""
    _scoring_method_counts.clear()


def log_scoring_telemetry() -> None:
    """Log per-signal-type scoring method counts.  Called after each scoring tick."""
    if not _scoring_method_counts:
        return
    for sig_type in sorted(_scoring_method_counts):
        c = _scoring_method_counts[sig_type]
        _log.debug(
            "[telemetry] %s: zscore=%d parametric_fallback=%d",
            sig_type, c["zscore"], c["parametric_fallback"],
        )


# ---------------------------------------------------------------------------
# Per-signal-type parametric score functions  (all return float in [0, 100])
#
# These are the FALLBACK path when z-score has insufficient history (< 30 obs).
# Do not delete — they remain active until enough data accumulates per signal.
# ---------------------------------------------------------------------------

def _score_rsi(rsi: float) -> float:
    """RSI 30 → 75 (bullish), 50 → 50 (neutral), 70 → 25 (bearish)."""
    return max(0.0, min(100.0, 50.0 - 1.25 * (rsi - 50.0)))


def _score_return(ret: float, scale: float) -> float:
    """Price return → score.  Positive return = bullish (> 50)."""
    return max(0.0, min(100.0, 50.0 + 50.0 * math.tanh(ret / scale)))


def _score_volume_ratio(vr: float) -> float:
    """Elevated volume = mild positive (market interest). Neutral at 1×."""
    return max(20.0, min(80.0, 50.0 + 15.0 * math.tanh(vr - 1.0)))


# Removed Stage 1 — _score_put_call, _score_short_interest, _score_implied_vol
# excluded from paper's implemented methodology. See docs/SIGNAL_CATALOG.md.


def _score_order_flow_imbalance(clv: float) -> float:
    """CLV (close location value) in [-1, 1]. Positive = close near high = bullish."""
    return max(0.0, min(100.0, 50.0 + 50.0 * clv))


def _score_buy_pressure(x: float) -> float:
    """Buy fraction in [0, 1]. 0.5 = neutral, 1.0 = bullish."""
    return max(0.0, min(100.0, 100.0 * x))


def _score_sell_pressure(x: float) -> float:
    """Sell fraction in [0, 1]. Inverted: 0.5 = neutral, 1.0 = bearish → score 0."""
    return max(0.0, min(100.0, 100.0 * (1.0 - x)))


def _score_bid_ask_spread_bps(bps: float) -> float:
    """Bid-ask spread in basis points. 10 bps neutral; wider = bearish (illiquidity)."""
    return max(0.0, min(100.0, 50.0 - 50.0 * math.tanh((bps - 10.0) / 30.0)))


def _score_insider_shares(shares: float) -> float:
    """Net insider shares bought (positive) / sold (negative)."""
    return max(0.0, min(100.0, 50.0 + 50.0 * math.tanh(shares / 100_000.0)))


def _score_analyst_buy_pct(pct: float) -> float:
    """Analyst buy/outperform pct (0–1). 50 % → 50, 100 % → 75, 0 % → 25."""
    return max(0.0, min(100.0, 25.0 + pct * 50.0))


def _score_analyst_target(target: float, current_price: float | None) -> float | None:
    """Analyst price target: compute upside vs current price and score."""
    if current_price is None or current_price <= 0:
        return None
    upside = (target - current_price) / current_price
    return max(0.0, min(100.0, 50.0 + 50.0 * math.tanh(upside / 0.15)))


def _score_earnings_revision_delta(delta: float) -> float:
    """Earnings estimate revision: period-over-period relative delta in mean EPS.

    Parametric cold-start fallback (paper Appendix 1.4 does not specify a
    formula for this signal; designed in Sprint P3.3 — flag for paper update).
    Half-scale point at ±5% revision. Z-score path activates once
    `earnings_estimate_revision` history exceeds the `RollingZScorer`
    `fill_threshold`.
    """
    return max(0.0, min(100.0, 50.0 + 50.0 * math.tanh(delta / 0.05)))


def _score_vix(vix: float) -> float:
    """VIX. Low fear = bullish (> 50). Linear around neutral at VIX=22."""
    return max(0.0, min(100.0, 50.0 - (vix - 22.0) / 8.0 * 25.0))


def _score_treasury_10y(yield_pct: float) -> float:
    """10-year Treasury yield. Rising yields = bearish for equities.

    Parametric cold-start fallback (Sprint P4.3). Neutral at 4.0%
    (approx. mid-2026 normal range), half-scale at ±1.5% deviation.
    Z-score path engages once 90-day history is available.
    """
    return max(0.0, min(100.0, 50.0 - 50.0 * math.tanh((yield_pct - 4.0) / 1.5)))


def _score_treasury_2y(yield_pct: float) -> float:
    """2-year Treasury yield. Rising yields = bearish for equities.

    Parametric cold-start fallback. Neutral at 4.5% (front-end runs
    slightly hotter than the 10y under normal Fed regimes), half-scale
    at ±1.5% deviation.
    """
    return max(0.0, min(100.0, 50.0 - 50.0 * math.tanh((yield_pct - 4.5) / 1.5)))


def _score_ted_spread(slope_pct: float) -> float:
    """TED spread substitute = 10y − 2y yield-curve slope.

    Negative slope (inversion) is the canonical recession signal →
    bearish for equities. We treat slope > 0 as bullish, slope < 0
    as bearish, with tanh saturation at ±1.0%.
    """
    return max(0.0, min(100.0, 50.0 + 50.0 * math.tanh(slope_pct / 1.0)))


# Lookup table for simple (single-argument) scorers
_SIMPLE_SCORERS: dict[str, object] = {
    "rsi_14":               _score_rsi,
    "return_1d":            lambda v: _score_return(v, 0.02),
    "return_5d":            lambda v: _score_return(v, 0.05),
    "return_20d":           lambda v: _score_return(v, 0.10),
    "volume_ratio":         _score_volume_ratio,
    # Removed Stage 1 — put_call_ratio, short_interest_ratio, implied_volatility
    # excluded from paper's implemented methodology. See docs/SIGNAL_CATALOG.md.
    "order_flow_imbalance": _score_order_flow_imbalance,
    "buy_pressure":         _score_buy_pressure,
    "sell_pressure":        _score_sell_pressure,
    "bid_ask_spread_bps":   _score_bid_ask_spread_bps,
    "insider_net_shares":   _score_insider_shares,
    "analyst_buy_pct":      _score_analyst_buy_pct,
    "vix":                  _score_vix,
    "sector_etf_return_20d": lambda v: _score_return(v, 0.10),
    # Sprint P4.3 — FRED Treasury / yield-curve signals
    "treasury_yield_10y":   _score_treasury_10y,
    "treasury_yield_2y":    _score_treasury_2y,
    "ted_spread":           _score_ted_spread,
}


# ---------------------------------------------------------------------------
# Signal dict builder
# ---------------------------------------------------------------------------

def _build(
    signal_type: str,
    value: float,
    score: float,
    source: str,
    timestamp: datetime,
    layer: str,
    ticker: str,
    now: datetime,
) -> dict:
    half_life = _get_half_life(layer, source, signal_type)
    w_src = _get_signal_weight(layer, source, signal_type)
    w = w_src * _time_weight(timestamp, now, half_life)
    if layer == "influencer":
        # Paper §Event-Level Weighting: w_i = w_src · w_author · w_conf · e^(−λΔt)
        # Both scaffolds are uniform 1.0 today (see _INFLUENCER_W_AUTHOR / _CONF).
        w = w * _INFLUENCER_W_AUTHOR * _INFLUENCER_W_CONF
    return {
        "signal_type": signal_type,
        "value":       value,
        "score":       round(score, 2),
        "weight":      round(max(w, 1e-6), 6),
        "source":      source,
        "layer":       layer,
        "ticker":      ticker,
    }


# ---------------------------------------------------------------------------
# Public scoring functions — one per layer
# ---------------------------------------------------------------------------

async def score_market_signals(
    ticker: str,
    raw: list[dict],
    now: datetime,
) -> list[dict]:
    """
    Convert raw market signal rows to scored dicts.

    Uses z-score normalization when sufficient history is available
    (≥ 30 observations); falls back to parametric scorers otherwise.

    Parameters
    ----------
    ticker : str
    raw    : rows from get_signals_since() — keys: signal_type, value, source, timestamp.
    now    : reference time for time decay.
    """
    from scripts.db.queries.raw_signals import get_signal_history

    result: list[dict] = []

    # Group by signal_type for efficient history fetch (one DB call per type)
    by_type: dict[str, list[dict]] = {}
    for row in raw:
        by_type.setdefault(row["signal_type"], []).append(row)

    for sig_type, rows in by_type.items():
        zscore_cfg = _ZSCORE_CONFIG.get(sig_type)
        parametric = _SIMPLE_SCORERS.get(sig_type)
        if zscore_cfg is None and parametric is None:
            continue

        # Pre-fetch z-score history once per signal_type
        history: list[float] | None = None
        if zscore_cfg is not None:
            history = await get_signal_history(ticker, sig_type, limit=zscore_cfg.window)

        for row in rows:
            value = float(row["value"])
            if math.isnan(value) or math.isinf(value):
                continue

            score: float | None = None
            method = "parametric_fallback"

            # Try z-score first
            if zscore_cfg is not None and history is not None:
                score = zscore_cfg.score_from_history(history, value)
                if score is not None:
                    method = "zscore"

            # Fallback to parametric
            if score is None and parametric is not None:
                score = parametric(value)

            if score is None:
                continue

            _record_method(sig_type, method)
            result.append(_build(
                sig_type, value, score,
                row.get("source", "unknown"), row["timestamp"],
                "market", ticker, now,
            ))

    return result


def _compute_w_conf(pos: float, neg: float, neu: float) -> float:
    """
    Compute confidence weight from FinBERT class probabilities.

    Paper §Event-Level Weighting:
        w_conf = 1 − (−Σ P_i(k) ln P_i(k)) / ln(3)

    Inverse normalized entropy of the 3-class output.  When one class
    dominates (e.g. 0.92, 0.05, 0.03), w_conf ≈ 0.74.  When probabilities
    are spread evenly (0.33, 0.33, 0.33), w_conf ≈ 0.

    Returns float in [0, 1].  Returns 0.0 if any probability is invalid.
    """
    probs = [pos, neg, neu]
    if any(p < 0 for p in probs):
        return 0.0
    total = sum(probs)
    if total < 1e-9:
        return 0.0

    entropy = 0.0
    for p in probs:
        if p > 0:
            entropy -= p * math.log(p)

    ln3 = math.log(3)
    w_conf = 1.0 - entropy / ln3
    return max(0.0, min(1.0, w_conf))


def score_narrative_signals(
    ticker: str,
    articles: list[dict],
    now: datetime,
) -> list[dict]:
    """
    Convert raw_articles rows (with finbert_score) to scored dicts.

    Sprint A: uses finbert_score (FinBERT S_i = P(pos) - P(neg)) instead of
    provider_sentiment.  Weight formula now includes w_conf from FinBERT class
    probabilities per paper §Event-Level Weighting:
        w_i = w_src · w_rel · w_conf · e^(−λΔt_i)

    Parameters
    ----------
    articles : rows from get_articles_since() — keys: published_at,
               finbert_score, relevance_score, source, finbert_pos,
               finbert_neg, finbert_neu.
    """
    result: list[dict] = []
    for art in articles:
        sentiment = art.get("finbert_score")
        if sentiment is None:
            continue
        sent_f = float(sentiment)
        if math.isnan(sent_f) or math.isinf(sent_f):
            continue
        # Sprint D: explicit None check + 0.6 threshold (paper Stage 2)
        relevance_raw = art.get("relevance_score")
        if relevance_raw is None:
            continue  # Paper Stage 2: exclude articles without explicit relevance
        relevance = float(relevance_raw)
        if relevance < 0.6:
            continue  # Paper Stage 2: direct narrative threshold
        source    = art.get("source", "news")
        published = art["published_at"]

        score  = max(0.0, min(100.0, 50.0 + 50.0 * sent_f))
        half_life = _get_half_life("narrative", source)
        w_time = _time_weight(published, now, half_life)

        # w_conf from FinBERT class probabilities (paper §Event-Level Weighting)
        pos = art.get("finbert_pos")
        neg = art.get("finbert_neg")
        neu = art.get("finbert_neu")
        if pos is not None and neg is not None and neu is not None:
            w_conf = _compute_w_conf(float(pos), float(neg), float(neu))
        else:
            w_conf = 1.0  # Fallback: treat as fully confident if no probs available

        # Paper formula: w_i = w_src · w_rel · w_conf · e^(−λΔt_i)
        weight = _source_weight(source) * w_time * relevance * w_conf

        result.append({
            "signal_type": "finbert_sentiment",
            "value":       float(sentiment),
            "score":       round(score, 2),
            "weight":      round(max(weight, 1e-6), 6),
            "source":      source,
            "layer":       "narrative",
            "ticker":      ticker,
        })
    return result


async def score_influencer_signals(
    ticker: str,
    raw: list[dict],
    now: datetime,
    current_price: float | None = None,
) -> list[dict]:
    """
    Convert raw influencer signal rows to scored dicts.

    Uses z-score normalization for insider_net_shares and analyst_buy_pct
    when sufficient history is available; falls back to parametric otherwise.
    analyst_target_price always uses parametric scoring (Decision 5).

    Parameters
    ----------
    current_price : Latest close price, used to score analyst_target_price.
                    If None, analyst_target_price signals are skipped.
    """
    from scripts.db.queries.raw_signals import get_signal_history

    result: list[dict] = []

    by_type: dict[str, list[dict]] = {}
    for row in raw:
        by_type.setdefault(row["signal_type"], []).append(row)

    for sig_type, rows in by_type.items():
        # analyst_eps_estimate_mean is the raw stored signal; the scored
        # signal is the derived earnings_estimate_revision (handled below).
        if sig_type == "analyst_eps_estimate_mean":
            continue

        zscore_cfg = _ZSCORE_CONFIG.get(sig_type)
        parametric = _SIMPLE_SCORERS.get(sig_type)
        is_target = (sig_type == "analyst_target_price")

        if zscore_cfg is None and parametric is None and not is_target:
            continue

        # Pre-fetch z-score history once per signal_type
        history: list[float] | None = None
        if zscore_cfg is not None:
            history = await get_signal_history(ticker, sig_type, limit=zscore_cfg.window)

        # Sprint P3.2 option (c): analyst_target_price stores RAW target; the
        # quantity actually z-scored is the per-row upside vs the *current* close.
        # Transform once per signal_type.
        upside_history: list[float] | None = None
        if is_target and history is not None and current_price is not None and current_price > 0:
            upside_history = [(h - current_price) / current_price for h in history]

        for row in rows:
            value = float(row["value"])
            if math.isnan(value) or math.isinf(value):
                continue
            source = row.get("source", "unknown")
            ts = row["timestamp"]

            score: float | None = None
            method = "parametric_fallback"

            if is_target:
                # Z-score upside if we have enough history and a current_price.
                if (zscore_cfg is not None and upside_history is not None
                        and current_price is not None and current_price > 0):
                    upside_now = (value - current_price) / current_price
                    score = zscore_cfg.score_from_history(upside_history, upside_now)
                    if score is not None:
                        method = "zscore"
                # Parametric fallback (cold-start path).
                if score is None:
                    score = _score_analyst_target(value, current_price)
                if score is None:
                    continue
            else:
                # Try z-score first
                if zscore_cfg is not None and history is not None:
                    score = zscore_cfg.score_from_history(history, value)
                    if score is not None:
                        method = "zscore"

                # Fallback to parametric
                if score is None and parametric is not None:
                    score = parametric(value)

            if score is None:
                continue

            _record_method(sig_type, method)
            result.append(_build(sig_type, value, score, source, ts, "influencer", ticker, now))

    # ------------------------------------------------------------------
    # Sprint P3.3 — derived earnings_estimate_revision from raw EPS history
    # ------------------------------------------------------------------
    eps_rows = by_type.get("analyst_eps_estimate_mean")
    if eps_rows:
        latest = max(eps_rows, key=lambda r: r["timestamp"])
        latest_val = float(latest["value"])
        if not (math.isnan(latest_val) or math.isinf(latest_val)):
            eps_history = await get_signal_history(
                ticker, "analyst_eps_estimate_mean", limit=120,
            )
            # Need ≥2 obs in history for a period-over-period delta.
            if len(eps_history) >= 2:
                prior = eps_history[-2]
                if prior != 0:
                    delta = (latest_val - prior) / abs(prior)
                    # Build delta history from consecutive pairs, excluding the
                    # final pair (which equals delta_now and would bias the z-score).
                    delta_history: list[float] = []
                    for i in range(1, len(eps_history) - 1):
                        p, c = eps_history[i - 1], eps_history[i]
                        if p != 0:
                            delta_history.append((c - p) / abs(p))

                    delta_cfg = _ZSCORE_CONFIG.get("earnings_estimate_revision")
                    score: float | None = None
                    method = "parametric_fallback"
                    if delta_cfg is not None and delta_history:
                        score = delta_cfg.score_from_history(delta_history, delta)
                        if score is not None:
                            method = "zscore"
                    if score is None:
                        score = _score_earnings_revision_delta(delta)
                    if score is not None:
                        _record_method("earnings_estimate_revision", method)
                        result.append(_build(
                            "earnings_estimate_revision", delta, score,
                            latest.get("source", "unknown"), latest["timestamp"],
                            "influencer", ticker, now,
                        ))

    return result


#: Macro signals stored as global rows under '_MACRO_'. Their z-score
#: history lookup uses '_MACRO_' regardless of which ticker is being
#: scored. Updated for Sprint P4.3 (FRED Treasury + yield-curve signals).
_MACRO_GLOBAL_TYPES: frozenset[str] = frozenset({
    "vix",
    "treasury_yield_10y",
    "treasury_yield_2y",
    "ted_spread",
})

#: All signal types that the macro scoring path recognises. Anything else
#: in the raw row list is silently ignored.
_MACRO_RECOGNISED_TYPES: frozenset[str] = _MACRO_GLOBAL_TYPES | frozenset({
    "sector_etf_return_20d",
})


async def score_macro_signals(
    ticker: str,
    sector: str | None,
    raw: list[dict],
    now: datetime,
) -> list[dict]:
    """
    Convert raw macro signal rows to scored dicts.

    Sprint P4.2: per-ticker macro. VIX and the FRED signals (Sprint P4.3:
    treasury_yield_10y / treasury_yield_2y / ted_spread) remain shared
    global signals — history under `_MACRO_`. The sector ETF return is
    z-scored against the relevant ETF's own history, looked up via the
    ticker's GICS sector. If the ticker has `sector IS NULL` the ETF
    component is silently dropped; the macro shrinkage stopgap (P4.2
    Decision 7 Option C, denominator=2) prevents the resulting score
    from being aggressively pulled toward 50.

    Parameters
    ----------
    ticker : str — the ticker whose macro sub-index we are computing
    sector : str | None — its GICS sector (e.g. "Information Technology")
    raw    : rows from get_signals_since() across '_MACRO_' and the ETF
             corresponding to the ticker's sector
    now    : scoring tick wall-clock time
    """
    from scripts.db.queries.raw_signals import get_signal_history
    from pipeline.sources.macro import SECTOR_ETFS

    result: list[dict] = []

    by_type: dict[str, list[dict]] = {}
    for row in raw:
        sig_type = row["signal_type"]
        if sig_type not in _MACRO_RECOGNISED_TYPES:
            continue
        by_type.setdefault(sig_type, []).append(row)

    for sig_type, rows in by_type.items():
        zscore_cfg = _ZSCORE_CONFIG.get(sig_type)
        parametric = _SIMPLE_SCORERS.get(sig_type)
        if zscore_cfg is None and parametric is None:
            continue

        # History lookup ticker depends on the signal type:
        # • Global macro types (VIX + FRED)  → stored under '_MACRO_'
        # • sector_etf_return_20d            → stored under the ETF symbol;
        #                                      resolve from the ticker's GICS sector
        history: list[float] | None = None
        history_ticker: str | None = None
        if zscore_cfg is not None:
            if sig_type in _MACRO_GLOBAL_TYPES:
                history_ticker = "_MACRO_"
            elif sig_type == "sector_etf_return_20d" and sector is not None:
                history_ticker = SECTOR_ETFS.get(sector)
            if history_ticker is not None:
                history = await get_signal_history(
                    history_ticker, sig_type, limit=zscore_cfg.window,
                )

        for row in rows:
            value = float(row["value"])
            if math.isnan(value) or math.isinf(value):
                continue

            score: float | None = None
            method = "parametric_fallback"

            if zscore_cfg is not None and history is not None:
                score = zscore_cfg.score_from_history(history, value)
                if score is not None:
                    method = "zscore"

            if score is None and parametric is not None:
                score = parametric(value)

            if score is None:
                continue

            _record_method(sig_type, method)
            result.append(_build(
                sig_type, value, score,
                row.get("source", "alpha_vantage"), row["timestamp"],
                "macro", ticker, now,
            ))

    return result


