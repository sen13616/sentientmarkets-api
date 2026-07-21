"""
pipeline/orchestrator.py  — Layer 02

Coordinates the full scoring pipeline for a single ticker.

Architecture (Sprint 3: global scoring tick)
---------------------------------------------
Ingestion jobs (market, narrative, influencer, macro) are data-only: they
fetch signals from external APIs and write to the database.  Scoring is
performed by a dedicated ``scoring_tick_job`` that runs every 30 minutes
and calls ``_score_and_write()`` for all tickers, recomputing all four
layers from current DB state.

Pipeline flow (per ticker, per scoring tick)
---------------------------------------------
    1. Read signals from DB and apply feature engineering  (Layer 07)
    2. Compute per-layer sub-indices  (Layer 08 — subindices)
    3. Compute composite score and divergence  (Layer 08 — composite + divergence)
    4. Compute confidence score  (Layer 09)
    5. Generate explanation  (Layer 10)
    6. Persist results to Redis and PostgreSQL  (System C)

Fallback propagation (per spec §Layer 02)
------------------------------------------
    no fresh DB data       → use last known state from Redis if within staleness window
    Redis also stale       → mark layer missing, redistribute composite weights,
                             apply confidence penalty

score_ticker() is provided for tests and one-off manual runs.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import NamedTuple

import httpx

from scripts.db.queries.raw_articles import get_articles_since
from scripts.db.queries.raw_signals import get_latest_close, get_signals_since
from pipeline.confidence.scorer import compute_confidence
from pipeline.confidence.staleness import (
    _market_stale,
    filter_stale_signals,
    market_lookback_since,
    stale_sources,
)
from pipeline.explanation.templates import generate_explanation
from pipeline.features.normalize import (
    score_influencer_signals,
    score_macro_signals,
    score_market_signals,
    score_narrative_signals,
)
from pipeline.persistence.pg_writer import persist_scored_state
from pipeline.persistence.redis_writer import read_scored_state, write_scored_state
from pipeline.features import positioning, surprise
from pipeline.scoring.composite import compute_composite, compute_exo_composite
from pipeline.scoring.ema import compute_ema
from pipeline.scoring.divergence import compute_divergence
from pipeline.scoring.drivers import extract_drivers
from pipeline.scoring.subindices import (
    SubIndexResult,
    compute_macro_sub_index,
    compute_market_sub_index,
    compute_sub_index,
)
from pipeline.sources.influencer import fetch_influencer_signals
from pipeline.sources.macro import fetch_macro_signals
from pipeline.sources.market import fetch_market_signals
from pipeline.sources.narrative import fetch_narrative_signals

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MACRO_TICKER = "_MACRO_"
_SECTOR_ETFS  = [
    "XLB", "XLC", "XLE", "XLF", "XLI",
    "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY",
]

# Maximum age of the last-scored timestamp before falling back to Redis cache.
# These are STALENESS thresholds — they determine when a layer's sub-index is
# considered too stale to trust and Redis cached state is used instead.
_LAYER_LOOKBACK: dict[str, timedelta] = {
    "market":     timedelta(minutes=90),
    "narrative":  timedelta(hours=6),
    "influencer": timedelta(days=3),
    "macro":      timedelta(hours=72),
}

# How far back to look in the DB when fetching raw data for scoring.
# Narrative articles are fetched from the last 3 days by the fetcher, so the
# scoring window must match — using the 6-hour staleness threshold here would
# miss articles published more than 6 hours ago.
_NARRATIVE_SCORE_LOOKBACK = timedelta(days=3)

# Insider rows are dated by transaction date, ingested over a 30-day window,
# carry a 7-day half-life and a 30-day staleness threshold. Fetching only the
# 3-day staleness window here would silently exclude every insider signal
# older than 3 days, leaving the influencer sub-index analyst-only. Match the
# insider ingest/staleness window instead.
_INFLUENCER_SCORE_LOOKBACK = timedelta(days=30)

_MARKET_SIGNAL_TYPES = [
    "rsi_14", "return_1d", "return_5d", "return_20d",
    "volume_ratio",
    "order_flow_imbalance", "buy_pressure", "sell_pressure",
    "bid_ask_spread_bps",
    "short_volume_otc", "short_volume_total_otc", "short_volume_ratio_otc",
]
_MARKET_SIGNAL_TYPES_LEGACY = [
    # Old signal types that may still exist in the DB from prior pipeline runs.
    # Included so the scoring layer can pick them up if present.
    "ohlcv_close", "ohlcv_open", "ohlcv_high", "ohlcv_low", "ohlcv_volume",
    # Removed Stage 1 — put_call_ratio, short_interest_ratio, implied_volatility
    # excluded from paper's implemented methodology. See docs/SIGNAL_CATALOG.md.
]
_INFLUENCER_SIGNAL_TYPES = [
    "insider_net_shares", "analyst_buy_pct", "analyst_target_price",
    "analyst_eps_estimate_mean",
]
_MACRO_SIGNAL_TYPES = [
    "vix",
    "sector_etf_return_20d",
    # Sprint P4.3 — FRED Treasury / yield-curve signals (stored under _MACRO_)
    "treasury_yield_10y",
    "treasury_yield_2y",
    "ted_spread",
]
#: Subset of macro signal types stored under `_MACRO_` (i.e. NOT per-ticker).
#: Used by `_score_macro` to issue a single batched fetch for all global rows.
_MACRO_GLOBAL_SIGNAL_TYPES = [
    "vix",
    "treasury_yield_10y",
    "treasury_yield_2y",
    "ted_spread",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ensure_tz(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_ts(val: str | datetime | None) -> datetime | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return _ensure_tz(val)
    try:
        return _ensure_tz(datetime.fromisoformat(str(val)))
    except ValueError:
        return None


def _latest_ts(rows: list[dict], key: str = "timestamp") -> datetime | None:
    ts_list = [_ensure_tz(r[key]) for r in rows if r.get(key)]
    return max(ts_list) if ts_list else None


def _fallback_subindex(
    layer: str,
    last_state: dict | None,
    now: datetime,
) -> tuple[SubIndexResult | None, datetime | None]:
    """
    Attempt to recover a sub-index from the last known Redis state.

    Returns (SubIndexResult, as_of) if the cached value is still within
    the layer's lookback window, otherwise (None, None).
    """
    if not last_state:
        return None, None

    freshness = last_state.get("freshness") or {}
    as_of     = _parse_ts(freshness.get(f"{layer}_as_of"))

    if as_of is None:
        return None, None

    # Market layer: use market-hours-aware staleness (an EOD score stays
    # fresh through the overnight period and entire weekend).
    if layer == "market":
        if _market_stale(as_of, now):
            return None, None
    elif (now - as_of) > _LAYER_LOOKBACK[layer]:
        return None, None

    entry = (last_state.get("sub_indices") or {}).get(layer)
    if entry is None:
        return None, None

    value     = entry.get("value") if isinstance(entry, dict) else float(entry)
    n_signals = entry.get("n_signals", 1) if isinstance(entry, dict) else 1
    sources   = entry.get("sources", [])  if isinstance(entry, dict) else []

    if value is None:
        return None, None

    _log.debug(f"[fallback] using cached {layer} sub-index (as_of={as_of.isoformat()})")
    return SubIndexResult(float(value), n_signals, sources), as_of


# ---------------------------------------------------------------------------
# Per-layer scoring  (read DB → normalize → sub-index → fallback if empty)
# ---------------------------------------------------------------------------

async def _score_market(
    ticker: str,
    now: datetime,
    last_state: dict | None,
) -> tuple[SubIndexResult | None, list[dict], datetime | None]:
    since = market_lookback_since(now)
    # NOTE: If an ingestion job is currently mid-write, this may read data from
    # the prior ingestion cycle for some tickers.  This is correct behavior —
    # the next scoring tick will pick up any in-flight data.
    raw   = await get_signals_since(ticker, since, _MARKET_SIGNAL_TYPES + _MARKET_SIGNAL_TYPES_LEGACY)
    raw   = filter_stale_signals(raw, now)
    if raw:
        # Sprint 4: score_market_signals is now async (z-score requires DB reads).
        # Short volume z-score is handled inline — no separate call needed.
        sigs  = await score_market_signals(ticker, raw, now)

        si    = compute_market_sub_index(sigs)
        as_of = _latest_ts(raw)
        if si is not None:
            return si, sigs, as_of

    si, as_of = _fallback_subindex("market", last_state, now)
    return si, [], as_of


async def _score_narrative(
    ticker: str,
    now: datetime,
    last_state: dict | None,
) -> tuple[SubIndexResult | None, list[dict], datetime | None]:
    since    = now - _NARRATIVE_SCORE_LOOKBACK  # articles published in last 3 days
    articles = await get_articles_since(ticker, since)
    if articles:
        sigs  = score_narrative_signals(ticker, articles, now)
        si    = compute_sub_index(sigs)
        as_of = _latest_ts(articles, key="published_at")
        if si is not None:
            return si, sigs, as_of

    si, as_of = _fallback_subindex("narrative", last_state, now)
    return si, [], as_of


async def _score_influencer(
    ticker: str,
    now: datetime,
    last_state: dict | None,
    current_price: float | None,
) -> tuple[SubIndexResult | None, list[dict], datetime | None, datetime | None, datetime | None]:
    """Returns (subindex, sigs, influencer_as_of, analyst_as_of, insider_as_of)."""
    since = now - _INFLUENCER_SCORE_LOOKBACK
    raw   = await get_signals_since(ticker, since, _INFLUENCER_SIGNAL_TYPES)
    if raw:
        sigs  = await score_influencer_signals(ticker, raw, now, current_price)
        si    = compute_sub_index(sigs)
        as_of = _latest_ts(raw)

        # Separate timestamps for analyst vs insider staleness checks
        insider_rows = [r for r in raw if r["signal_type"] == "insider_net_shares"]
        analyst_rows = [r for r in raw if r["signal_type"] in ("analyst_buy_pct", "analyst_target_price")]
        insider_as_of = _latest_ts(insider_rows)
        analyst_as_of = _latest_ts(analyst_rows)

        if si is not None:
            return si, sigs, as_of, analyst_as_of, insider_as_of

    si, as_of = _fallback_subindex("influencer", last_state, now)
    # Recover individual as_of from last_state freshness if available
    freshness    = (last_state or {}).get("freshness") or {}
    analyst_as_of = _parse_ts(freshness.get("influencer_as_of"))
    insider_as_of = analyst_as_of  # Phase 1 approximation
    return si, [], as_of, analyst_as_of, insider_as_of


async def _score_macro(
    ticker: str,
    sector: str | None,
    now: datetime,
    last_state: dict | None,
) -> tuple[SubIndexResult | None, list[dict], datetime | None]:
    """
    Per-ticker macro scoring.

    • VIX + 3 FRED signals (P4.3) are shared global signals under `_MACRO_`.
    • Sector ETF return is routed per-ticker via the ticker's GICS sector (P4.2).
    • Aggregation uses the dedicated `compute_macro_sub_index` with the paper's
      per-signal-type weight table and NO volume shrinkage (P4.4, M9 + M10).
      The P4.2-era `compute_sub_index(..., shrinkage_denominator=2)` stopgap
      has been retired — `compute_macro_sub_index` supersedes it.
    """
    from pipeline.sources.macro import SECTOR_ETFS

    since = now - _LAYER_LOOKBACK["macro"]

    # Global macro signals — single batched query under _MACRO_.
    # Sprint P4.3 expanded this from VIX-only to VIX + 3 FRED Treasury signals.
    global_rows = await get_signals_since(_MACRO_TICKER, since, _MACRO_GLOBAL_SIGNAL_TYPES)

    # Sector ETF — per-ticker, resolved through the ticker's GICS sector
    etf_rows: list[dict] = []
    if sector is not None:
        etf = SECTOR_ETFS.get(sector)
        if etf is not None:
            etf_rows = await get_signals_since(etf, since, ["sector_etf_return_20d"])
        else:
            _log.warning(
                "_score_macro(%s): unknown GICS sector %r — skipping ETF component",
                ticker, sector,
            )
    else:
        _log.debug(
            "_score_macro(%s): sector is NULL — skipping ETF component",
            ticker,
        )

    all_raw = global_rows + etf_rows
    if all_raw:
        sigs  = await score_macro_signals(ticker, sector, all_raw, now)
        si    = compute_macro_sub_index(sigs)
        as_of = _latest_ts(all_raw)
        if si is not None:
            return si, sigs, as_of

    si, as_of = _fallback_subindex("macro", last_state, now)
    return si, [], as_of


# ---------------------------------------------------------------------------
# Core compute + persist  (no external API calls)
# ---------------------------------------------------------------------------

class ScoreResult(NamedTuple):
    """Per-ticker outcome of one scoring pass, consumed by _score_all."""
    n_populated: int            # non-null sub-indices (0-4)
    smoothed_score: float       # EMA-smoothed composite written to state
    score_change_1d: float | None       # vs 24-48h-old baseline; None if no baseline
    score_change_1d_pct: float | None   # same basis, as % of the baseline
    raw_score: float | None = None      # divergence-capped raw composite (cross-sectional stats)
    exo_score: float | None = None      # exogenous sentiment-only composite (no market layer)


async def _score_and_write(
    ticker: str,
    sector: str | None = None,
    baseline_score: float | None = None,
) -> ScoreResult:
    """
    Read all layers from DB, compute the full scored state, and persist it.

    Called by the global scoring tick job after ingestion jobs have written
    fresh data to the database.  All four layers are always recomputed from
    current DB state — no carry-forward.

    Parameters
    ----------
    ticker : str
        The ticker symbol to score.
    sector : str | None
        The ticker's GICS sector (Sprint P4.2). Passed in pre-resolved so
        the per-ticker macro routing in ``_score_macro`` doesn't issue a
        DB query per ticker per tick. The scoring tick job preloads the
        full ticker→sector map once via ``get_ticker_sector_map()`` and
        threads it through here. ``None`` is valid and produces a
        VIX-only macro sub-index for that ticker.
    baseline_score : float | None
        Smoothed composite from this ticker's most recent tick aged 24–48h
        (preloaded once per tick via ``get_baseline_scores()``). Drives
        ``score_change_1d``. ``None`` (new ticker, or a data gap such as
        the 2026-06-23→07-03 outage) makes the change fields null — the
        change is never computed across a gap.

    Returns
    -------
    ScoreResult — (n_populated, smoothed_score, score_change_1d).
    """
    now        = datetime.now(timezone.utc)
    ticker     = ticker.upper()
    last_state = await read_scored_state(ticker)

    # Current close price (for analyst_target_price normalization)
    current_price: float | None = await get_latest_close(ticker)

    # ── Layer 07+08: feature engineering + sub-indices ────────────────────────
    # All four layers are freshly recomputed from DB state every scoring tick.
    # If a layer has no fresh data, _fallback_subindex() recovers the cached
    # value from Redis within the staleness window.

    (market_si, market_sigs, market_as_of) = await _score_market(ticker, now, last_state)
    (narrative_si, narrative_sigs, narrative_as_of) = await _score_narrative(ticker, now, last_state)
    (influencer_si, influencer_sigs, influencer_as_of, analyst_as_of, insider_as_of) = await _score_influencer(ticker, now, last_state, current_price)
    (macro_si, macro_sigs, macro_as_of) = await _score_macro(ticker, sector, now, last_state)

    sub_indices: dict = {
        "market":     market_si,
        "narrative":  narrative_si,
        "influencer": influencer_si,
        "macro":      macro_si,
    }

    # ── Composite + divergence ─────────────────────────────────────────────────
    composite_result           = compute_composite(sub_indices)
    present_values             = {k: v.value for k, v in sub_indices.items() if v is not None}
    div_result, effective_score = compute_divergence(present_values, composite_result.score)

    # Exogenous sentiment-only composite (nowcasting plan, Phase 3): raw only —
    # no EMA, no divergence cap (divergence is a market-vs-composite construct).
    exo_result = compute_exo_composite(sub_indices)
    score_exo: float | None = round(exo_result.score, 2) if exo_result is not None else None

    # ── EMA smoothing (Sprint 5a, G-C3) ────────────────────────────────────────
    prev_smoothed: float | None = None
    prev_obs_count: int = 0
    dt_hours: float = 0.0

    if last_state:
        prev_smoothed  = last_state.get("composite_score_smoothed")
        # Fall back to composite_score if composite_score_smoothed is absent
        # (pre-Sprint-5a states in Redis have no smoothed key).
        if prev_smoothed is None:
            prev_smoothed = last_state.get("composite_score")
        prev_obs_count = int(last_state.get("ema_obs_count") or 0)
        prev_ts        = _parse_ts(last_state.get("timestamp"))
        if prev_ts is not None:
            dt_hours = max(0.0, (now - prev_ts).total_seconds() / 3600.0)

    smoothed_score = compute_ema(effective_score, prev_smoothed, dt_hours)
    ema_obs_count  = prev_obs_count + 1

    # ── 1-day change vs the 24–48h-old baseline (null across gaps) ────────────
    score_change_1d: float | None = None
    score_change_1d_pct: float | None = None
    if baseline_score is not None:
        score_change_1d = round(smoothed_score - baseline_score, 2)
        if baseline_score != 0:
            score_change_1d_pct = round(
                100.0 * (smoothed_score - baseline_score) / baseline_score, 2
            )

    # ── Staleness and confidence ───────────────────────────────────────────────
    as_of_map = {
        "market":  market_as_of,
        "news":    narrative_as_of,
        "analyst": analyst_as_of,
        "insider": insider_as_of,
        "macro":   macro_as_of,
    }
    stale_list  = stale_sources(as_of_map, now=now)

    all_sigs    = market_sigs + narrative_sigs + influencer_sigs + macro_sigs
    n_signals   = sum(1 for s in all_sigs if (s.get("weight") or 0) > 0)

    conf_result = compute_confidence(
        missing_layers  = composite_result.missing_layers,
        stale_sources   = stale_list,
        n_signals       = n_signals,
        divergence_flag = div_result.flag,
    )

    # ── Drivers + explanation ──────────────────────────────────────────────────
    drivers     = extract_drivers(all_sigs)
    explanation = generate_explanation(drivers)

    # ── Narrative surprise (Phase 5b — flag-off by default, research-only) ────
    # Accumulates in Redis/DB for the eval gate; never enters the composite or
    # any served score. Flag off ⇒ no query, no state key.
    narrative_surprise: float | None = None
    if surprise.enabled():
        try:
            narrative_surprise = await surprise.compute_narrative_surprise(ticker, now)
        except Exception as exc:
            _log.debug("[%s] narrative surprise failed: %s", ticker, exc)

    # ── Positioning research features (Track B3 — flag-off, research-only) ────
    # Same contract as narrative surprise: accumulates in the research_features
    # JSONB for the eval harness; never enters the composite or any served
    # score. Flag off ⇒ no query, no state key.
    research_features: dict = {}
    if positioning.enabled():
        try:
            research_features = await positioning.compute_positioning_features(ticker, now)
        except Exception as exc:
            _log.debug("[%s] positioning features failed: %s", ticker, exc)

    # ── Assemble state ─────────────────────────────────────────────────────────
    # In Redis, composite_score stores the SMOOTHED value (served as API score).
    # composite_score_raw stores the raw value (served as API score_raw, pro only).
    # composite_score_smoothed mirrors composite_score for the PG writer, which
    # writes it to the DB column of the same name (the DB composite_score column
    # retains the raw value — see pg_writer.py).
    state: dict = {
        "ticker":                    ticker,
        "timestamp":                 now,
        "composite_score":           round(smoothed_score, 2),
        "composite_score_raw":       round(effective_score, 2),
        "composite_score_smoothed":  round(smoothed_score, 2),
        "score_exo":                 score_exo,
        "score_change_1d":           score_change_1d,
        "score_change_1d_pct":       score_change_1d_pct,
        "ema_obs_count":             ema_obs_count,
        "sub_indices": {
            k: {
                "value":     round(v.value, 2),
                "n_signals": v.n_signals,
                "sources":   v.sources,
            } if v is not None else None
            for k, v in sub_indices.items()
        },
        "confidence": {
            "score": conf_result.score,
            "flags": conf_result.flags,
        },
        "top_drivers": [d.to_dict() for d in drivers],
        "explanation": explanation,
        "freshness": {
            "market_as_of":     market_as_of,
            "narrative_as_of":  narrative_as_of,
            "influencer_as_of": influencer_as_of,
            "macro_as_of":      macro_as_of,
        },
        "divergence": div_result.flag,
        "price": {
            "close":  current_price,
            "volume": None,  # populated by market fetcher in raw_signals; snapshot uses close only
        },
    }
    if surprise.enabled():
        state["narrative_surprise"] = narrative_surprise
    if research_features:
        state["research_features"] = research_features

    # ── Persist ────────────────────────────────────────────────────────────────
    await write_scored_state(ticker, state)
    await persist_scored_state(state)

    n_populated = sum(1 for v in sub_indices.values() if v is not None)

    _log.debug(
        "[%s] scored  raw=%.1f  smoothed=%.1f  ema_n=%d  confidence=%d  divergence=%s  missing=%s  layers=%d/4",
        ticker,
        effective_score,
        smoothed_score,
        ema_obs_count,
        conf_result.score,
        div_result.flag,
        composite_result.missing_layers or "none",
        n_populated,
    )
    return ScoreResult(
        n_populated=n_populated,
        smoothed_score=round(smoothed_score, 2),
        score_change_1d=score_change_1d,
        score_change_1d_pct=score_change_1d_pct,
        raw_score=round(effective_score, 2),
        exo_score=score_exo,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def score_ticker(
    ticker: str,
    layer_scope: str | None = None,
) -> None:
    """
    Fetch fresh data for `layer_scope` then compute and persist the scored state.

    Intended for one-off runs and tests.  Scheduler jobs use the lower-level
    helpers (_score_and_write, fetch_*) for batching efficiency.

    Parameters
    ----------
    ticker      : Ticker symbol (case-insensitive).
    layer_scope : Which external APIs to fetch from before scoring.
                  None = fetch all four layers.
                  "market" | "narrative" | "influencer" | "macro"
                  Note: scoring always recomputes all four layers from DB
                  state regardless of which APIs were fetched.
    """
    ticker = ticker.upper()

    async with httpx.AsyncClient(timeout=30) as client:
        coros = []
        if layer_scope in (None, "market"):
            coros.append(fetch_market_signals(ticker, client))
        if layer_scope in (None, "narrative"):
            coros.append(fetch_narrative_signals(ticker, client))
        if layer_scope in (None, "influencer"):
            coros.append(fetch_influencer_signals(ticker, client))
        if layer_scope in (None, "macro"):
            coros.append(fetch_macro_signals(client))

        if coros:
            fetch_results = await asyncio.gather(*coros, return_exceptions=True)
            for result in fetch_results:
                if isinstance(result, BaseException):
                    _log.warning("[%s] source fetch error: %s", ticker, result)

    await _score_and_write(ticker)
