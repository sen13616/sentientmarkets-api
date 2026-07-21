"""
api/response/assembler.py  — Layer 11

Reads the pre-computed scored state and assembles the JSON response.

Resolution order
----------------
1. Redis key  sentiment:{ticker}          — always-serve-last-known-state
2. sentiment_history table (latest row)   — fallback when Redis is cold
3. NoDataResponse                         — when no data exists at all

Tier filtering
--------------
Free tier  : score, label, confidence, timestamp, cache_age_seconds only.
Pro tier   : all fields including sub_indices, drivers, freshness, explanation.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

from scripts.db.queries.sentiment_history import get_latest
from scripts.db.redis import get_redis
from pipeline.confidence.staleness import is_market_hours
from pipeline.scoring.market_overview import PERCENTILE_KEY, XS_KEY

from .labels import score_to_label
from .schemas import (
    Driver,
    FreeTierResponse,
    Freshness,
    MarketHours,
    NoDataResponse,
    ProTierResponse,
    SubIndices,
)

_log = logging.getLogger(__name__)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _cache_age(timestamp: datetime | str | None) -> int:
    """Seconds since the state was last scored. Returns 0 on parse failure."""
    if timestamp is None:
        return 0
    try:
        if isinstance(timestamp, str):
            ts = datetime.fromisoformat(timestamp)
        else:
            ts = timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return max(0, int((_now_utc() - ts).total_seconds()))
    except Exception as exc:
        _log.debug("cache_age: timestamp parse failed, returning 0: %s", exc)
        return 0


def _parse_dt(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        dt = datetime.fromisoformat(str(value))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


async def _load_from_redis(ticker: str) -> dict | None:
    """Return the parsed state dict from Redis, or None."""
    try:
        client = get_redis()
        raw = await client.get(f"sentiment:{ticker.upper()}")
        if raw is None:
            return None
        return json.loads(raw)
    except Exception as exc:
        _log.warning("assembler: Redis read failed for %s: %s", ticker, exc)
        return None


async def _load_percentile(ticker: str) -> float | None:
    """Universe percentile from the latest scoring tick; None if the ticker
    was not in that tick (or Redis is unavailable)."""
    try:
        client = get_redis()
        raw = await client.hget(PERCENTILE_KEY, ticker.upper())
        return float(raw) if raw is not None else None
    except Exception as exc:
        _log.warning("assembler: percentile read failed for %s: %s", ticker, exc)
        return None


async def _load_xs(ticker: str) -> dict | None:
    """Cross-sectional raw-score stats ({raw_z, raw_pctl, sector_pctl}) from
    the latest scoring tick; None if the ticker was not in that tick (or
    Redis is unavailable)."""
    try:
        client = get_redis()
        raw = await client.hget(XS_KEY, ticker.upper())
        return json.loads(raw) if raw is not None else None
    except Exception as exc:
        _log.warning("assembler: xs read failed for %s: %s", ticker, exc)
        return None


async def _load_from_db(ticker: str) -> dict | None:
    """Return the latest sentiment_history row as a dict, or None."""
    try:
        row = await get_latest(ticker)
        if row is None:
            return None
        # Normalise to match Redis state layout expected by _build_*
        flags = row.get("confidence_flags")
        if isinstance(flags, str):
            flags = json.loads(flags)
        drivers = row.get("top_drivers")
        if isinstance(drivers, str):
            drivers = json.loads(drivers)
        # Map DB columns to Redis state shape.
        # DB composite_score = raw; DB composite_score_smoothed = smoothed.
        # Redis composite_score = smoothed (with raw fallback for pre-EMA rows).
        smoothed = row.get("composite_score_smoothed")
        raw_score = row["composite_score"]
        return {
            "ticker":                    ticker.upper(),
            "composite_score":           smoothed if smoothed is not None else raw_score,
            "composite_score_raw":       raw_score,
            "composite_score_smoothed":  smoothed,
            "score_exo":                 row.get("composite_score_exo"),
            "ema_obs_count":             row.get("ema_obs_count") or 0,
            "confidence":                {"score": row["confidence_score"], "flags": flags or []},
            "sub_indices": {
                "market":     {"value": row.get("market_index")},
                "narrative":  {"value": row.get("narrative_index")},
                "influencer": {"value": row.get("influencer_index")},
                "macro":      {"value": row.get("macro_index")},
            },
            "divergence":  row.get("divergence"),
            "top_drivers": drivers or [],
            "explanation": "",
            "freshness": {
                "market_as_of":     row.get("market_as_of"),
                "narrative_as_of":  row.get("narrative_as_of"),
                "influencer_as_of": row.get("influencer_as_of"),
                "macro_as_of":      row.get("macro_as_of"),
            },
            "timestamp": row["timestamp"],
        }
    except Exception as exc:
        _log.warning("assembler: DB read failed for %s: %s", ticker, exc)
        return None


def _market_hours_info(now: datetime) -> MarketHours:
    """
    Compute market open/close context from the current UTC time.

    next_open  — next weekday at 14:30 UTC.
    last_close — most recent weekday at 21:00 UTC.
    """
    _OPEN_H, _OPEN_M   = 14, 30
    _CLOSE_H, _CLOSE_M = 21,  0

    # ---- last_close ----
    close_today = now.replace(hour=_CLOSE_H, minute=_CLOSE_M, second=0, microsecond=0)
    lc_candidate = now if now >= close_today else now - timedelta(days=1)
    while lc_candidate.isoweekday() > 5:
        lc_candidate -= timedelta(days=1)
    last_close = lc_candidate.replace(hour=_CLOSE_H, minute=_CLOSE_M, second=0, microsecond=0)

    # ---- next_open ----
    open_today = now.replace(hour=_OPEN_H, minute=_OPEN_M, second=0, microsecond=0)
    no_candidate = now if now < open_today else now + timedelta(days=1)
    while no_candidate.isoweekday() > 5:
        no_candidate += timedelta(days=1)
    next_open = no_candidate.replace(hour=_OPEN_H, minute=_OPEN_M, second=0, microsecond=0)

    return MarketHours(
        is_open    = is_market_hours(now),
        next_open  = next_open,
        last_close = last_close,
    )


def _sub_val(state: dict, layer: str) -> float | None:
    si = (state.get("sub_indices") or {}).get(layer)
    if si is None:
        return None
    if isinstance(si, dict):
        return si.get("value")
    return float(si)


def _change_fields(state: dict) -> tuple[float | None, float | None]:
    """1-day change pair from state; None for pre-feature states and gaps."""
    change = state.get("score_change_1d")
    pct = state.get("score_change_1d_pct")
    return (
        float(change) if change is not None else None,
        float(pct) if pct is not None else None,
    )


def _composite_score(state: dict) -> int:
    """Rounded composite; explicit None-check so a legitimate 0.0 isn't skipped."""
    val = state.get("composite_score")
    return int(round(val if val is not None else 0))


def _confidence_score(conf) -> int:
    raw = conf.get("score") if isinstance(conf, dict) else conf
    return int(raw or 0)


def _build_free(state: dict) -> FreeTierResponse:
    score = _composite_score(state)
    conf  = state.get("confidence") or {}
    confidence = _confidence_score(conf)
    ts    = _parse_dt(state.get("timestamp"))
    now   = _now_utc()
    change, change_pct = _change_fields(state)
    return FreeTierResponse(
        ticker              = state["ticker"].upper(),
        score               = score,
        score_change_1d     = change,
        score_change_1d_pct = change_pct,
        label               = score_to_label(score),
        confidence          = confidence,
        timestamp           = ts or now,
        cache_age_seconds   = _cache_age(state.get("timestamp")),
        market_hours        = _market_hours_info(now),
    )


def _build_pro(
    state: dict,
    universe_percentile: float | None = None,
    xs: dict | None = None,
) -> ProTierResponse:
    score = _composite_score(state)
    conf  = state.get("confidence") or {}
    confidence = _confidence_score(conf)
    flags = (conf.get("flags") or []) if isinstance(conf, dict) else []
    ts    = _parse_dt(state.get("timestamp"))
    now   = _now_utc()

    freshness_raw = state.get("freshness") or {}
    freshness = Freshness(
        market_as_of     = _parse_dt(freshness_raw.get("market_as_of")),
        narrative_as_of  = _parse_dt(freshness_raw.get("narrative_as_of")),
        influencer_as_of = _parse_dt(freshness_raw.get("influencer_as_of")),
        macro_as_of      = _parse_dt(freshness_raw.get("macro_as_of")),
    )

    raw_drivers = state.get("top_drivers") or []
    drivers = [
        Driver(
            signal       = d.get("signal", ""),
            description  = d.get("description", ""),
            direction    = d.get("direction", "neutral"),
            magnitude    = float(d.get("magnitude", 0.0)),
            source_layer = d.get("source_layer", ""),
        )
        for d in raw_drivers
        if isinstance(d, dict)
    ]

    layer_values = {
        "market":     _sub_val(state, "market"),
        "narrative":  _sub_val(state, "narrative"),
        "influencer": _sub_val(state, "influencer"),
        "macro":      _sub_val(state, "macro"),
    }
    missing_layers = [layer for layer, val in layer_values.items() if val is None]

    # score_raw: unsmoothed composite (pro only)
    raw_val = state.get("composite_score_raw")
    score_raw = int(round(raw_val)) if raw_val is not None else None

    # ema_obs_count: monotonic counter (pro only)
    obs_count = state.get("ema_obs_count")
    ema_obs_count = int(obs_count) if obs_count is not None else None

    change, change_pct = _change_fields(state)

    return ProTierResponse(
        ticker            = state["ticker"].upper(),
        score             = score,
        score_raw         = score_raw,
        score_change_1d     = change,
        score_change_1d_pct = change_pct,
        universe_percentile = universe_percentile,
        score_raw_z          = (xs or {}).get("raw_z"),
        score_raw_percentile = (xs or {}).get("raw_pctl"),
        sector_percentile    = (xs or {}).get("sector_pctl"),
        score_exo            = state.get("score_exo"),
        score_exo_percentile = (xs or {}).get("exo_pctl"),
        ema_obs_count     = ema_obs_count,
        label             = score_to_label(score),
        confidence        = confidence,
        sub_indices       = SubIndices(**layer_values),
        missing_layers    = missing_layers,
        divergence        = state.get("divergence"),
        top_drivers       = drivers,
        explanation       = state.get("explanation") or "",
        freshness         = freshness,
        confidence_flags  = list(flags),
        timestamp         = ts or now,
        cache_age_seconds = _cache_age(state.get("timestamp")),
        market_hours      = _market_hours_info(now),
    )


async def assemble(
    ticker: str,
    tier: str,
    detail: str = "summary",
) -> FreeTierResponse | ProTierResponse | NoDataResponse:
    """
    Load the scored state and return the appropriate response model.

    Parameters
    ----------
    ticker : Ticker symbol (upper-cased internally).
    tier   : 'free' or 'pro'.
    detail : 'summary' or 'full' (full only honoured for pro tier).
    """
    ticker = ticker.upper()

    state = await _load_from_redis(ticker)
    if state is None:
        _log.info("assembler: Redis miss for %s — falling back to DB", ticker)
        state = await _load_from_db(ticker)

    if state is None:
        return NoDataResponse(
            ticker  = ticker,
            status  = "insufficient_data",
            message = "Not enough historical data to compute a reliable sentiment score yet.",
        )

    use_full = (tier == "pro" and detail == "full")
    if use_full:
        percentile = await _load_percentile(ticker)
        xs = await _load_xs(ticker)
        return _build_pro(state, universe_percentile=percentile, xs=xs)
    return _build_free(state)
