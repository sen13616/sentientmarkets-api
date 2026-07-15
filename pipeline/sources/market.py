"""
pipeline/sources/market.py  — Layer 03

Fetches live market signals for a single ticker and writes them to raw_signals.

Signals produced
----------------
yf_open / yf_high / yf_low / yf_close / yf_volume
  Source:   yfinance batch download (called once in scheduler, passed in)
  Note:     Polygon /prev is NOT used as live fallback (returns yesterday's
            bar which would poison return/order-flow calculations)

bid_ask_spread / bid_ask_spread_bps / bid / ask
  Fetched: yfinance Ticker.info (market hours only)

rsi_14
  Computed: Wilder's smoothed RSI(14) from DB close-price history

order_flow_imbalance / buy_pressure / sell_pressure
  Computed: Lee-Ready OHLCV proxy (close location value) from current bar

return_1d / return_5d / return_20d / volume_ratio
  Computed: from DB close/volume history vs current live close/volume
"""
from __future__ import annotations

import asyncio
import logging
import math
import os
from datetime import datetime, timezone

_log = logging.getLogger(__name__)

import httpx
from dotenv import load_dotenv

from scripts.db.queries.raw_signals import (
    get_close_history,
    get_volume_history,
    insert_signals,
)
from pipeline.confidence.staleness import is_market_hours
from pipeline.rate_limits import (
    POLYGON_SEM, POLYGON_DELAY,
    YF_INFO_SEM,
    guarded_get,
)

load_dotenv(override=False)

_POLYGON_KEY  = os.environ.get("POLYGON_KEY", "")
_POLYGON_BASE = "https://api.polygon.io"


# ---------------------------------------------------------------------------
# OHLCV helpers
# ---------------------------------------------------------------------------

async def _ohlcv_polygon(ticker: str, client: httpx.AsyncClient) -> dict | None:
    """Polygon /v2/aggs/ticker/{ticker}/prev fallback."""
    resp = await guarded_get(
        client, f"{_POLYGON_BASE}/v2/aggs/ticker/{ticker}/prev",
        params={"adjusted": "true", "apiKey": _POLYGON_KEY},
        sem=POLYGON_SEM, delay=POLYGON_DELAY, label=f"Polygon prev-day {ticker}",
    )
    if resp is None or resp.status_code != 200:
        return None

    try:
        body = resp.json()
    except Exception as exc:
        _log.warning("Polygon prev-day JSON parse error for %s: %s", ticker, exc)
        return None

    results = body.get("results", [])
    if not results:
        return None

    try:
        r = results[0]
        ts = datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc)
        return {
            "open":      float(r["o"]),
            "high":      float(r["h"]),
            "low":       float(r["l"]),
            "close":     float(r["c"]),
            "volume":    float(r["v"]),
            "timestamp": ts,
            "source":    "polygon",
        }
    except (KeyError, ValueError) as exc:
        _log.warning("Polygon prev-day parse error for %s: %s", ticker, exc)
        return None


# ---------------------------------------------------------------------------
# Yahoo symbol mapping
# ---------------------------------------------------------------------------

def to_yahoo_symbol(ticker: str) -> str:
    """
    Map a universe ticker to Yahoo Finance's symbol convention.

    Class-share symbols use a dot in the universe (BRK.B, BF.B) but a dash
    on Yahoo (BRK-B, BF-B). Apply this at every yfinance call boundary ONLY —
    the universe/DB symbol stays dotted everywhere else.
    """
    return ticker.replace(".", "-")


# ---------------------------------------------------------------------------
# Bid-ask spread (yfinance Ticker.info)
# ---------------------------------------------------------------------------

def _fetch_bid_ask_spread(ticker: str) -> dict | None:
    """
    Fetch the current bid/ask quote via ``yf.Ticker(ticker).info`` and
    compute the spread in both raw dollars and basis points.

    This is a **blocking** call — always run via ``asyncio.to_thread``.

    Returns
    -------
    dict with keys ``bid``, ``ask``, ``spread``, ``spread_bps``,
    ``midpoint``, or None on any failure.
    """
    import yfinance as yf

    try:
        info = yf.Ticker(to_yahoo_symbol(ticker)).info
    except Exception as exc:
        _log.warning("yfinance Ticker.info failed for %s: %s", ticker, exc)
        return None

    bid = info.get("bid")
    ask = info.get("ask")

    if bid is None or ask is None:
        return None
    if bid <= 0 or ask <= 0 or ask < bid:
        return None

    midpoint = (bid + ask) / 2.0
    spread_bps = ((ask - bid) / midpoint) * 10_000

    return {
        "bid":        round(bid, 4),
        "ask":        round(ask, 4),
        "spread":     round(ask - bid, 4),
        "spread_bps": round(spread_bps, 2),
        "midpoint":   round(midpoint, 4),
    }


# ---------------------------------------------------------------------------
# RSI computation
# ---------------------------------------------------------------------------

def _compute_rsi(closes: list[float], period: int = 14) -> float | None:
    """
    Compute Wilder's smoothed RSI from a chronological series of close prices.

    Uses SMA for the initial seed, then Wilder's exponential smoothing for
    remaining data points.  More history → more stable output.

    Parameters
    ----------
    closes : Close prices sorted ascending (oldest first).
             Must have at least ``period + 1`` entries (15 for RSI-14).
    period : RSI lookback period (default 14).

    Returns
    -------
    RSI value (0–100), or None if insufficient data.
    """
    if len(closes) < period + 1:
        return None

    changes = [closes[i] - closes[i - 1] for i in range(1, len(closes))]

    # SMA seed from first `period` changes
    avg_gain = sum(max(c, 0) for c in changes[:period]) / period
    avg_loss = sum(-min(c, 0) for c in changes[:period]) / period

    # Wilder's exponential smoothing for remaining changes
    for c in changes[period:]:
        avg_gain = (avg_gain * (period - 1) + max(c, 0)) / period
        avg_loss = (avg_loss * (period - 1) + (-min(c, 0))) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return round(100.0 - (100.0 / (1.0 + rs)), 2)


# ---------------------------------------------------------------------------
# Order flow (Lee-Ready OHLCV proxy)
# ---------------------------------------------------------------------------

def _compute_order_flow(ohlcv: dict) -> list[tuple[str, float]]:
    """
    Estimate buy/sell pressure from a single OHLCV bar using the Close
    Location Value — the standard OHLCV proxy for Lee & Ready (1991)
    trade classification.

    CLV = (2·close − high − low) / (high − low), ranging from −1 (close
    at the low) to +1 (close at the high).  Volume is then split
    proportionally into buyer- and seller-initiated fractions.

    Returns
    -------
    [(signal_type, value)] for order_flow_imbalance, buy_pressure,
    sell_pressure.  Empty list if the bar is degenerate (zero volume or
    high == low).
    """
    high   = ohlcv["high"]
    low    = ohlcv["low"]
    close  = ohlcv["close"]
    volume = ohlcv["volume"]

    if volume <= 0 or high == low:
        return []

    clv = (2.0 * close - high - low) / (high - low)
    buy_frac = (1.0 + clv) / 2.0

    return [
        ("order_flow_imbalance", round(clv, 4)),
        ("buy_pressure",         round(buy_frac, 4)),
        ("sell_pressure",        round(1.0 - buy_frac, 4)),
    ]


# ---------------------------------------------------------------------------
# Computed signals (returns, volume ratio)
# ---------------------------------------------------------------------------

def _compute_returns(
    current_close: float,
    close_history: list[tuple[datetime, float]],
) -> list[tuple[str, float]]:
    """
    Return [(signal_type, value)] for 1d/5d/20d returns.
    close_history is sorted ascending (oldest first).
    The most recent entry is history[-1] (previous session's close).
    """
    closes = [c for _, c in close_history]
    results = []

    if len(closes) >= 1 and closes[-1] > 0 and current_close > 0:
        r1 = math.log(current_close / closes[-1])
        results.append(("return_1d", round(r1, 6)))

    if len(closes) >= 5 and closes[-5] > 0 and current_close > 0:
        r5 = math.log(current_close / closes[-5])
        results.append(("return_5d", round(r5, 6)))

    if len(closes) >= 20 and closes[-20] > 0 and current_close > 0:
        r20 = math.log(current_close / closes[-20])
        results.append(("return_20d", round(r20, 6)))

    return results


def _compute_volume_ratio(
    current_volume: float,
    volume_history: list[float],
) -> float | None:
    """volume_ratio = current_volume / mean(last 20 sessions' volume)."""
    if not volume_history:
        return None
    avg = sum(volume_history) / len(volume_history)
    if avg == 0:
        return None
    return round(current_volume / avg, 4)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def _run_market(
    ticker: str,
    client: httpx.AsyncClient,
    ohlcv_batch: dict | None = None,
) -> None:
    """Core implementation — requires a live client.

    Parameters
    ----------
    ohlcv_batch : Pre-fetched OHLCV dict from yfinance batch download.
                  ``{ticker: {open, high, low, close, volume, timestamp, source}}``.
                  When provided, the ticker's data is read from memory instead
                  of making an HTTP call.  Polygon is used as fallback for
                  tickers missing from the batch.
    """
    now = datetime.now(timezone.utc)
    rows: list[tuple] = []

    # --- OHLCV (yfinance batch only in live cycle) ---
    # Polygon /prev is NOT used as a live fallback because it returns
    # yesterday's bar, which downstream treats as today's — poisoning
    # return calculations and order flow.  _ohlcv_polygon remains
    # defined for backfill scripts that legitimately want prior-day data.
    ohlcv = (ohlcv_batch or {}).get(ticker)

    if ohlcv:
        src = ohlcv["source"]
        ts  = ohlcv["timestamp"]
        # yfinance data → yf_* signal types; Polygon fallback → ohlcv_*
        pfx = "yf" if src == "yfinance" else "ohlcv"
        rows.extend([
            (ticker, f"{pfx}_open",   ohlcv["open"],   src, "live", ts),
            (ticker, f"{pfx}_high",   ohlcv["high"],   src, "live", ts),
            (ticker, f"{pfx}_low",    ohlcv["low"],    src, "live", ts),
            (ticker, f"{pfx}_close",  ohlcv["close"],  src, "live", ts),
            (ticker, f"{pfx}_volume", ohlcv["volume"], src, "live", ts),
        ])

    # --- Bid-ask spread (yfinance .info, market hours only) ---
    if is_market_hours(now):
        async with YF_INFO_SEM:
            ba = await asyncio.to_thread(_fetch_bid_ask_spread, ticker)
        if ba is not None:
            _log.debug("bid_ask_spread %s bps=%.2f", ticker, ba["spread_bps"])
            rows.extend([
                (ticker, "bid_ask_spread",     ba["spread"],     "yfinance", "live", now),
                (ticker, "bid_ask_spread_bps", ba["spread_bps"], "yfinance", "live", now),
                (ticker, "bid",                ba["bid"],        "yfinance", "live", now),
                (ticker, "ask",                ba["ask"],        "yfinance", "live", now),
            ])
        else:
            _log.debug("bid_ask_spread %s: no data", ticker)

    # --- Order flow (Lee-Ready proxy from current OHLCV bar) ---
    if ohlcv:
        for sig_type, val in _compute_order_flow(ohlcv):
            rows.append((ticker, sig_type, val, "computed", "live", now))

    # --- Computed: RSI, returns, volume ratio ---
    # Fetch close history once — shared by RSI (needs 15+) and returns (needs 20).
    # Request 50 rows so Wilder's smoothing has room to stabilise.
    close_history = await get_close_history(ticker, limit=50)

    # RSI(14) from historical closes + current close (if available)
    closes_for_rsi = [c for _, c in close_history]
    if ohlcv:
        closes_for_rsi.append(ohlcv["close"])
    rsi = _compute_rsi(closes_for_rsi)
    if rsi is not None:
        rows.append((ticker, "rsi_14", rsi, "computed", "live", now))

    # Returns and volume ratio (require a current live close)
    if ohlcv:
        volume_history = await get_volume_history(ticker, limit=20)

        for sig_type, val in _compute_returns(ohlcv["close"], close_history):
            rows.append((ticker, sig_type, val, "computed", "live", now))

        vr = _compute_volume_ratio(ohlcv["volume"], volume_history)
        if vr is not None:
            rows.append((ticker, "volume_ratio", vr, "computed", "live", now))

    await insert_signals(rows)


async def fetch_market_signals(
    ticker: str,
    client: httpx.AsyncClient | None = None,
    ohlcv_batch: dict | None = None,
) -> None:
    """
    Fetch all Layer 03 market signals for `ticker` and write to raw_signals
    with upload_type='live'.

    Parameters
    ----------
    client      : Shared httpx.AsyncClient.  If None a temporary one is created.
    ohlcv_batch : Pre-fetched OHLCV dict from yfinance batch download
                  (scheduler passes this in for efficiency).
    """
    if client is None:
        async with httpx.AsyncClient(timeout=30) as _client:
            return await _run_market(ticker, _client, ohlcv_batch)
    return await _run_market(ticker, client, ohlcv_batch)
