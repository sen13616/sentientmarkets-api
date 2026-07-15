"""
pipeline/sources/influencer.py  — Layer 04

Fetches insider transactions and analyst signals for a single ticker
and writes them to raw_signals.

Signals produced
----------------
insider_net_shares
  Source:   Finnhub /stock/insider-transactions  (paper Data Collection table)
            Sprint P3.4 — removed the SEC EDGAR Form 4 primary path which
            produced 0 rows in production (SGML wrapper breaks
            xml.etree.ElementTree.fromstring); Finnhub fallback was already
            carrying 100% of the load.

analyst_buy_pct
  Source:   Finnhub /stock/recommendation
            = (strongBuy + buy) / total recommendations  (0–1 scale)

analyst_target_price
  Source:   yfinance — Ticker(t).info.get("targetMeanPrice")
            (paper Data Collection table; Sprint P3.2 — replaced Finnhub
            /stock/price-target which 403s on free tier)

analyst_eps_estimate_mean
  Source:   yfinance — Ticker(t).get_earnings_estimate() 0q row 'avg' column
            (paper Data Collection table; Sprint P3.3 — raw stored value;
            scoring derives period-over-period delta as
            ``earnings_estimate_revision``)

All written with upload_type='live'.
"""
from __future__ import annotations

import asyncio
import os
import logging
from datetime import datetime, timedelta, timezone

import httpx
import yfinance as yf
from dotenv import load_dotenv

from scripts.db.queries.raw_signals import insert_signals
from pipeline.rate_limits import (
    FINNHUB_SEM, FINNHUB_DELAY,
    guarded_get,
)
from pipeline.sources.market import to_yahoo_symbol

_log = logging.getLogger(__name__)

load_dotenv(override=False)

_FINNHUB_KEY = os.environ.get("FINNHUB_KEY", "")

_FINNHUB_BASE    = "https://finnhub.io/api/v1"

_INSIDER_LOOKBACK_DAYS = 30


# ---------------------------------------------------------------------------
# Finnhub insider transactions
# ---------------------------------------------------------------------------

async def _insider_finnhub(ticker: str, client: httpx.AsyncClient) -> list[tuple]:
    """
    Finnhub /stock/insider-transactions fallback.
    Returns list of (ticker, 'insider_net_shares', value, 'finnhub', 'live', timestamp).
    """
    from_date = (datetime.now(timezone.utc) - timedelta(days=_INSIDER_LOOKBACK_DAYS)).date()
    rows: list[tuple] = []

    resp = await guarded_get(
        client, f"{_FINNHUB_BASE}/stock/insider-transactions",
        params={"symbol": ticker, "from": str(from_date), "token": _FINNHUB_KEY},
        sem=FINNHUB_SEM, delay=FINNHUB_DELAY, label=f"Finnhub insider-txn {ticker}",
    )
    if resp is None or resp.status_code != 200:
        # 403 expected on Finnhub free tier
        _log.debug("Finnhub insider-transactions unavailable for %s", ticker)
        return rows

    try:
        body = resp.json()
    except Exception as exc:
        _log.debug("Finnhub insider-transactions JSON parse failed for %s: %s", ticker, exc)
        return rows

    for txn in body.get("data", []):
        try:
            change = float(txn.get("change") or 0)
            date_str = txn.get("transactionDate") or txn.get("filingDate", "")
            ts = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            rows.append((ticker, "insider_net_shares", change, "finnhub", "live", ts))
        except (ValueError, TypeError):
            continue

    return rows


# ---------------------------------------------------------------------------
# Analyst signals (Finnhub)
# ---------------------------------------------------------------------------

async def _analyst_recommendations(
    ticker: str,
    client: httpx.AsyncClient,
) -> float | None:
    """
    Finnhub /stock/recommendation → analyst_buy_pct.
    = (strongBuy + buy) / (strongBuy + buy + hold + sell + strongSell).
    Returns the most recent period's ratio (0–1), or None.
    """
    resp = await guarded_get(
        client, f"{_FINNHUB_BASE}/stock/recommendation",
        params={"symbol": ticker, "token": _FINNHUB_KEY},
        sem=FINNHUB_SEM, delay=FINNHUB_DELAY, label=f"Finnhub recommendation {ticker}",
    )
    if resp is None or resp.status_code != 200:
        # 403 expected on Finnhub free tier
        _log.debug("Finnhub recommendation unavailable for %s", ticker)
        return None

    try:
        data = resp.json()
    except Exception as exc:
        _log.debug("Finnhub recommendation JSON parse failed for %s: %s", ticker, exc)
        return None

    if not isinstance(data, list) or not data:
        return None

    try:
        latest = data[0]  # most recent period
        strong_buy = int(latest.get("strongBuy") or 0)
        buy        = int(latest.get("buy")        or 0)
        hold       = int(latest.get("hold")       or 0)
        sell       = int(latest.get("sell")       or 0)
        strong_sell = int(latest.get("strongSell") or 0)
        total = strong_buy + buy + hold + sell + strong_sell
        if total == 0:
            return None
        return round((strong_buy + buy) / total, 4)
    except (KeyError, ValueError, TypeError):
        return None


async def _analyst_target_yf(ticker: str) -> float | None:
    """yfinance Ticker(t).info["targetMeanPrice"] → analyst_target_price.

    Paper Data Collection table specifies yfinance for this signal. Wrapped in
    asyncio.to_thread because yfinance is synchronous (mirrors macro.py:75-89).
    """
    def _fetch() -> float | None:
        info = yf.Ticker(to_yahoo_symbol(ticker)).info
        target = info.get("targetMeanPrice")
        if target is None:
            return None
        try:
            value = float(target)
        except (TypeError, ValueError):
            return None
        if value <= 0:
            return None
        return value

    try:
        return await asyncio.to_thread(_fetch)
    except Exception as exc:
        _log.debug("yfinance targetMeanPrice unavailable for %s: %s", ticker, exc)
        return None


async def _earnings_estimate_yf(ticker: str) -> float | None:
    """yfinance Ticker(t).get_earnings_estimate() → current-quarter mean EPS.

    Returns the `avg` column of the `0q` row (current fiscal quarter). Stored
    raw at signal_type ``analyst_eps_estimate_mean``; scoring derives the
    period-over-period delta from history (Sprint P3.3).
    """
    def _fetch() -> float | None:
        est = yf.Ticker(to_yahoo_symbol(ticker)).get_earnings_estimate()
        if est is None:
            return None
        try:
            if est.empty or "0q" not in est.index:
                return None
        except AttributeError:
            return None
        try:
            row = est.loc["0q"]
        except KeyError:
            return None
        avg = row.get("avg")
        if avg is None:
            return None
        try:
            value = float(avg)
        except (TypeError, ValueError):
            return None
        if value != value:  # NaN
            return None
        return value

    try:
        return await asyncio.to_thread(_fetch)
    except Exception as exc:
        _log.debug("yfinance get_earnings_estimate unavailable for %s: %s", ticker, exc)
        return None


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def _run_influencer(ticker: str, client: httpx.AsyncClient) -> None:
    """Core implementation — requires a live client."""
    now  = datetime.now(timezone.utc)
    rows: list[tuple] = []

    # --- Insider transactions (Finnhub per paper Data Collection table) ---
    rows.extend(await _insider_finnhub(ticker, client))

    # --- Analyst buy percentage ---
    buy_pct = await _analyst_recommendations(ticker, client)
    if buy_pct is not None:
        rows.append((ticker, "analyst_buy_pct", buy_pct, "finnhub", "live", now))

    # --- Analyst target price (P3.2: yfinance per paper Data Collection table) ---
    target = await _analyst_target_yf(ticker)
    if target is not None:
        rows.append((ticker, "analyst_target_price", target, "yfinance", "live", now))

    # --- Earnings estimate mean EPS (P3.3: yfinance current-quarter avg) ---
    eps = await _earnings_estimate_yf(ticker)
    if eps is not None:
        rows.append((ticker, "analyst_eps_estimate_mean", eps, "yfinance", "live", now))

    await insert_signals(rows)


async def fetch_influencer_signals(
    ticker: str,
    client: httpx.AsyncClient | None = None,
) -> None:
    """
    Fetch Layer 04 influencer signals for `ticker` and write to raw_signals.

    If `client` is None a new AsyncClient is created internally, making this
    function callable standalone (e.g. in tests or one-off scripts).
    Pass a shared client from the orchestrator for efficiency.
    """
    if client is None:
        async with httpx.AsyncClient(timeout=30) as _client:
            return await _run_influencer(ticker, _client)
    return await _run_influencer(ticker, client)
