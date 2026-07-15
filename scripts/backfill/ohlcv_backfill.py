"""
backfill/ohlcv_backfill.py

Pull 2 years of daily OHLCV for all tier1 tickers from Alpha Vantage
(TIME_SERIES_DAILY_ADJUSTED, outputsize=full) and write to raw_signals
with upload_type='manual_backfill'.

Requires a premium Alpha Vantage key — free tier does not include this
endpoint. Set ALPHA_VANTAGE_KEY in .env.

Rate limiting:
  Premium standard tier: 75 requests/minute.
  INTER_REQUEST_DELAY = 0.8 s → ~62 req/min (safe margin below 75).
  No daily cap on premium plans.

The script is resumable: tickers with existing ohlcv_close rows are
skipped automatically.

Usage:
  python backfill/ohlcv_backfill.py
"""
import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts.db.connection import close_pool, get_pool, init_pool

load_dotenv(override=False)

AV_BASE = "https://www.alphavantage.co/query"
LOOKBACK_DAYS = 730
INTER_REQUEST_DELAY = 0.8   # seconds between requests (~62 req/min)


# --------------------------------------------------------------------------- #
# Alpha Vantage helpers                                                         #
# --------------------------------------------------------------------------- #

async def _fetch_time_series(
    client: httpx.AsyncClient,
    ticker: str,
    api_key: str,
) -> dict[str, dict]:
    """
    Call TIME_SERIES_DAILY_ADJUSTED with outputsize=full.
    Returns the inner time-series dict, or {} on any failure / rate-limit.
    """
    try:
        resp = await client.get(
            AV_BASE,
            params={
                "function":   "TIME_SERIES_DAILY_ADJUSTED",
                "symbol":     ticker,
                "outputsize": "full",
                "apikey":     api_key,
            },
        )
        resp.raise_for_status()
        body = resp.json()
    except Exception as exc:
        print(f"    [!] HTTP error for {ticker}: {exc}")
        return {}

    if "Note" in body or "Information" in body:
        msg = (body.get("Note") or body.get("Information") or "")[:120]
        print(f"    [!] AV message: {msg}")
        return {}

    ts = body.get("Time Series (Daily)")
    if not ts:
        print(f"    [!] No time-series data for {ticker}. Keys: {list(body)}")
        return {}

    return ts


# --------------------------------------------------------------------------- #
# Row builder                                                                   #
# --------------------------------------------------------------------------- #

def _build_rows(
    ticker: str,
    time_series: dict[str, dict],
    cutoff: datetime,
) -> list[tuple]:
    rows: list[tuple] = []
    for date_str, ohlcv in time_series.items():
        try:
            ts = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            continue

        if ts < cutoff:
            continue

        try:
            signals = [
                ("ohlcv_open",           float(ohlcv["1. open"])),
                ("ohlcv_high",           float(ohlcv["2. high"])),
                ("ohlcv_low",            float(ohlcv["3. low"])),
                ("ohlcv_close",          float(ohlcv["4. close"])),
                ("ohlcv_adjusted_close", float(ohlcv["5. adjusted close"])),
                ("ohlcv_volume",         float(ohlcv["6. volume"])),
            ]
        except (KeyError, ValueError) as exc:
            print(f"    [!] Parse error on {date_str}: {exc}")
            continue

        for signal_type, value in signals:
            rows.append((
                ticker, signal_type, value,
                "alpha_vantage", "manual_backfill", ts,
            ))

    return rows


# --------------------------------------------------------------------------- #
# DB helpers                                                                    #
# --------------------------------------------------------------------------- #

async def _already_backfilled(ticker: str) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        n = await conn.fetchval(
            """
            SELECT COUNT(*) FROM raw_signals
            WHERE ticker      = $1
              AND signal_type = 'ohlcv_close'
              AND upload_type = 'manual_backfill'
            """,
            ticker,
        )
    return n > 0


async def _insert_rows(rows: list[tuple]) -> None:
    if not rows:
        return
    pool = await get_pool()
    async with pool.acquire() as conn:
        # CAUTION: blind INSERT — bypasses insert_signals()'s dedup guard
        # (NOT EXISTS on ticker/signal_type/timestamp/value/source). Re-running
        # this script can create duplicate rows; dedupe manually or port to
        # scripts.db.queries.raw_signals.insert_signals() before reuse.
        await conn.executemany(
            """
            INSERT INTO raw_signals
                (ticker, signal_type, value, source, upload_type, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            rows,
        )


# --------------------------------------------------------------------------- #
# Main                                                                          #
# --------------------------------------------------------------------------- #

async def main() -> None:
    api_key = os.environ.get("ALPHA_VANTAGE_KEY", "")
    if not api_key:
        sys.exit("ALPHA_VANTAGE_KEY not set in environment / .env")

    await init_pool()
    pool = await get_pool()

    async with pool.acquire() as conn:
        tickers: list[str] = [
            r["ticker"]
            for r in await conn.fetch(
                "SELECT ticker FROM ticker_universe"
                " WHERE tier = 'tier1_supported' ORDER BY ticker"
            )
        ]

    total = len(tickers)
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    print(f"OHLCV backfill — {total} tier1 tickers via Alpha Vantage.")
    print(f"Lookback: {LOOKBACK_DAYS} days. Delay: {INTER_REQUEST_DELAY}s/request.\n")

    requests_made = 0
    processed = 0
    skipped = 0

    async with httpx.AsyncClient(timeout=30) as client:
        for idx, ticker in enumerate(tickers, 1):
            if await _already_backfilled(ticker):
                skipped += 1
                print(f"  [{idx}/{total}] {ticker}: already done — skipped.")
                continue

            print(f"  [{idx}/{total}] Fetching {ticker} ...", end="", flush=True)
            time_series = await _fetch_time_series(client, ticker, api_key)
            requests_made += 1

            if not time_series:
                print(" no data.")
                await asyncio.sleep(INTER_REQUEST_DELAY)
                continue

            rows = _build_rows(ticker, time_series, cutoff)
            if rows:
                await _insert_rows(rows)
                trading_days = len(rows) // 6
                print(f" {trading_days} days ({len(rows)} rows).")
                processed += 1
            else:
                print(" 0 rows within 2-year window.")

            if idx < total:
                await asyncio.sleep(INTER_REQUEST_DELAY)

    await close_pool()
    print(
        f"\nDone.  Requests: {requests_made}  |  "
        f"Processed: {processed}  |  Skipped (already done): {skipped}"
    )


if __name__ == "__main__":
    asyncio.run(main())
