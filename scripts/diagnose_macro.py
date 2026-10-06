"""One-shot diagnostic for the macro sub-index pipeline.

Checks:
  1. Last-run timestamps in Redis for macro_daily / macro_intraday / scoring_tick.
  2. Most recent timestamps per macro signal type in raw_signals.
  3. Sentiment-history coverage of macro_sub_index over the last 24h.
  4. A few sample tickers' latest macro_sub_index.
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv

load_dotenv(override=True)

from scripts.db.connection import close_pool, get_pool
from scripts.db.redis import close_redis, get_redis, init_redis

MACRO_SIGNALS_GLOBAL = ["vix", "treasury_yield_10y", "treasury_yield_2y", "ted_spread"]
SECTOR_ETFS = ["XLK", "XLV", "XLF", "XLY", "XLP", "XLE", "XLI", "XLB", "XLU", "XLRE", "XLC"]


async def main() -> None:
    await init_redis()
    redis = get_redis()
    pool = await get_pool()
    now = datetime.now(timezone.utc)

    print(f"now (UTC): {now.isoformat()}\n")

    # --- 1. Job run timestamps ------------------------------------------------
    print("=== Redis: last-run timestamps ===")
    for job_id in ("macro_daily", "macro_intraday", "scoring_tick", "market", "narrative"):
        val = await redis.get(f"pipeline:last_run:{job_id}")
        if val is None:
            print(f"  {job_id:18s}  MISSING")
            continue
        val = val.decode() if isinstance(val, bytes) else val
        try:
            ts = datetime.fromisoformat(val)
            age = now - ts
            print(f"  {job_id:18s}  {val}   (age: {age})")
        except Exception:
            print(f"  {job_id:18s}  {val}  (unparseable)")

    # --- 2. Latest macro signals in DB ---------------------------------------
    print("\n=== raw_signals: latest macro signals under _MACRO_ ===")
    async with pool.acquire() as con:
        for sig in MACRO_SIGNALS_GLOBAL:
            row = await con.fetchrow(
                "SELECT value, source, timestamp FROM raw_signals "
                "WHERE ticker = '_MACRO_' AND signal_type = $1 "
                "ORDER BY timestamp DESC LIMIT 1",
                sig,
            )
            if row is None:
                print(f"  {sig:22s}  NO ROWS")
            else:
                age = now - row["timestamp"]
                print(f"  {sig:22s}  value={row['value']:>10.4f}  source={row['source']:<14s}  ts={row['timestamp'].isoformat()}  (age: {age})")

        print("\n=== raw_signals: latest sector-ETF closes (signal_type='ohlcv_close') ===")
        for etf in SECTOR_ETFS:
            row = await con.fetchrow(
                "SELECT value, source, timestamp FROM raw_signals "
                "WHERE ticker = $1 AND signal_type = 'ohlcv_close' "
                "ORDER BY timestamp DESC LIMIT 1",
                etf,
            )
            if row is None:
                print(f"  {etf:6s}  NO ROWS")
            else:
                age = now - row["timestamp"]
                print(f"  {etf:6s}  value={row['value']:>10.4f}  source={row['source']:<14s}  ts={row['timestamp'].isoformat()}  (age: {age})")

        # --- 3. Discover sentiment_history columns ---------------------------
        print("\n=== sentiment_history: columns ===")
        cols = await con.fetch(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = 'sentiment_history' ORDER BY ordinal_position"
        )
        col_names = [c["column_name"] for c in cols]
        for c in cols:
            print(f"  {c['column_name']:30s}  {c['data_type']}")

        # Pick the most likely time + macro columns
        time_col = next(
            (c for c in ("created_at", "timestamp", "ts", "scored_at") if c in col_names),
            None,
        )
        macro_col = next(
            (c for c in ("macro_sub_index", "macro_index", "macro_score", "macro") if c in col_names),
            None,
        )
        market_col = next(
            (c for c in ("market_sub_index", "market_index", "market_score") if c in col_names),
            None,
        )
        print(f"\n  using time_col={time_col!r}  macro_col={macro_col!r}  market_col={market_col!r}")

        if time_col and macro_col:
            print("\n=== sentiment_history: macro coverage (last 24h) ===")
            cutoff = now - timedelta(hours=24)
            total = await con.fetchval(
                f"SELECT COUNT(*) FROM sentiment_history WHERE {time_col} >= $1",
                cutoff,
            )
            with_macro = await con.fetchval(
                f"SELECT COUNT(*) FROM sentiment_history "
                f"WHERE {time_col} >= $1 AND {macro_col} IS NOT NULL",
                cutoff,
            )
            print(f"  total rows (24h):    {total}")
            print(f"  with {macro_col}:    {with_macro}")
            if total:
                pct = 100.0 * (with_macro or 0) / total
                print(f"  macro present:       {pct:.1f}%")

            print("\n=== sentiment_history: latest row for sample tickers ===")
            for ticker in ("AAPL", "MSFT", "NVDA", "JPM", "XOM"):
                row = await con.fetchrow(
                    f"SELECT {macro_col} AS macro, "
                    f"{market_col + ' AS market,' if market_col else ''} "
                    f"{time_col} AS ts "
                    f"FROM sentiment_history WHERE ticker = $1 "
                    f"ORDER BY {time_col} DESC LIMIT 1",
                    ticker,
                )
                if row is None:
                    print(f"  {ticker:6s}  NO ROWS")
                else:
                    m = row["macro"]
                    mk = row["market"] if market_col else "?"
                    print(f"  {ticker:6s}  macro={str(m):>8s}  market={str(mk):>8s}  ts={row['ts'].isoformat()}")

    await close_pool()
    await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
