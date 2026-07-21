"""Backfill positioning research features over historical sentiment_history rows.

    python3 -m scripts.eval.backfill_features [--tickers AAPL,MSFT] [--dry-run]

Track B4 (2026-07-22). Computes the positioning features defined in
pipeline/features/positioning.py (short_vol_z, insider_net_z) over all retained
raw_signals history and writes them into `sentiment_history.research_features`
(JSONB, migration 012).

Storage decision (documented per B4): features are written into
sentiment_history.research_features directly — NOT a side export — so the eval
harness has a single source of truth (scripts/eval/data.py reads the column and
auto-registers every key). The write targets the LAST tick per (ticker, ET
calendar day): exactly the row `load_sentiment_panel(granularity="daily")`
selects, so the daily analysis sees every backfilled value. Other ticks of the
day keep research_features NULL (positioning features are daily-cadence).

Point-in-time discipline: for a target row stamped T, feature inputs are
restricted to raw_signals rows with timestamp <= T — a (ticker, day) never
sees a signal that had not been written yet. Caveat inherited from ingestion:
insider rows are stamped with the Finnhub transactionDate, which can precede
the public filing by up to ~2 business days (see positioning.py docstring) —
the backfill preserves that basis so live and backfilled values agree.

Idempotent and re-runnable: writes use
    research_features = COALESCE(research_features, '{}') || $new
so re-runs overwrite the same keys in place and never clobber keys written by
other tools (e.g. the live tick path).
"""

from __future__ import annotations

import argparse
import asyncio
import bisect
import json
import sys
from datetime import date

from dotenv import load_dotenv

# stale DATABASE_URL may be exported in the shell — .env must win in scripts
load_dotenv(override=True)

from scripts.db.connection import close_pool, get_pool  # noqa: E402
from pipeline.features.positioning import (  # noqa: E402
    WINDOW,
    insider_net_z_from_daily,
    short_vol_z_from_series,
)


async def _target_rows(conn, ticker: str) -> list[dict]:
    """Last sentiment_history tick per (ticker, ET calendar day) — the row the
    harness daily loader selects. Returns [{id, timestamp}] oldest first."""
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (((timestamp AT TIME ZONE 'America/New_York')::date))
               id, timestamp
          FROM sentiment_history
         WHERE ticker = $1
         ORDER BY ((timestamp AT TIME ZONE 'America/New_York')::date),
                  timestamp DESC
        """,
        ticker,
    )
    return sorted((dict(r) for r in rows), key=lambda r: r["timestamp"])


async def _signal_rows(conn, ticker: str, signal_type: str) -> list[tuple]:
    """[(timestamp, value)] for ticker/signal_type, oldest first."""
    rows = await conn.fetch(
        """
        SELECT timestamp, value FROM raw_signals
         WHERE ticker = $1 AND signal_type = $2
         ORDER BY timestamp
        """,
        ticker,
        signal_type,
    )
    return [(r["timestamp"], float(r["value"])) for r in rows]


def compute_features_for_ticker(
    targets: list[dict],
    sv_rows: list[tuple],
    insider_rows: list[tuple],
) -> list[tuple[int, dict]]:
    """Pure per-ticker computation -> [(sentiment_history_id, features_dict)].

    Point-in-time: for a target at time T only rows with timestamp <= T enter
    the feature. Targets where nothing computes are omitted entirely.
    """
    sv_ts = [ts for ts, _ in sv_rows]
    sv_vals = [v for _, v in sv_rows]
    ins_ts = [ts for ts, _ in insider_rows]

    out: list[tuple[int, dict]] = []
    for tgt in targets:
        t = tgt["timestamp"]
        feats: dict[str, float] = {}

        # short_vol_z: series of ratio values with ts <= t, last WINDOW+1
        hi = bisect.bisect_right(sv_ts, t)
        if hi >= 2:
            z = short_vol_z_from_series(sv_vals[max(0, hi - (WINDOW + 1)):hi])
            if z is not None:
                feats["short_vol_z"] = z

        # insider_net_z: daily net sums (UTC date) from rows with ts <= t
        hi_i = bisect.bisect_right(ins_ts, t)
        if hi_i > 0:
            daily: dict[date, float] = {}
            for ts, v in insider_rows[:hi_i]:
                d = ts.date()
                daily[d] = daily.get(d, 0.0) + v
            z = insider_net_z_from_daily(daily, t.date())
            if z is not None:
                feats["insider_net_z"] = z

        if feats:
            out.append((tgt["id"], feats))
    return out


async def _run(tickers_arg: str | None, dry_run: bool) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        if tickers_arg:
            tickers = [t.strip().upper() for t in tickers_arg.split(",") if t.strip()]
        else:
            rows = await conn.fetch("SELECT DISTINCT ticker FROM sentiment_history")
            tickers = sorted(r["ticker"] for r in rows)

        total_rows = 0
        total_tickers = 0
        for i, ticker in enumerate(tickers, 1):
            targets = await _target_rows(conn, ticker)
            if not targets:
                continue
            sv_rows = await _signal_rows(conn, ticker, "short_volume_ratio_otc")
            insider_rows = await _signal_rows(conn, ticker, "insider_net_shares")
            updates = compute_features_for_ticker(targets, sv_rows, insider_rows)
            if updates and not dry_run:
                await conn.executemany(
                    """
                    UPDATE sentiment_history
                       SET research_features =
                           COALESCE(research_features, '{}'::jsonb) || $2::jsonb
                     WHERE id = $1
                    """,
                    [(row_id, json.dumps(feats)) for row_id, feats in updates],
                )
            total_rows += len(updates)
            total_tickers += 1 if updates else 0
            if i % 50 == 0 or i == len(tickers):
                print(f"[{i}/{len(tickers)}] {ticker}: cumulative "
                      f"{total_rows} rows across {total_tickers} tickers"
                      f"{' (dry-run)' if dry_run else ''}")

    print(f"done: {total_rows} sentiment_history rows "
          f"{'would be ' if dry_run else ''}updated across {total_tickers} tickers")
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--tickers", help="comma-separated subset (default: all)")
    p.add_argument("--dry-run", action="store_true",
                   help="compute and report counts without writing")
    args = p.parse_args(argv)

    async def _main() -> int:
        try:
            return await _run(args.tickers, args.dry_run)
        finally:
            await close_pool()

    try:
        return asyncio.run(_main())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
