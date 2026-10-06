"""
research/snapshot.py — export a versioned, point-in-time research snapshot.

Writes Parquet files + a manifest to ``<out>/<end date>/`` so strategy tests run
offline and reproducibly, never against the production database:

    sentiment_daily.parquet   last scoring tick per (ticker, US/Eastern day):
                              raw + smoothed + exogenous composite, the four
                              sub-indices, confidence, divergence, replay_run
    sentiment_ticks.parquet   every scoring tick (only with --ticks; large)
    prices_daily.parquet      daily adjusted open/close/volume per ticker
    universe.parquet          ticker metadata + lifecycle (added_at, delisted_at,
                              successor_ticker, in_sp500, sp500_added, sector)
    manifest.json             window, row counts, git commit, schema version,
                              documented gaps, replay runs present

All queries are read-only. Usage:

    python -m research.snapshot --start 2026-05-21 --end 2026-10-06 [--ticks]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

SCHEMA_VERSION = 1
DEFAULT_OUT = _ROOT / "data" / "snapshots"

# Remaining sentiment gaps after the 2026 repairs (METHODOLOGY.md §16.5):
# no scoring ticks exist inside these windows.
KNOWN_GAPS = [
    ("2026-07-17T15:30:00Z", "2026-07-18T04:00:00Z"),
    ("2026-07-23T20:30:00Z", "2026-07-24T07:00:00Z"),
    ("2026-08-10T03:00:00Z", "2026-08-10T04:22:00Z"),
]

_SENTIMENT_COLS = """
    ticker, timestamp, composite_score, composite_score_smoothed, composite_score_exo,
    market_index, narrative_index, influencer_index, macro_index,
    confidence_score, divergence, replay_run
"""

SENTIMENT_DAILY_SQL = f"""
    SELECT DISTINCT ON (ticker, ((timestamp AT TIME ZONE 'America/New_York')::date))
           {_SENTIMENT_COLS}
      FROM sentiment_history
     WHERE timestamp >= $1 AND timestamp < $2
     ORDER BY ticker, ((timestamp AT TIME ZONE 'America/New_York')::date), timestamp DESC
"""

SENTIMENT_TICKS_SQL = f"""
    SELECT {_SENTIMENT_COLS}
      FROM sentiment_history
     WHERE timestamp >= $1 AND timestamp < $2
     ORDER BY ticker, timestamp
"""

# One value per (ticker, bar date, field): the latest-written row, i.e. the
# end-of-day bar (same tie-break as the corrected live history readers).
PRICES_DAILY_SQL = """
    SELECT DISTINCT ON (ticker, ((timestamp AT TIME ZONE 'UTC')::date), field)
           ticker,
           (timestamp AT TIME ZONE 'UTC')::date AS date,
           field,
           value
      FROM (
            SELECT ticker, timestamp, created_at, value,
                   CASE WHEN signal_type IN ('yf_close', 'ohlcv_close') THEN 'close'
                        WHEN signal_type IN ('yf_open', 'ohlcv_open')   THEN 'open'
                        ELSE 'volume' END AS field
              FROM raw_signals
             WHERE signal_type IN ('yf_close', 'ohlcv_close', 'yf_open', 'ohlcv_open',
                                   'yf_volume', 'ohlcv_volume')
               AND timestamp >= $1 AND timestamp < $2
           ) p
     ORDER BY ticker, ((timestamp AT TIME ZONE 'UTC')::date), field, timestamp DESC, created_at DESC
"""

UNIVERSE_SQL = """
    SELECT ticker, company_name, sector, in_sp500, sp500_added,
           added_at, delisted_at, successor_ticker, delisted_reason
      FROM ticker_universe
     WHERE tier = 'tier1_supported'
     ORDER BY ticker
"""


def _git_commit() -> str | None:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=_ROOT, capture_output=True, text=True, check=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def _frame(rows) -> pd.DataFrame:
    return pd.DataFrame([dict(r) for r in rows])


def tidy_prices(long: pd.DataFrame) -> pd.DataFrame:
    """Long (ticker, date, field, value) → one row per (ticker, date) with open/close/volume."""
    if long.empty:
        return pd.DataFrame(columns=["ticker", "date", "open", "close", "volume"])
    wide = long.pivot_table(
        index=["ticker", "date"], columns="field", values="value", aggfunc="last"
    ).reset_index()
    wide.columns.name = None
    for col in ("open", "close", "volume"):
        if col not in wide:
            wide[col] = float("nan")
    wide["date"] = pd.to_datetime(wide["date"])
    return wide[["ticker", "date", "open", "close", "volume"]].sort_values(["ticker", "date"])


def build_manifest(
    start: date,
    end: date,
    counts: dict[str, int],
    replay_runs: list[str],
    commit: str | None,
) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "created_at": datetime.now(UTC).isoformat(),
        "window": {"start": start.isoformat(), "end_exclusive": end.isoformat()},
        "git_commit": commit,
        "row_counts": counts,
        "replay_runs": replay_runs,
        "known_gaps": [{"start": a, "end": b} for a, b in KNOWN_GAPS],
        "conventions": {
            "sentiment_daily": "last scoring tick per ticker per US/Eastern calendar day",
            "prices": "adjusted daily bars (yfinance auto_adjust); date = bar date",
            "replay_run": "NULL = served live; otherwise recomputed offline (METHODOLOGY.md §16.5)",
            "universe": "added_at / delisted_at give point-in-time membership; "
            "in_sp500 is a snapshot as of the export, not membership history",
        },
    }


async def export(start: date, end: date, out_root: Path, ticks: bool = False) -> Path:
    """Run the read-only queries and write the snapshot directory."""
    from scripts.db.connection import close_pool, get_pool, init_pool

    t0 = datetime(start.year, start.month, start.day, tzinfo=UTC)
    t1 = datetime(end.year, end.month, end.day, tzinfo=UTC)
    out = out_root / end.isoformat()
    out.mkdir(parents=True, exist_ok=True)

    await init_pool(command_timeout=1800)
    try:
        pool = await get_pool()
        sentiment = _frame(await pool.fetch(SENTIMENT_DAILY_SQL, t0, t1))
        # 120 days of prices before the window feed returns/benchmarks at its start
        prices = tidy_prices(
            _frame(await pool.fetch(PRICES_DAILY_SQL, t0 - timedelta(days=120), t1))
        )
        universe = _frame(await pool.fetch(UNIVERSE_SQL))
        tick_rows = _frame(await pool.fetch(SENTIMENT_TICKS_SQL, t0, t1)) if ticks else None
    finally:
        await close_pool()

    sentiment.to_parquet(out / "sentiment_daily.parquet", index=False)
    prices.to_parquet(out / "prices_daily.parquet", index=False)
    universe.to_parquet(out / "universe.parquet", index=False)
    counts = {
        "sentiment_daily": len(sentiment),
        "prices_daily": len(prices),
        "universe": len(universe),
    }
    if tick_rows is not None:
        tick_rows.to_parquet(out / "sentiment_ticks.parquet", index=False)
        counts["sentiment_ticks"] = len(tick_rows)

    replay_runs = (
        sorted(sentiment["replay_run"].dropna().unique().tolist()) if len(sentiment) else []
    )
    manifest = build_manifest(start, end, counts, replay_runs, _git_commit())
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--start", required=True, type=date.fromisoformat, help="YYYY-MM-DD (UTC)")
    p.add_argument("--end", required=True, type=date.fromisoformat, help="YYYY-MM-DD, exclusive")
    p.add_argument("--out", type=Path, default=DEFAULT_OUT, help=f"default: {DEFAULT_OUT}")
    p.add_argument("--ticks", action="store_true", help="also export every scoring tick (large)")
    args = p.parse_args(argv)

    load_dotenv(_ROOT / ".env", override=True)
    out = asyncio.run(export(args.start, args.end, args.out, args.ticks))
    manifest = json.loads((out / "manifest.json").read_text())
    print(f"snapshot written to {out}")
    print(json.dumps(manifest["row_counts"], indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
