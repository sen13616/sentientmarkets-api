"""
scripts/backfill/reconstruct_signals.py

Rebuild the market-derived (and optionally macro) raw_signals that the live
jobs would have written during an outage, from yfinance bars, so an offline
replay (replay_scores.py) can score those ticks with the market layer present.

Uses the live pure functions — market._compute_order_flow / _compute_rsi /
_compute_returns / _compute_volume_ratio and macro._compute_etf_return_20d —
on bars rebuilt from yfinance:

  - marks follow the live schedule at hourly resolution: weekdays
    14:00…20:00 UTC on the hour (market_job / macro_intraday_job), plus a
    21:15 UTC end-of-day mark (market_eod_job, full daily bar)
  - at mark T the partial daily bar aggregates the 1h bars that had
    finished by T (open/high/low/close/cumulative volume); none → no row
  - "previous close" = the prior trading day's final close. Live code reads
    it via get_close_history(), whose DISTINCT ON date can pick an earlier
    *same-day* partial close during trading hours (an arbitrary row — not
    reproducible), so rebuilt intraday return_1d is the intended 1-day return.

Rows: source='computed' (macro: VIX source='yfinance'), upload_type=
'manual_backfill'. Idempotent via insert_signals' NOT EXISTS guard.

Retention: rows older than 45 days (market-derived) / 90 days (macro) are
deleted by the next 03:30 UTC retention_job — rebuild and replay such ranges
within one retention cycle.

Usage
-----
    python3 scripts/backfill/reconstruct_signals.py --parity            # compare vs live rows, no writes
    python3 scripts/backfill/reconstruct_signals.py --start 2026-03-09 \\
        --end 2026-07-03T05:30 --macro                                   # June gap + z-score history

    python3 scripts/backfill/reconstruct_signals.py --interval 15m --replace \\
        --start <25 trading days back> --end <fix deploy time>           # market-history fix rollout

Plain inserts are only for ranges whose z-score history is rebuilt too (e.g.
June). Before the 2026-10 market-history fix, live get_close_history /
get_volume_history read same-day intraday partial rows, so pre-fix live values
differ systematically from rebuilt ones. --replace (with --interval 15m, the
market_job cadence) archives and swaps the history-dependent derived rows
(rsi_14, return_1d/5d/20d, volume_ratio, sector_etf_return_20d) in a window
for corrected values, so z-scores are consistent once the fix is live.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import gzip
import json
import logging
import statistics
import sys
from datetime import date, datetime, timedelta, timezone
from datetime import time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

_project_root = str(Path(__file__).resolve().parent.parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

load_dotenv(override=True)

from pipeline.sources.macro import SECTOR_ETFS, _compute_etf_return_20d  # noqa: E402
from pipeline.sources.market import (  # noqa: E402
    _compute_order_flow,
    _compute_returns,
    _compute_rsi,
    _compute_volume_ratio,
    to_yahoo_symbol,
)
from scripts.db.connection import close_pool, get_pool, init_pool  # noqa: E402
from scripts.db.queries.raw_signals import insert_signals, replace_signals  # noqa: E402
from scripts.db.queries.universe import get_universe_as_of  # noqa: E402

_log = logging.getLogger("reconstruct_signals")

ET = ZoneInfo("America/New_York")
SESSION_CLOSE_ET = dtime(16, 0)
INTRADAY_HOURS_UTC = range(14, 21)          # market_job / macro_intraday_job: 14:00…20:xx
# Derived types whose live values depended on the contaminated history readers
# (get_close_history / get_volume_history) — what --replace rewrites.
REPLACE_MARKET_TYPES = ["rsi_14", "return_1d", "return_5d", "return_20d", "volume_ratio"]
REPLACE_MACRO_TYPES = ["sector_etf_return_20d"]
EOD_MARK_UTC = dtime(21, 15)                # market_eod_job
RSI_HISTORY = 50                            # live: get_close_history(limit=50)
VOLUME_HISTORY = 20                         # live: get_volume_history(limit=20)
ETF_HISTORY = 22                            # live: get_close_history(etf, limit=22)
DAILY_PAD = timedelta(days=120)             # daily bars before --start for the histories
INSERT_CHUNK = 20_000


# ---------------------------------------------------------------------------
# Pure helpers (unit-tested in tests/test_reconstruct_signals.py)
# ---------------------------------------------------------------------------

def hourly_bar_end(bar_start: datetime, minutes: int = 60) -> datetime:
    """End of a yfinance intraday bar (labelled by start); the last bar ends at the close."""
    start_et = bar_start.astimezone(ET)
    close = datetime.combine(start_et.date(), SESSION_CLOSE_ET, ET)
    return min(bar_start + timedelta(minutes=minutes), close.astimezone(timezone.utc))


def partial_bar(bars: list[dict], mark: datetime, minutes: int = 60) -> dict | None:
    """
    Aggregate one day's intraday bars (dicts with start/open/high/low/close/
    volume, sorted by start; ``minutes`` long) that had finished by ``mark``
    into a partial daily bar.
    """
    done = [b for b in bars if hourly_bar_end(b["start"], minutes) <= mark]
    if not done:
        return None
    return {
        "open":   done[0]["open"],
        "high":   max(b["high"] for b in done),
        "low":    min(b["low"] for b in done),
        "close":  done[-1]["close"],
        "volume": sum(b["volume"] for b in done),
    }


def intraday_marks(day: date, step_minutes: int = 60) -> list[datetime]:
    """Live job marks: hourly 14:00…20:00 (macro_intraday_job), or every
    ``step_minutes`` from 14:00 through 20:xx (market_job: */15 → 20:45)."""
    if day.weekday() >= 5:
        return []
    return [datetime(day.year, day.month, day.day, h, m, tzinfo=timezone.utc)
            for h in INTRADAY_HOURS_UTC for m in range(0, 60, step_minutes)]


def eod_mark(day: date) -> datetime:
    return datetime.combine(day, EOD_MARK_UTC, timezone.utc)


def market_rows(ticker: str, mark: datetime, bar: dict,
                prior_closes: list[tuple[datetime, float]],
                prior_volumes: list[float]) -> list[tuple]:
    """The derived market signals live _run_market computes for one bar."""
    vals: list[tuple[str, float]] = list(_compute_order_flow(bar))
    rsi = _compute_rsi([c for _, c in prior_closes[-RSI_HISTORY:]] + [bar["close"]])
    if rsi is not None:
        vals.append(("rsi_14", rsi))
    vals += _compute_returns(bar["close"], prior_closes[-RSI_HISTORY:])
    vr = _compute_volume_ratio(bar["volume"], prior_volumes[-VOLUME_HISTORY:])
    if vr is not None:
        vals.append(("volume_ratio", vr))
    return [(ticker, sig, float(v), "computed", "manual_backfill", mark) for sig, v in vals]


# ---------------------------------------------------------------------------
# yfinance → per-symbol bar lists
# ---------------------------------------------------------------------------

def _download(symbols: list[str], start: datetime, end: datetime, interval: str):
    import yfinance as yf

    return yf.download(
        tickers=symbols, start=start.date().isoformat(),
        end=(end + timedelta(days=1)).date().isoformat(),
        interval=interval, group_by="ticker", auto_adjust=True,
        prepost=False, threads=True, progress=False,
    )


def _frame_rows(raw, symbol: str) -> list[dict]:
    # group_by="ticker" yields (symbol, field) columns even for one symbol.
    df = raw[symbol] if raw.columns.nlevels > 1 else raw
    df = df.dropna(subset=["Close"])
    out = []
    for ts, r in df.iterrows():
        t = ts.to_pydatetime()
        t = t.replace(tzinfo=timezone.utc) if t.tzinfo is None else t.astimezone(timezone.utc)
        out.append({"start": t, "open": float(r["Open"]), "high": float(r["High"]),
                    "low": float(r["Low"]), "close": float(r["Close"]),
                    "volume": float(r.get("Volume", 0) or 0)})
    return out


def _by_symbol(symbols: list[str], start: datetime, end: datetime, interval: str) -> dict[str, list[dict]]:
    raw = _download(symbols, start, end, interval)
    out: dict[str, list[dict]] = {}
    for s in symbols:
        try:
            rows = _frame_rows(raw, s)
        except KeyError:
            rows = []
        if rows:
            out[s] = rows
    return out


def _daily_index(daily: list[dict]) -> list[tuple[date, dict]]:
    # Daily bars are labelled at midnight (exchange-local date) → use the date part.
    return [(b["start"].date(), b) for b in daily]


# ---------------------------------------------------------------------------
# Rebuild
# ---------------------------------------------------------------------------

def build_ticker_rows(ticker: str, hourly: list[dict], daily: list[dict],
                      start: datetime, end: datetime, eod: bool,
                      minutes: int = 60, types: set[str] | None = None) -> list[tuple]:
    """Rebuilt rows for one ticker; ``hourly`` holds ``minutes``-long bars and
    marks follow that cadence. ``types`` keeps only those signal types."""
    days = _daily_index(daily)
    by_day: dict[date, list[dict]] = {}
    for b in hourly:
        by_day.setdefault(b["start"].astimezone(ET).date(), []).append(b)

    rows: list[tuple] = []
    for i, (d, dbar) in enumerate(days):
        prior = days[:i]
        prior_closes = [(datetime.combine(pd, dtime(), timezone.utc), pb["close"]) for pd, pb in prior]
        prior_volumes = [pb["volume"] for _, pb in prior]
        for mark in intraday_marks(d, minutes):
            if not (start <= mark < end):
                continue
            bar = partial_bar(by_day.get(d, []), mark, minutes)
            if bar is not None:
                rows += market_rows(ticker, mark, bar, prior_closes, prior_volumes)
        if eod and start <= eod_mark(d) < end:
            rows += market_rows(ticker, eod_mark(d), dbar, prior_closes, prior_volumes)
    return [r for r in rows if r[1] in types] if types is not None else rows


def build_macro_rows(vix_hourly: list[dict], etf_hourly: dict[str, list[dict]],
                     etf_daily: dict[str, list[dict]], start: datetime, end: datetime) -> list[tuple]:
    """VIX + sector-ETF returns at the hourly macro_intraday_job marks
    (pass ``vix_hourly=[]`` to rebuild only the ETF returns)."""
    rows: list[tuple] = []
    vix_by_day: dict[date, list[dict]] = {}
    for b in vix_hourly:
        vix_by_day.setdefault(b["start"].astimezone(ET).date(), []).append(b)
    for d, bars in sorted(vix_by_day.items()):
        for mark in intraday_marks(d):
            if start <= mark < end and (bar := partial_bar(bars, mark)) is not None:
                rows.append(("_MACRO_", "vix", bar["close"], "yfinance", "manual_backfill", mark))

    for etf, hourly in etf_hourly.items():
        days = _daily_index(etf_daily.get(etf, []))
        by_day: dict[date, list[dict]] = {}
        for b in hourly:
            by_day.setdefault(b["start"].astimezone(ET).date(), []).append(b)
        for i, (d, _) in enumerate(days):
            prior = [(datetime.combine(pd, dtime(), timezone.utc), pb["close"]) for pd, pb in days[:i]]
            for mark in intraday_marks(d):
                if not (start <= mark < end):
                    continue
                bar = partial_bar(by_day.get(d, []), mark)
                if bar is None:
                    continue
                ret = _compute_etf_return_20d(bar["close"], prior[-ETF_HISTORY:])
                if ret is not None:
                    rows.append((etf, "sector_etf_return_20d", ret, "computed", "manual_backfill", mark))
    return rows


async def _insert(rows: list[tuple]) -> None:
    for i in range(0, len(rows), INSERT_CHUNK):
        await insert_signals(rows[i:i + INSERT_CHUNK])


async def _parity() -> None:
    """Rebuild live-covered marks for AAPL and compare with the live rows."""
    pool = await get_pool()
    checks = [
        ("EOD 2026-09-22", datetime(2026, 9, 22, tzinfo=timezone.utc),
         datetime(2026, 9, 23, tzinfo=timezone.utc), True),
        ("intraday 2026-09-15", datetime(2026, 9, 15, 14, tzinfo=timezone.utc),
         datetime(2026, 9, 15, 21, tzinfo=timezone.utc), False),
    ]
    sym = to_yahoo_symbol("AAPL")
    hourly = _by_symbol([sym], datetime(2026, 9, 1, tzinfo=timezone.utc),
                        datetime(2026, 9, 24, tzinfo=timezone.utc), "1h").get(sym, [])
    daily = _by_symbol([sym], datetime(2026, 9, 1, tzinfo=timezone.utc) - DAILY_PAD,
                       datetime(2026, 9, 24, tzinfo=timezone.utc), "1d").get(sym, [])
    for label, s, e, eod in checks:
        rebuilt = build_ticker_rows("AAPL", hourly, daily, s, e, eod=eod)
        if eod:
            rebuilt = [r for r in rebuilt if r[5].time() == EOD_MARK_UTC]
        diffs: dict[str, list[float]] = {}
        for _, sig, val, _, _, mark in rebuilt:
            live = await pool.fetchval(
                "SELECT value FROM raw_signals WHERE ticker='AAPL' AND signal_type=$1 "
                "AND upload_type='live' AND timestamp BETWEEN $2 AND $3 "
                "ORDER BY abs(extract(epoch FROM timestamp - $4)) LIMIT 1",
                sig, mark - timedelta(minutes=10), mark + timedelta(minutes=10), mark)
            if live is not None:
                diffs.setdefault(sig, []).append(abs(val - float(live)))
                if eod:
                    print(f"  {label} {sig:22} rebuilt={val:<12.6g} live={float(live):<12.6g}")
        print(f"{label}: median |rebuilt − live| per signal:")
        for sig, d in sorted(diffs.items()):
            print(f"    {sig:22} n={len(d):2d}  median={statistics.median(d):.6g}")


def _archive_path(start: datetime, end: datetime) -> Path:
    return (Path(_project_root) / "exports"
            / f"derived_signals_pre_fix_{start:%Y%m%d}_{end:%Y%m%d}.csv.gz")


async def archive_replaced(start: datetime, end: datetime, tickers: list[str]) -> int:
    """
    COPY every row --replace will delete (REPLACE_MARKET_TYPES for ``tickers``
    + REPLACE_MACRO_TYPES for the sector ETFs, in [start, end)) to a gzip CSV,
    verify the count, and write a sidecar. Skipped if a verified archive for
    the same window exists (re-runs only delete already-rebuilt rows).
    """
    path = _archive_path(start, end)
    meta = path.with_suffix("").with_suffix(".meta.json")
    if path.exists() and meta.exists():
        return json.loads(meta.read_text())["rows"]
    etfs = sorted(set(SECTOR_ETFS.values()))
    where = ("((ticker = ANY($1::text[]) AND signal_type = ANY($2::text[])) "
             " OR (ticker = ANY($3::text[]) AND signal_type = ANY($4::text[]))) "
             "AND timestamp >= $5 AND timestamp < $6")
    params = (tickers, REPLACE_MARKET_TYPES, etfs, REPLACE_MACRO_TYPES, start, end)
    pool = await get_pool()
    async with pool.acquire() as conn:
        expected = await conn.fetchval(f"SELECT COUNT(*) FROM raw_signals WHERE {where}", *params)
        tmp = path.with_suffix(".tmp")
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(tmp, "wb") as fh:
            await conn.copy_from_query(f"SELECT * FROM raw_signals WHERE {where}", *params,
                                       output=fh, format="csv", header=True)
    with gzip.open(tmp, "rt", newline="") as fh:
        written = sum(1 for _ in csv.reader(fh)) - 1
    if written != expected:
        raise RuntimeError(f"archive row count {written} != expected {expected}; aborting")
    tmp.rename(path)
    meta.write_text(json.dumps({"rows": expected, "start": start.isoformat(), "end": end.isoformat()}))
    return expected


def daily_bar_rows(ticker: str, daily: list[dict], start: datetime, end: datetime) -> list[tuple]:
    """yf_open/high/low/close/volume rows for daily bars in [start, end),
    timestamped at the bar date (midnight UTC) exactly like the live market
    job's batch download — so get_close_history / get_volume_history see
    them as prior sessions."""
    rows: list[tuple] = []
    for b in daily:
        ts = datetime.combine(b["start"].date(), dtime(), timezone.utc)
        if not (start <= ts < end):
            continue
        for field in ("open", "high", "low", "close", "volume"):
            rows.append((ticker, f"yf_{field}", float(b[field]), "yfinance", "manual_backfill", ts))
    return rows


def _parse_ts(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


async def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--start", type=_parse_ts)
    p.add_argument("--end", type=_parse_ts, help="exclusive")
    p.add_argument("--no-eod", action="store_true", help="skip 21:15 EOD marks (live EOD rows exist)")
    p.add_argument("--macro", action="store_true", help="also rebuild VIX + sector-ETF returns")
    p.add_argument("--universe-as-of", type=_parse_ts, default=None,
                   help="tickers that existed at this time (default: --start); "
                        "retired symbols trading then are included, later additions excluded")
    p.add_argument("--tickers", help="comma-separated subset (default: universe as of --universe-as-of)")
    p.add_argument("--dry-run", action="store_true", help="build + count, no writes")
    p.add_argument("--parity", action="store_true", help="compare rebuilt vs live AAPL rows and exit")
    p.add_argument("--interval", choices=["1h", "15m"], default="1h",
                   help="bar size = mark cadence (15m mirrors market_job; yfinance keeps ~60 days)")
    p.add_argument("--replace", action="store_true",
                   help="archive, then atomically replace the history-dependent derived rows "
                        "(REPLACE_MARKET_TYPES + sector-ETF returns) in the window")
    p.add_argument("--daily-bars", action="store_true",
                   help="only backfill daily yf_* OHLCV bars in [start, end) (warm-start for newly added tickers)")
    args = p.parse_args(argv)
    minutes = 15 if args.interval == "15m" else 60

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s")
    await init_pool(command_timeout=300)
    try:
        if args.parity:
            await _parity()
            return 0
        if not (args.start and args.end):
            p.error("--start and --end are required")

        tickers = (args.tickers.upper().split(",") if args.tickers
                   else await get_universe_as_of(args.universe_as_of or args.start))
        sym_of = {t: to_yahoo_symbol(t) for t in tickers}
        symbols = list(sym_of.values())

        if args.daily_bars:
            daily = await asyncio.to_thread(_by_symbol, symbols, args.start, args.end, "1d")
            rows = [r for t in tickers if sym_of[t] in daily
                    for r in daily_bar_rows(t, daily[sym_of[t]], args.start, args.end)]
            if not args.dry_run:
                await _insert(rows)
            missing = [t for t in tickers if sym_of[t] not in daily]
            _log.info("daily bars: %d rows %s for %d tickers (no yfinance data: %s)", len(rows),
                      "built" if args.dry_run else "inserted", len(tickers) - len(missing), ",".join(missing))
            return 0

        if args.replace and not args.dry_run:
            n = await archive_replaced(args.start, args.end, tickers)
            _log.info("archived %d rows → %s", n, _archive_path(args.start, args.end))

        _log.info("downloading %s + 1d bars for %d tickers …", args.interval, len(symbols))
        hourly = await asyncio.to_thread(_by_symbol, symbols, args.start, args.end, args.interval)
        daily = await asyncio.to_thread(_by_symbol, symbols, args.start - DAILY_PAD, args.end, "1d")

        total = deleted = 0
        missing = []
        for t in tickers:
            s = sym_of[t]
            if s not in hourly or s not in daily:
                missing.append(t)
                continue
            rows = build_ticker_rows(
                t, hourly[s], daily[s], args.start, args.end, eod=not args.no_eod, minutes=minutes,
                types=set(REPLACE_MARKET_TYPES) if args.replace else None,
            )
            total += len(rows)
            if args.dry_run:
                continue
            if args.replace:
                deleted += await replace_signals(t, REPLACE_MARKET_TYPES, args.start, args.end, rows)
            else:
                await _insert(rows)
        _log.info("market: %d rows %s for %d tickers (%d without yfinance data: %s)%s",
                  total, "built" if args.dry_run else "inserted", len(tickers) - len(missing),
                  len(missing), ",".join(missing[:30]),
                  f"; {deleted} old rows replaced" if args.replace else "")

        if args.macro or args.replace:
            # Macro marks are hourly (macro_intraday_job) regardless of --interval.
            # --replace rebuilds only the ETF returns (VIX never read history).
            etfs = sorted(set(SECTOR_ETFS.values()))
            vix = ([] if args.replace else
                   (await asyncio.to_thread(_by_symbol, ["^VIX"], args.start, args.end, "1h")).get("^VIX", []))
            etf_h = await asyncio.to_thread(_by_symbol, etfs, args.start, args.end, "1h")
            etf_d = await asyncio.to_thread(_by_symbol, etfs, args.start - DAILY_PAD, args.end, "1d")
            mrows = build_macro_rows(vix, etf_h, etf_d, args.start, args.end)
            if args.dry_run:
                pass
            elif args.replace:
                for etf in etfs:
                    await replace_signals(etf, REPLACE_MACRO_TYPES, args.start, args.end,
                                          [r for r in mrows if r[0] == etf])
            else:
                await _insert(mrows)
            _log.info("macro: %d rows %s", len(mrows), "built" if args.dry_run else
                      ("replaced" if args.replace else "inserted"))
        return 0
    finally:
        await close_pool()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
