"""
scripts/tools/update_universe.py

Bring ticker_universe in line with reality (2026-10-03 universe update,
migration 015):

  1. RETIRE symbols that stopped trading (acquired / taken private / merged /
     ticker change / symbol reused) — sets delisted_at (last trading day,
     21:00 UTC), successor_ticker, delisted_reason. Source:
     scripts/tools/data/universe_changes_2026-10.csv (with citations).
  2. FLAG current S&P 500 membership (in_sp500, sp500_added) from
     scripts/tools/data/sp500_constituents_2026-10-03.csv, and re-sync current
     members' GICS sector (drives sector-ETF macro routing; the 2023 GICS
     reshuffle moved payments → Financials/Industrials, packaging →
     Materials). Non-members that still trade are KEPT (website + research).
  3. ADD missing current S&P 500 members, plus successors that aren't members
     (e.g. FISV), as tier1_supported with company name + GICS sector.
  4. CLEAN UP data that isn't what it claims to be (archived first):
       - rows for PARA from 2026-08-07, when the symbol was reused by an
         unrelated company (Banzai International)
       - market-derived rows (rsi_14 …) written after a symbol's last trading
         day (computed from frozen history)
       - post-delisting price rows whose value never changes (frozen feed)
  5. Delete retired tickers' Redis `sentiment:{ticker}` keys.

Idempotent. Usage:
    python3 scripts/tools/update_universe.py --dry-run     # print the full diff
    python3 scripts/tools/update_universe.py               # apply
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import gzip
import json
import logging
import sys
from datetime import date, datetime, time, timezone
from pathlib import Path

from dotenv import load_dotenv

_project_root = str(Path(__file__).resolve().parent.parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
load_dotenv(override=True)

from pipeline.sources.macro import SECTOR_ETFS  # noqa: E402
from scripts.db.connection import close_pool, get_pool, init_pool  # noqa: E402

_log = logging.getLogger("update_universe")

DATA = Path(_project_root) / "scripts" / "tools" / "data"
CONSTITUENTS_CSV = DATA / "sp500_constituents_2026-10-03.csv"
CHANGES_CSV = DATA / "universe_changes_2026-10.csv"
ARCHIVE = Path(_project_root) / "exports" / "universe_cleanup_20261003.json.gz"

# The PARA symbol was re-issued to Banzai International; our first PARA price
# row for that security is 2026-08-07.
REUSED_SYMBOLS = {"PARA": datetime(2026, 8, 7, tzinfo=timezone.utc)}
DERIVED_MARKET_TYPES = [
    "rsi_14", "return_1d", "return_5d", "return_20d", "volume_ratio",
    "order_flow_imbalance", "buy_pressure", "sell_pressure", "bid_ask_spread_bps",
]
PRICE_TYPES = ["yf_open", "yf_high", "yf_low", "yf_close", "yf_volume",
               "ohlcv_open", "ohlcv_high", "ohlcv_low", "ohlcv_close",
               "ohlcv_adjusted_close", "ohlcv_volume"]


# ---------------------------------------------------------------------------
# Pure planning (unit-tested)
# ---------------------------------------------------------------------------

def load_constituents(path: Path = CONSTITUENTS_CSV) -> dict[str, dict]:
    with open(path, newline="") as fh:
        return {r["symbol"]: r for r in csv.DictReader(fh)}


def load_changes(path: Path = CHANGES_CSV) -> dict[str, dict]:
    with open(path, newline="") as fh:
        return {r["ticker"]: r for r in csv.DictReader(fh)}


def last_trade_ts(day: str) -> datetime:
    """Retirement timestamp: the close (21:00 UTC) of the last trading day, so
    an as-of lookup during that day still treats the symbol as live."""
    return datetime.combine(date.fromisoformat(day), time(21, 0), timezone.utc)


def plan_update(universe: dict[str, dict], constituents: dict[str, dict],
                changes: dict[str, dict]) -> dict:
    """
    Diff the DB universe ({ticker: {company_name, sector, delisted_at,
    in_sp500, sp500_added}}) against the snapshots. Returns
    {"retire": [...], "flag": [...], "resector": [...], "add": [...]};
    raises if any added/re-sectored ticker's sector has no sector ETF.
    """
    retire, flag, resector, add = [], [], [], []
    for t, ch in sorted(changes.items()):
        if t not in universe:
            continue
        when = last_trade_ts(ch["last_trading_day"])
        if universe[t].get("delisted_at") != when:
            retire.append({"ticker": t, "delisted_at": when,
                           "successor_ticker": ch["successor_ticker"] or None,
                           "delisted_reason": ch["reason"]})

    for t, row in sorted(universe.items()):
        member = constituents.get(t)
        want = (member is not None,
                date.fromisoformat(member["date_added"]) if member and member["date_added"] else None)
        if (bool(row.get("in_sp500")), row.get("sp500_added")) != want:
            flag.append({"ticker": t, "in_sp500": want[0], "sp500_added": want[1]})
        if member and t not in changes and row.get("sector") != member["gics_sector"]:
            resector.append({"ticker": t, "from": row.get("sector"), "sector": member["gics_sector"]})

    for t, m in sorted(constituents.items()):
        if t not in universe and t not in changes:
            add.append({"ticker": t, "company_name": m["security"], "sector": m["gics_sector"],
                        "in_sp500": True,
                        "sp500_added": date.fromisoformat(m["date_added"]) if m["date_added"] else None})
    # Successors that trade but aren't index members (e.g. FI → FISV): carry
    # the predecessor's name/sector.
    added = {a["ticker"] for a in add}
    for t, ch in sorted(changes.items()):
        succ = ch["successor_ticker"]
        if succ and succ not in universe and succ not in added and succ not in constituents:
            prev = universe.get(t, {})
            add.append({"ticker": succ, "company_name": prev.get("company_name"),
                        "sector": prev.get("sector"), "in_sp500": False, "sp500_added": None})
            added.add(succ)

    bad = [a["ticker"] for a in add + resector if a["sector"] not in SECTOR_ETFS]
    if bad:
        raise ValueError(f"sector without a sector ETF for: {bad}")
    return {"retire": retire, "flag": flag, "resector": resector, "add": add}


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

async def _load_universe(conn) -> dict[str, dict]:
    rows = await conn.fetch(
        "SELECT ticker, company_name, sector, delisted_at, in_sp500, sp500_added "
        "FROM ticker_universe WHERE tier = 'tier1_supported'")
    return {r["ticker"]: dict(r) for r in rows}


def _cleanup_queries(retired: dict[str, datetime]) -> list[tuple[str, str, tuple]]:
    """(table, WHERE clause, params) for every row the cleanup removes."""
    q: list[tuple[str, str, tuple]] = []
    for t, since in REUSED_SYMBOLS.items():
        q.append(("raw_signals", "ticker = $1 AND timestamp >= $2", (t, since)))
        q.append(("sentiment_history", "ticker = $1 AND timestamp >= $2", (t, since)))
        q.append(("raw_articles", "ticker = $1 AND published_at >= $2", (t, since)))
    for t, when in retired.items():
        cutoff = REUSED_SYMBOLS.get(t, None)
        upper = "AND timestamp < $4" if cutoff else ""
        params = (t, DERIVED_MARKET_TYPES, when) + ((cutoff,) if cutoff else ())
        q.append(("raw_signals",
                  f"ticker = $1 AND signal_type = ANY($2::text[]) AND timestamp > $3 {upper}", params))
    return q


async def _frozen_price_tickers(conn, retired: dict[str, datetime]) -> list[tuple[str, datetime]]:
    """Retired tickers whose post-delisting price rows hold a single repeated value."""
    out = []
    for t, when in retired.items():
        n_distinct = await conn.fetchval(
            "SELECT COUNT(DISTINCT round(value::numeric, 4)) FROM raw_signals "
            "WHERE ticker = $1 AND signal_type IN ('yf_close', 'ohlcv_close') AND timestamp > $2",
            t, when)
        if n_distinct == 1:
            out.append((t, when))
    return out


async def run(dry_run: bool) -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        universe = await _load_universe(conn)
        plan = plan_update(universe, load_constituents(), load_changes())

        changes = load_changes()
        retired = {t: last_trade_ts(ch["last_trading_day"]) for t, ch in changes.items() if t in universe}
        cleanup = _cleanup_queries(retired)
        frozen = await _frozen_price_tickers(conn, retired)
        for t, when in frozen:
            if t not in REUSED_SYMBOLS:
                cleanup.append(("raw_signals", "ticker = $1 AND signal_type = ANY($2::text[]) AND timestamp > $3",
                                (t, PRICE_TYPES, when)))
        counts = []
        for table, where, params in cleanup:
            n = await conn.fetchval(f"SELECT COUNT(*) FROM {table} WHERE {where}", *params)
            if n:
                counts.append((table, where, params, n))

        summary = {
            "retire": len(plan["retire"]), "flag": len(plan["flag"]),
            "resector": len(plan["resector"]), "add": len(plan["add"]),
            "active_after": len([t for t, r in universe.items()
                                 if r["delisted_at"] is None and t not in retired]) + len(plan["add"]),
            "in_sp500_after": sum(1 for t in universe if t in load_constituents()) + sum(a["in_sp500"] for a in plan["add"]),
            "cleanup_rows": {f"{tb}:{p[0]}": n for tb, _, p, n in counts},
            "frozen_price_tickers": [t for t, _ in frozen],
        }
        print(json.dumps(summary, indent=2, default=str))
        print("RETIRE:", ", ".join(f"{r['ticker']}→{r['successor_ticker'] or '∅'}" for r in plan["retire"]))
        print("RESECTOR:", ", ".join(f"{r['ticker']} {r['from']}→{r['sector']}" for r in plan["resector"]))
        print("ADD:", ", ".join(a["ticker"] + ("" if a["in_sp500"] else "(non-S&P)") for a in plan["add"]))
        if dry_run:
            return summary

        # Archive everything the cleanup deletes, then apply in one transaction.
        ARCHIVE.parent.mkdir(parents=True, exist_ok=True)
        archived = {}
        for table, where, params, _n in counts:
            rows = await conn.fetch(f"SELECT * FROM {table} WHERE {where}", *params)
            archived.setdefault(table, []).extend(dict(r) for r in rows)
        with gzip.open(ARCHIVE, "wt") as fh:
            json.dump(archived, fh, default=str)
        with gzip.open(ARCHIVE, "rt") as fh:
            check = json.load(fh)
        if sum(len(v) for v in check.values()) != sum(n for *_, n in counts):
            raise RuntimeError("archive row count mismatch; nothing changed")

        async with conn.transaction():
            for r in plan["retire"]:
                await conn.execute(
                    "UPDATE ticker_universe SET delisted_at = $2, successor_ticker = $3, "
                    "delisted_reason = $4 WHERE ticker = $1",
                    r["ticker"], r["delisted_at"], r["successor_ticker"], r["delisted_reason"])
            for f in plan["flag"]:
                await conn.execute(
                    "UPDATE ticker_universe SET in_sp500 = $2, sp500_added = $3 WHERE ticker = $1",
                    f["ticker"], f["in_sp500"], f["sp500_added"])
            for r in plan["resector"]:
                await conn.execute("UPDATE ticker_universe SET sector = $2 WHERE ticker = $1",
                                   r["ticker"], r["sector"])
            for a in plan["add"]:
                await conn.execute(
                    "INSERT INTO ticker_universe (ticker, tier, company_name, sector, in_sp500, sp500_added) "
                    "VALUES ($1, 'tier1_supported', $2, $3, $4, $5) ON CONFLICT (ticker) DO NOTHING",
                    a["ticker"], a["company_name"], a["sector"], a["in_sp500"], a["sp500_added"])
            for table, where, params, _ in counts:
                await conn.execute(f"DELETE FROM {table} WHERE {where}", *params)

    from scripts.db.redis import close_redis, get_redis, init_redis
    await init_redis()
    try:
        n = await get_redis().delete(*[f"sentiment:{t}" for t in retired]) if retired else 0
        summary["redis_keys_deleted"] = n
    finally:
        await close_redis()
    _log.info("applied: %s", summary)
    return summary


async def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s")
    await init_pool(command_timeout=300)
    try:
        await run(args.dry_run)
        return 0
    finally:
        await close_pool()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
