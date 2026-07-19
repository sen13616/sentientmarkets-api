#!/usr/bin/env python3
"""
scripts/diagnose_api_health.py

Ad-hoc API/pipeline health diagnostic. Probes the live /health endpoint and
runs the tools/db_health.py queries against the configured DATABASE_URL, then
prints a human-readable report. Read-only.
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timezone

import asyncpg

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.tools import db_health as H  # noqa: E402


def _load_env() -> None:
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    if not os.path.exists(path):
        return
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k, v)


def _age(ts) -> str:
    if ts is None:
        return "never"
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - ts
    mins = delta.total_seconds() / 60
    if mins < 60:
        return f"{mins:.0f}m ago"
    if mins < 60 * 24:
        return f"{mins/60:.1f}h ago"
    return f"{mins/1440:.1f}d ago"


async def main() -> None:
    _load_env()
    dsn = os.environ.get("DATABASE_URL", "").replace("+asyncpg", "")
    if not dsn:
        print("DATABASE_URL not set")
        return

    conn = await asyncpg.connect(dsn, timeout=20)
    try:
        print("=" * 66)
        print("  SENTIENTMARKETS — PIPELINE / DATA-QUALITY DIAGNOSTIC")
        print("  run:", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
        print("=" * 66)

        act = await H.query_scoring_activity_24h(conn)
        print("\n» SCORING ACTIVITY (24h)")
        print(f"  rows written : {act.get('n_rows', 0):,}")
        print(f"  tickers      : {act.get('n_tickers', 0)}")
        print(f"  window       : {_age(act.get('earliest'))}  ->  {_age(act.get('latest'))}")
        print(f"  avg conf     : {act.get('avg_conf')}")

        cov = await H.query_ticker_coverage(conn)
        uni = cov.get("universe_size", 0) or 0
        s24 = cov.get("scored_24h", 0) or 0
        pct = (s24 / uni * 100) if uni else 0
        print("\n» TICKER COVERAGE")
        print(f"  universe     : {uni}")
        print(f"  scored 24h   : {s24}  ({pct:.1f}%)")
        print(f"  scored 7d    : {cov.get('scored_7d', 0)}")

        fresh = await H.query_signal_freshness(conn)
        print("\n» SIGNAL FRESHNESS BY SOURCE (24h)")
        if not fresh:
            print("  (no raw_signals in last 24h)")
        for r in fresh:
            print(f"  {r['source']:<16} {r['n_signals_24h']:>7,} signals   latest {_age(r['latest'])}")

        arts = await H.query_article_volume_24h(conn)
        print("\n» ARTICLE VOLUME BY SOURCE (24h)")
        if not arts:
            print("  (no raw_articles in last 24h)")
        for r in arts:
            print(f"  {r['source']:<16} {r['n']:>6,} articles   {r['n_tickers']} tickers")

        miss = await H.query_missing_layer_breakdown_24h(conn)
        tot = miss.get("total", 0) or 0
        print("\n» MISSING SUB-INDEX LAYERS (24h)")
        if tot:
            for layer in ("market", "narrative", "influencer", "macro"):
                n = miss.get(f"{layer}_null", 0) or 0
                print(f"  {layer:<12} null in {n:>6,}/{tot:,}  ({n/tot*100:.1f}%)")
        else:
            print("  (no scored rows)")

        flags = await H.query_confidence_flag_breakdown_24h(conn)
        print("\n» CONFIDENCE FLAGS (24h)")
        if not flags:
            print("  (none)")
        for r in flags[:12]:
            print(f"  {r['flag']:<34} {r['cnt']:>7,}")

        div = await H.query_divergence_distribution_24h(conn)
        print("\n» DIVERGENCE DISTRIBUTION (24h)")
        for r in div:
            print(f"  {r['divergence']:<12} {r['cnt']:>7,}")

        stale = await H.query_stale_tickers(conn)
        print("\n» STALE / UNSCORED TICKERS (>24h, cap 50)")
        print(f"  count (capped): {len(stale)}")
        for r in stale[:15]:
            print(f"  {r['ticker']:<8} {str(r['company_name'] or '')[:28]:<28} last {_age(r['last_scored'])}")
        if len(stale) > 15:
            print(f"  ... and {len(stale) - 15} more")

        print("\n" + "=" * 66)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
