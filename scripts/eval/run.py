"""Release-gate CLI for the eval harness.

    python3 -m scripts.eval.run --start 2026-04-24 --end 2026-07-20 \
        --out exports/eval [--baseline scripts/eval/baselines/BASELINE_2026-07-21.json] \
        [--intraday] [--gap 2026-06-23:2026-07-03]

Reads sentiment_history + raw_signals closes from the DB, runs the daily
analysis (and optionally intraday), writes scorecard.json / scorecard.md /
CSVs to --out, and — when --baseline is given — exits non-zero if any
scorecard metric regressed beyond tolerance (see scorecard.DEFAULT_RULES).
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# stale DATABASE_URL may be exported in the shell — .env must win in scripts
load_dotenv(override=True)

from scripts.db.connection import close_pool  # noqa: E402
from scripts.eval import analyze, data, intraday, scorecard


def _parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start", required=True, help="window start, YYYY-MM-DD (UTC)")
    p.add_argument("--end", required=True, help="window end (exclusive), YYYY-MM-DD (UTC)")
    p.add_argument("--out", default="exports/eval", help="output directory")
    p.add_argument("--baseline", help="baseline scorecard JSON to gate against")
    p.add_argument("--intraday", action="store_true",
                   help="also run the intraday lead-lag (heavier query)")
    p.add_argument("--prices", choices=["snapshots", "yfinance"], default="snapshots",
                   help="intraday price source (default: price_snapshots)")
    p.add_argument("--gap", action="append", default=None, metavar="START:END",
                   help="data-gap window to excise (repeatable); "
                        "default: 2026-06-23:2026-07-03")
    return p.parse_args(argv)


def _parse_gaps(raw: list[str] | None) -> analyze.GapList:
    if not raw:
        return analyze.DEFAULT_GAPS
    gaps = []
    for item in raw:
        a, b = item.split(":")
        gaps.append((pd.Timestamp(a), pd.Timestamp(b)))
    return gaps


def _utc(day: str) -> datetime:
    return datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)


async def _run(args) -> int:
    gaps = _parse_gaps(args.gap)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    start, end = _utc(args.start), _utc(args.end)

    print(f"loading sentiment (daily) + closes for {args.start}..{args.end} ...")
    sent = await data.load_sentiment_panel(start, end, granularity="daily")
    closes = await data.load_close_panel(start, end)
    latency = await data.load_article_latency(start, end)
    if sent.empty or closes.empty:
        print("no data in window — aborting", file=sys.stderr)
        return 2

    s = analyze.prepare_daily(sent)
    print(f"sentiment: {s['ticker'].nunique()} tickers, {len(s)} ticker-days; "
          f"prices: {closes.shape[1]} tickers, {closes.shape[0]} trading days")

    panel = analyze.build_panel(s, closes, gaps=gaps)
    panel.to_csv(out / "panel.csv", index=False)

    ic = analyze.ic_table(panel)
    ic.to_csv(out / "ic_table.csv", index=False)

    ls_rows = []
    for feat in ["score_raw", "xs_pct", "exo", "exo_pct",
                 "dscore_raw_1", "dscore_raw_3", "dexo_1", "dexo_3",
                 "narrative", "influencer", "macro"]:
        for h in [1, 3, 5]:
            r = analyze.quintile_ls(panel, feat, h, neutral=True)
            if r:
                ls_rows.append(r)
    pd.DataFrame(ls_rows).to_csv(out / "quintile_ls.csv", index=False)

    ll_raw = analyze.lead_lag(s, closes, "dscore_raw_1", gaps=gaps)
    ll_raw.to_csv(out / "leadlag_raw.csv", index=False)
    ll_exo = analyze.lead_lag(s, closes, "dexo_1", gaps=gaps)
    ll_exo.to_csv(out / "leadlag_exo.csv", index=False)

    card = scorecard.build_scorecard(
        s, ic, ll_raw, ll_exo,
        meta={"start": args.start, "end": args.end,
              "gaps": [f"{g0.date()}:{g1.date()}" for g0, g1 in gaps],
              "generated_at": datetime.now(timezone.utc).isoformat()},
        latency=latency,
    )

    if args.intraday:
        print("loading raw sentiment ticks + intraday prices ...")
        raw = await data.load_sentiment_panel(start, end, granularity="raw")
        st = intraday.prepare_raw_sentiment(raw)
        if args.prices == "yfinance":
            prices = intraday.fetch_yf_intraday(sorted(st["ticker"].unique()))
        else:
            snaps = await data.load_price_snapshots(start, end)
            prices = intraday.snapshot_bars(snaps)
        if prices.empty:
            print("no intraday prices — skipping intraday section", file=sys.stderr)
        else:
            long = intraday.build_long(prices, st, gaps=gaps)
            card["leadlag_intraday"] = {}
            for sig in ["dscore_raw", "dnarrative", "dmarket"]:
                ll = intraday.leadlag(long, sig)
                ll.to_csv(out / f"intraday_leadlag_{sig}.csv", index=False)
                card["leadlag_intraday"][sig] = scorecard._leadlag_summary(
                    ll, offset_col="lag_bars")
            ov = intraday.overnight(st, closes, gaps=gaps)
            ov.to_csv(out / "overnight.csv", index=False)
            card["overnight"] = {} if ov.empty else {
                r["signal"]: {"corr_nextday": r["corr_nextday"], "n": r["n"]}
                for _, r in ov.iterrows()
            }

    scorecard.save(card, str(out / "scorecard.json"), str(out / "scorecard.md"))
    print(f"\nscorecard written to {out}/scorecard.{{json,md}}")
    print(scorecard.render_markdown(card))

    if args.baseline:
        baseline = scorecard.load(args.baseline)
        regressions = scorecard.diff(baseline, card)
        if regressions:
            print("\n*** GATE FAILED — regressions vs baseline: ***", file=sys.stderr)
            for r in regressions:
                print(f"  - {r}", file=sys.stderr)
            return 1
        print(f"\ngate PASSED vs {args.baseline}")
    return 0


def main(argv=None) -> int:
    args = _parse_args(argv)
    try:
        return asyncio.run(_main_async(args))
    except KeyboardInterrupt:
        return 130


async def _main_async(args) -> int:
    try:
        return await _run(args)
    finally:
        await close_pool()


if __name__ == "__main__":
    sys.exit(main())
