"""What-if: re-smooth the historical raw composite under candidate EMA
half-lives and measure how much lag each one adds.

    python3 -m scripts.eval.experiments.ema_halflife \
        --start 2026-04-24 --end 2026-07-21 [--halflives 1,2,4]

For each candidate T½ the raw score series (sentiment_history.composite_score,
every tick) is re-smoothed offline with the production formula
(α = 1 − 0.5^(dt/T½)), then evaluated with the daily lead-lag diagnostic and
IC of the 3-day smoothed-score change. The gate for actually changing the
production default (EMA_HALF_LIFE_HOURS): a shorter half-life must move the
lead-lag peak no further into the past and not worsen forward IC — see the
nowcasting plan, Phase 4. This script only reports; it changes nothing.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv

load_dotenv(override=True)

from scripts.db.connection import close_pool  # noqa: E402
from scripts.eval import analyze, data  # noqa: E402


def resmooth(raw_ticks: pd.DataFrame, half_life_hours: float) -> pd.DataFrame:
    """Apply the production EMA formula per ticker over raw tick scores.

    raw_ticks columns: ticker, timestamp (tz-aware), composite_score (raw).
    Returns the frame with a `smoothed` column added.
    """
    out = []
    for ticker, g in raw_ticks.groupby("ticker", sort=False):
        g = g.sort_values("timestamp").copy()
        dt_h = g["timestamp"].diff().dt.total_seconds().fillna(0.0) / 3600.0
        alpha = 1.0 - 0.5 ** (dt_h / half_life_hours)
        smoothed = []
        prev = None
        for raw, a in zip(g["composite_score"], alpha):
            prev = raw if prev is None else a * raw + (1 - a) * prev
            smoothed.append(prev)
        g["smoothed"] = smoothed
        out.append(g)
    return pd.concat(out, ignore_index=True)


async def _run(args) -> int:
    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    halflives = [float(h) for h in args.halflives.split(",")]

    print(f"loading raw ticks + closes for {args.start}..{args.end} ...")
    raw = await data.load_sentiment_panel(start, end, granularity="raw")
    closes = await data.load_close_panel(start, end)
    if raw.empty or closes.empty:
        print("no data in window", file=sys.stderr)
        return 2
    raw["timestamp"] = pd.to_datetime(raw["timestamp"], utc=True)

    rows = []
    for hl in halflives:
        sm = resmooth(raw[["ticker", "timestamp", "composite_score"]], hl)
        # feed the re-smoothed series through the daily engine as the
        # "smoothed" score; layer columns aren't needed for these metrics
        daily_in = sm.rename(columns={"smoothed": "composite_score_smoothed"})
        for col in ("market_index", "narrative_index", "influencer_index",
                    "macro_index", "confidence_score"):
            daily_in[col] = None
        s = analyze.prepare_daily(daily_in)

        ll = analyze.lead_lag(s, closes, "dscore_1", max_k=3)
        peak = ll.loc[ll["corr"].abs().idxmax()] if not ll.empty else None

        panel = analyze.build_panel(s, closes)
        ic = analyze.ic_table(panel, feats=["score", "dscore_raw_3"], horizons=[1, 3])

        rows.append({
            "half_life_h": hl,
            "leadlag_peak_offset": int(peak["lag_k"]) if peak is not None else None,
            "leadlag_peak_corr": float(peak["corr"]) if peak is not None else None,
            "ic_rows": len(ic),
            "best_abs_ic_t": float(ic["IC_t"].abs().max()) if not ic.empty else None,
        })
        print(f"T½={hl}h → peak offset {rows[-1]['leadlag_peak_offset']} "
              f"(corr {rows[-1]['leadlag_peak_corr']})")

    print("\n" + pd.DataFrame(rows).to_string(index=False))
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--halflives", default="1,2,4")
    args = p.parse_args(argv)

    async def _wrapped():
        try:
            return await _run(args)
        finally:
            await close_pool()

    return asyncio.run(_wrapped())


if __name__ == "__main__":
    sys.exit(main())
