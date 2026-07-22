"""E003 — evaluate positioning features on the research window.

    python3 -m scripts.eval.experiments.e003_positioning

Runs exactly what §E003 of scripts/eval/EXPERIMENTS.md pre-registers, in the
pre-registered order:

  1. Data-quality preflight (BEFORE any IC): per-feature per-day coverage,
     point-in-time spot-recomputation of stored values, delisted-name NaN
     (not zero-fill) confirmation. Hard-fails on a PIT mismatch.
  2. Primary cells: rf_short_vol_z and rf_insider_net_z_lag2 levels ×
     h {1,2,3,5}, market-neutral, plus the 3-sub-period sign-consistency
     check. insider_net_z_lag2 is an EVAL-TIME shift (analyze.
     add_lagged_feature) — positioning.py and pipeline writes untouched.
  3. Exploratory battery (cannot trigger promotion): unlagged insider,
     drf_*_1 diffs, raw-target ICs.
  4. Sign-aligned quintile L/S at holds {1,3,5}, gross and net of 15 bps.
  5. Daily lead-lag curves for both primary features (mass-location flag).

Research window only (scripts/eval/run.py resolve_window defaults). No
holdout access. Artifacts to exports/eval/E003/.
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv(override=True)

from scripts.db.connection import close_pool, get_pool  # noqa: E402
from scripts.eval import analyze, data  # noqa: E402
from scripts.eval.run import _utc, resolve_window  # noqa: E402

OUT = Path("exports/eval/E003")

PRIMARY_HORIZONS = [1, 2, 3, 5]
HOLDS = [1, 3, 5]
COST_BPS = 15.0

#: (feature, registered IC sign) — the promotion cells
PRIMARY = [("rf_short_vol_z", -1), ("rf_insider_net_z_lag2", +1)]


# ------------------------------------------------------------- preflight --

async def _pit_spot_check(s: pd.DataFrame, n_samples: int = 6) -> list[dict]:
    """Recompute stored short_vol_z values from raw signals with ts <= tick.

    Uses the same pure function as the backfill; a mismatch means the
    evaluation path is reading values the tick could not have known.
    """
    from pipeline.features.positioning import WINDOW, short_vol_z_from_series

    sample = (
        s.dropna(subset=["rf_short_vol_z"])
        .sort_values(["ticker", "date"])
        .groupby("ticker").last().reset_index()
        .head(n_samples)
    )
    results = []
    pool = await get_pool()
    async with pool.acquire() as conn:
        for _, r in sample.iterrows():
            tick_ts = r["ts"]
            rows = await conn.fetch(
                """
                SELECT value FROM raw_signals
                 WHERE ticker = $1 AND signal_type = 'short_volume_ratio_otc'
                   AND timestamp <= $2
                 ORDER BY timestamp
                """,
                r["ticker"], tick_ts,
            )
            series = [float(x["value"]) for x in rows][-(WINDOW + 1):]
            recomputed = short_vol_z_from_series(series)
            results.append({
                "ticker": r["ticker"],
                "tick": str(tick_ts),
                "stored": float(r["rf_short_vol_z"]),
                "recomputed": recomputed,
                "match": recomputed is not None
                and abs(recomputed - float(r["rf_short_vol_z"])) < 1e-6,
            })
    return results


def _coverage(s: pd.DataFrame) -> pd.DataFrame:
    per_day = s.groupby("date").agg(
        tickers=("ticker", "nunique"),
        short_vol_z=("rf_short_vol_z", lambda x: int(x.notna().sum())),
        insider_net_z=("rf_insider_net_z", lambda x: int(x.notna().sum())),
    ).reset_index()
    return per_day


# ------------------------------------------------------------ evaluation --

def _subperiod_ics(panel: pd.DataFrame, feat: str, h: int) -> list[dict]:
    """IC per third of the window's date range (sign-consistency input)."""
    dates = sorted(panel["date"].unique())
    thirds = [dates[i * len(dates) // 3:(i + 1) * len(dates) // 3] for i in range(3)]
    out = []
    for i, chunk in enumerate(thirds, 1):
        sub = panel[panel["date"].isin(chunk)]
        ic = analyze.ic_table(sub, feats=[feat], horizons=[h],
                              min_rows=60, min_days=3)
        cell = ic[(ic.target == "mktneutral")] if not ic.empty else ic
        out.append({
            "feature": feat, "horizon_d": h, "subperiod": i,
            "mean_IC": None if cell.empty else float(cell.iloc[0]["mean_IC"]),
            "n_days": 0 if cell.empty else int(cell.iloc[0]["n_days"]),
        })
    return out


async def _run() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    start, end = (_utc(d) for d in resolve_window("research", None, None))

    print("loading research-window sentiment + closes ...")
    sent = await data.load_sentiment_panel(start, end, granularity="daily")
    closes = await data.load_close_panel(start, end)
    s = analyze.prepare_daily(sent)

    # ── 1. PREFLIGHT (before any IC) ────────────────────────────────────────
    cov = _coverage(s)
    cov.to_csv(OUT / "preflight_coverage.csv", index=False)
    cov_days = {
        "days_total": len(cov),
        "days_with_short_vol_z": int((cov["short_vol_z"] > 0).sum()),
        "days_with_insider_net_z": int((cov["insider_net_z"] > 0).sum()),
        "median_daily_short_vol_z": float(cov.loc[cov.short_vol_z > 0, "short_vol_z"].median()),
        "median_daily_insider_net_z": float(cov.loc[cov.insider_net_z > 0, "insider_net_z"].median()),
    }
    print("coverage:", cov_days)

    pit = await _pit_spot_check(s)
    json.dump(pit, open(OUT / "pit_spotcheck.json", "w"), indent=2, default=str)
    if not all(r["match"] for r in pit):
        print("PIT SPOT-CHECK FAILED — aborting before any IC", file=sys.stderr)
        for r in pit:
            print(" ", r, file=sys.stderr)
        return 1
    print(f"PIT spot-check: {len(pit)}/{len(pit)} stored values recomputed exactly")

    # Delisted / no-data names: NaN, never zero-filled
    per_ticker = s.groupby("ticker")["rf_short_vol_z"].agg(["count"])
    no_data = per_ticker[per_ticker["count"] == 0]
    zero_filled = s[(s["rf_short_vol_z"] == 0.0)]
    excl = {
        "tickers_total": int(s["ticker"].nunique()),
        "tickers_without_short_vol_z": int(len(no_data)),
        "rows_exactly_zero_short_vol_z": int(len(zero_filled)),
    }
    print("exclusion check:", excl)

    # ── 2. Eval-time lag + sign-aligned columns ────────────────────────────
    analyze.add_lagged_feature(s, "rf_insider_net_z", 2)
    s["rf_short_vol_z_neg"] = -s["rf_short_vol_z"]  # prior-aligned for L/S

    panel = analyze.build_panel(s, closes, horizons=PRIMARY_HORIZONS)
    panel.to_csv(OUT / "panel.csv", index=False)

    # ── Primary cells ──────────────────────────────────────────────────────
    prim_feats = [f for f, _ in PRIMARY]
    ic_primary = analyze.ic_table(panel, feats=prim_feats, horizons=PRIMARY_HORIZONS)
    ic_primary.to_csv(OUT / "ic_primary.csv", index=False)

    subperiods: list[dict] = []
    for feat, _sign in PRIMARY:
        for h in PRIMARY_HORIZONS:
            subperiods += _subperiod_ics(panel, feat, h)
    pd.DataFrame(subperiods).to_csv(OUT / "subperiod_ic.csv", index=False)

    # ── 3. Exploratory battery (cannot trigger promotion) ──────────────────
    expl_feats = ["rf_insider_net_z", "drf_short_vol_z_1", "drf_insider_net_z_1"]
    ic_expl = analyze.ic_table(panel, feats=expl_feats, horizons=PRIMARY_HORIZONS)
    ic_expl.to_csv(OUT / "ic_exploratory.csv", index=False)

    # ── 4. Sign-aligned quintile L/S, gross + net ──────────────────────────
    ls_rows = []
    for feat in ["rf_short_vol_z_neg", "rf_insider_net_z_lag2"]:
        for h in HOLDS:
            r = analyze.quintile_ls(panel, feat, h, neutral=True, cost_bps=COST_BPS)
            if r:
                ls_rows.append(r)
    pd.DataFrame(ls_rows).to_csv(OUT / "quintile_ls.csv", index=False)

    # ── 5. Daily lead-lag (mass-location flag) ─────────────────────────────
    leadlag = {}
    for feat, _sign in PRIMARY:
        ll = analyze.lead_lag(s, closes, feat)
        ll.to_csv(OUT / f"leadlag_{feat}.csv", index=False)
        if not ll.empty:
            past = ll[ll.lag_k < 0]["corr"].abs().sum()
            future = ll[ll.lag_k > 0]["corr"].abs().sum()
            leadlag[feat] = {
                "curve": {int(r.lag_k): float(r.corr) for r in ll.itertuples()},
                "abs_mass_past": round(float(past), 4),
                "abs_mass_future": round(float(future), 4),
            }

    summary = {
        "window": [str(start.date()), str(end.date())],
        "coverage": cov_days,
        "exclusion_check": excl,
        "pit_spotcheck_pass": True,
        "ic_primary": ic_primary.to_dict("records"),
        "subperiod_ic": subperiods,
        "ic_exploratory": ic_expl.to_dict("records"),
        "quintile_ls": ls_rows,
        "leadlag": leadlag,
    }
    json.dump(summary, open(OUT / "summary.json", "w"), indent=2, default=str)

    print("\n=== PRIMARY IC CELLS (mktneutral) ===")
    print(ic_primary[ic_primary.target == "mktneutral"].to_string(index=False))
    print("\n=== EXPLORATORY IC (mktneutral) ===")
    if not ic_expl.empty:
        print(ic_expl[ic_expl.target == "mktneutral"].to_string(index=False))
    print("\n=== SUB-PERIOD ICs ===")
    print(pd.DataFrame(subperiods).to_string(index=False))
    print("\n=== SIGN-ALIGNED QUINTILE L/S (gross vs net) ===")
    if ls_rows:
        cols = ["feature", "horizon_d", "mean_LS_per_period", "t",
                "mean_LS_net_per_period", "t_net", "ann_sharpe_net",
                "avg_leg_turnover", "n_days"]
        print(pd.DataFrame(ls_rows)[cols].to_string(index=False))
    print("\n=== LEAD-LAG MASS ===")
    for feat, d in leadlag.items():
        print(f"{feat}: past |mass|={d['abs_mass_past']} future |mass|={d['abs_mass_future']}")
        print("  curve:", {k: round(v, 4) for k, v in sorted(d["curve"].items())})
    return 0


def main() -> int:
    async def _main():
        try:
            return await _run()
        finally:
            await close_pool()
    try:
        return asyncio.run(_main())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
