"""E004 — EMA half-life nowcast experiment (runs what §E004 pre-registers).

    python3 -m scripts.eval.experiments.e004_ema

Engine: the validated replay scorer (identity check @aa282d6). All candidates
share ONE replayed raw composite (production layers + divergence cap, no
EMA); only the smoothing differs — primary {1h, 2h} vs the 4h incumbent,
exploratory {0.5h, adaptive}. Metrics per §E004: tracking lag (pooled xcorr
of Δsmoothed vs lagged Δraw, ticks), mean |smoothed − raw|, label flips per
ticker-week, tick-to-tick std, and the research-window-only forward-IC guard.
Changes nothing; emits a recommendation. Artifacts to exports/eval/E004/.
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv(override=True)

from scripts.db.connection import close_pool  # noqa: E402
from scripts.eval import analyze, data  # noqa: E402
from scripts.eval.run import RESEARCH_END, RESEARCH_START, _utc  # noqa: E402
from scripts.eval.replay import (  # noqa: E402
    PRODUCTION_CONFIG,
    apply_ema_series,
    replay_ticks,
    to_panel_input,
)
from api.response.labels import score_to_label  # noqa: E402
from pipeline.scoring.ema import compute_ema  # noqa: E402

OUT = Path("exports/eval/E004")

PRIMARY = [("2h", 2.0), ("1h", 1.0)]
INCUMBENT = ("4h_incumbent", 4.0)
EXPLORATORY = [("0.5h", 0.5), ("adaptive", "adaptive")]

FLIP_LIMIT = 1.5    # × incumbent
STD_LIMIT = 1.75    # × incumbent
GUARD_DELTA = 0.01  # candidate IC may not drop more than this below incumbent


def adaptive_ema(dt_hours: list[float], raws: list[float]) -> list[float]:
    """Exploratory: T½ = clamp(4h / (1 + |raw − prev|/5), 1h, 4h)."""
    out: list[float] = []
    prev: float | None = None
    for r, dth in zip(raws, dt_hours):
        if prev is None:
            s = r
        else:
            hl = max(1.0, min(4.0, 4.0 / (1.0 + abs(r - prev) / 5.0)))
            s = compute_ema(r, prev, dth, half_life_hours=hl)
        s = round(s, 2)
        out.append(s)
        prev = s
    return out


def smooth_all(groups: list[tuple[str, list[float], list[float]]], cand) -> pd.Series:
    """Per-ticker smoothing under a candidate; returns the concatenated series
    aligned with the concatenation order of `groups`."""
    chunks = []
    for _tk, dt_h, raws in groups:
        if cand == "adaptive":
            chunks.append(adaptive_ema(dt_h, raws))
        else:
            chunks.append(apply_ema_series(dt_h, raws, cand))
    return pd.Series(np.concatenate(chunks))


def metrics_for(frame: pd.DataFrame, smoothed: pd.Series) -> dict:
    """Nowcast metrics over the full history. frame: ticker/timestamp/raw
    (sorted ticker, timestamp; same order as smoothed)."""
    f = frame.copy()
    f["s"] = smoothed.values

    g = f.groupby("ticker", sort=False)
    f["ds"] = g["s"].diff()
    f["dr"] = g["raw"].diff()

    # tracking lag: pooled corr(Δs_t, Δraw_{t−k}), k = 0..6 ticks
    xcorr = {}
    for k in range(0, 7):
        drk = g["dr"].shift(k)
        d = pd.DataFrame({"x": f["ds"], "y": drk}).dropna()
        xcorr[k] = round(float(d["x"].corr(d["y"])), 4)
    peak_lag = max(xcorr, key=lambda k: xcorr[k])

    mean_gap = float((f["s"] - f["raw"]).abs().mean())

    # label flips per ticker-week
    f["label"] = f["s"].round().astype(int).map(score_to_label)
    flips = int((f["label"] != g["label"].shift()).sum() - f["ticker"].nunique())
    span_weeks = (
        (g["timestamp"].max() - g["timestamp"].min()).dt.total_seconds()
        / (7 * 86400)
    ).sum()
    flips_per_ticker_week = float(flips / span_weeks)

    tick_std = float(f["ds"].std())

    return {
        "xcorr_by_lag": xcorr,
        "tracking_lag_ticks": int(peak_lag),
        "mean_abs_gap_vs_raw": round(mean_gap, 3),
        "label_flips_per_ticker_week": round(flips_per_ticker_week, 4),
        "tick_std": round(tick_std, 4),
    }


def guard_ic(raw_ticks: pd.DataFrame, replayed_base: pd.DataFrame,
             smoothed: pd.Series, closes: pd.DataFrame) -> dict:
    """Research-window-only forward IC of the smoothed level (holdout guard)."""
    rep = replayed_base.copy()
    rep["replay_smoothed"] = smoothed.values
    panel_in = to_panel_input(raw_ticks, rep)
    panel_in = panel_in[panel_in["timestamp"] < _utc(RESEARCH_END)]
    s = analyze.prepare_daily(panel_in)
    panel = analyze.build_panel(s, closes)
    ic = analyze.ic_table(panel, feats=["score"], horizons=[1, 3])
    out = {}
    for h in (1, 3):
        cell = ic[(ic.horizon_d == h) & (ic.target == "mktneutral")]
        out[f"ic_h{h}"] = None if cell.empty else float(cell.iloc[0]["mean_IC"])
    return out


async def _run() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    from datetime import datetime, timedelta, timezone
    load_end = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"loading full tick history {RESEARCH_START}..{load_end} ...")
    raw_ticks = await data.load_sentiment_panel(
        _utc(RESEARCH_START), _utc(load_end), granularity="raw")
    closes = await data.load_close_panel(_utc(RESEARCH_START), _utc(RESEARCH_END))
    raw_ticks["timestamp"] = pd.to_datetime(raw_ticks["timestamp"], utc=True)

    # ONE composite replay shared by every candidate (production layers + cap)
    print("replaying raw composite (shared) ...")
    base_cfg = dict(PRODUCTION_CONFIG, ema_half_life_hours=None, name="raw_base")
    base = replay_ticks(raw_ticks, base_cfg)
    base = base.sort_values(["ticker", "timestamp"]).reset_index(drop=True)

    frame = base.rename(columns={"replay_raw": "raw"})[["ticker", "timestamp", "raw"]]
    groups: list[tuple[str, list[float], list[float]]] = []
    for tk, gr in frame.groupby("ticker", sort=False):
        dt_h = (gr["timestamp"].diff().dt.total_seconds().fillna(0.0) / 3600.0)
        groups.append((tk, list(dt_h), list(gr["raw"])))

    rows = []
    for name, cand in [INCUMBENT] + PRIMARY + EXPLORATORY:
        print(f"candidate {name} ...")
        smoothed = smooth_all(groups, cand)
        m = metrics_for(frame, smoothed)
        m.update(guard_ic(raw_ticks, base[["ticker", "timestamp", "replay_raw"]]
                          .rename(columns={"replay_raw": "replay_raw"}), smoothed, closes))
        m["candidate"] = name
        m["tier"] = ("incumbent" if name == INCUMBENT[0]
                     else "primary" if name in [n for n, _ in PRIMARY]
                     else "exploratory")
        rows.append(m)

    df = pd.DataFrame(rows).set_index("candidate")
    inc = df.loc[INCUMBENT[0]]

    # decision rule (§E004): shortest primary with flips ≤1.5×, std ≤1.75×, guard ok
    def qualifies(r) -> bool:
        guard_ok = all(
            r[f"ic_h{h}"] is not None and inc[f"ic_h{h}"] is not None
            and r[f"ic_h{h}"] >= inc[f"ic_h{h}"] - GUARD_DELTA
            for h in (1, 3)
        )
        return (
            r["label_flips_per_ticker_week"] <= FLIP_LIMIT * inc["label_flips_per_ticker_week"]
            and r["tick_std"] <= STD_LIMIT * inc["tick_std"]
            and guard_ok
        )

    qualifying = [n for n, hl in sorted(PRIMARY, key=lambda x: x[1]) if qualifies(df.loc[n])]
    recommendation = qualifying[0] if qualifying else INCUMBENT[0]

    df.to_csv(OUT / "metrics.csv")
    json.dump({"metrics": rows, "recommendation": recommendation,
               "rule": {"flip_limit_x": FLIP_LIMIT, "std_limit_x": STD_LIMIT,
                        "guard_delta": GUARD_DELTA}},
              open(OUT / "summary.json", "w"), indent=2, default=str)

    cols = ["tier", "tracking_lag_ticks", "mean_abs_gap_vs_raw",
            "label_flips_per_ticker_week", "tick_std", "ic_h1", "ic_h3"]
    print("\n=== E004 METRICS (full clean history; IC guard research-window only) ===")
    print(df[cols].to_string())
    print(f"\nincumbent flip baseline ×1.5 = "
          f"{1.5 * inc['label_flips_per_ticker_week']:.4f}; "
          f"std baseline ×1.75 = {1.75 * inc['tick_std']:.4f}")
    print(f"\nRULE-DERIVED RECOMMENDATION: {recommendation}")
    return 0


def main() -> int:
    async def _wrapped():
        try:
            return await _run()
        finally:
            await close_pool()
    try:
        return asyncio.run(_wrapped())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
