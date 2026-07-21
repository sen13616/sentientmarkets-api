"""Scorecard assembly + regression detection for the release gate.

The scorecard is the measurable form of the guiding statement: every published
number should be an honest, calibrated, responsive description of current
cross-sectional sentiment, and anything predictive must beat this scorecard.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

import numpy as np
import pandas as pd

LAYERS = ["market", "narrative", "influencer", "macro"]


def _series_stats(daily: pd.DataFrame, col: str) -> dict | None:
    vals = daily[col].dropna()
    if vals.empty:
        return None
    # xs_dispersion = mean of per-day cross-sectional std — the "does the
    # universe actually spread out?" number the composite was crushing
    per_day_std = daily.dropna(subset=[col]).groupby("date")[col].std()
    return {
        "mean": round(float(vals.mean()), 2),
        "std": round(float(vals.std(ddof=1)), 2),
        "xs_dispersion": round(float(per_day_std.mean()), 2),
        "abs_dev_from_50": round(abs(float(vals.mean()) - 50.0), 2),
    }


def _leadlag_summary(ll: pd.DataFrame, offset_col: str = "lag_k") -> dict | None:
    if ll is None or ll.empty:
        return None
    peak = ll.loc[ll["corr"].abs().idxmax()]
    future = ll[ll[offset_col] > 0]
    return {
        "peak_offset": int(peak[offset_col]),
        "peak_corr": float(peak["corr"]),
        "max_future_corr": float(future["corr"].max()) if not future.empty else None,
        "curve": {int(r[offset_col]): float(r["corr"]) for _, r in ll.iterrows()},
    }


def build_scorecard(
    daily: pd.DataFrame,
    ic: pd.DataFrame,
    leadlag_raw: pd.DataFrame,
    leadlag_exo: pd.DataFrame,
    meta: dict | None = None,
    latency: pd.DataFrame | None = None,
    ls: list[dict] | None = None,
) -> dict:
    card: dict = {"meta": meta or {}}
    card["meta"].setdefault("n_tickers", int(daily["ticker"].nunique()))
    card["meta"].setdefault("n_ticker_days", int(len(daily)))

    card["calibration"] = {
        "composite_raw": _series_stats(daily, "score_raw"),
        "composite_smoothed": _series_stats(daily, "score"),
        "exo": _series_stats(daily, "exo") if "exo" in daily else None,
        "layers": {ly: _series_stats(daily, ly) for ly in LAYERS if ly in daily},
    }

    ic_records = [] if ic is None or ic.empty else ic.to_dict("records")
    card["predictive"] = {
        "ic_table": ic_records,
        "top_by_abs_t": ic_records[:8],
    }

    card["leadlag"] = {
        "raw_change": _leadlag_summary(leadlag_raw),
        "exo_change": _leadlag_summary(leadlag_exo),
    }

    # Quintile long-short spreads, gross AND net of the turnover cost overlay
    # (analyze.quintile_ls cost_bps). Absent from pre-E001 cards; scorecard
    # diff skips missing metrics, so old baselines stay comparable.
    if ls:
        card["quintile_ls"] = ls

    if latency is not None and not latency.empty:
        by_src = latency.groupby("source")["latency_minutes"]
        card["information_latency"] = {
            src: {
                "median_minutes": round(float(g.median()), 1),
                "p90_minutes": round(float(g.quantile(0.9)), 1),
                "n": int(len(g)),
            }
            for src, g in by_src
        }
    return card


# ---------------------------------------------------------------- regression --

@dataclass(frozen=True)
class Rule:
    path: str          # dot-path into the scorecard dict
    better: str        # "higher" | "lower"
    abs_tol: float = 0.0
    rel_tol: float = 0.0


# Tolerances documented in scripts/eval/baselines/README.md. A change "regresses" when it
# moves the metric in the bad direction by more than the tolerance.
DEFAULT_RULES = [
    # dispersion must not shrink further (the composite already crushes tails)
    Rule("calibration.composite_raw.std", "higher", rel_tol=0.10),
    Rule("calibration.composite_raw.xs_dispersion", "higher", rel_tol=0.10),
    # calibration: layer means must not drift further from neutral-50
    *[
        Rule(f"calibration.layers.{ly}.abs_dev_from_50", "lower", abs_tol=1.0)
        for ly in LAYERS
    ],
    # the lead-lag peak must never move further into the past
    Rule("leadlag.raw_change.peak_offset", "higher"),
    Rule("leadlag.exo_change.peak_offset", "higher"),
]


def _get(card: dict, path: str):
    node = card
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def diff(baseline: dict, current: dict, rules: list[Rule] = DEFAULT_RULES) -> list[str]:
    """Return human-readable regression messages (empty list = gate passes).

    Metrics missing from either card are skipped with a note rather than
    failed — a new metric has no baseline, and that's fine.
    """
    regressions = []
    for rule in rules:
        base = _get(baseline, rule.path)
        cur = _get(current, rule.path)
        if base is None or cur is None:
            continue
        delta = cur - base
        allowed = rule.abs_tol + rule.rel_tol * abs(base)
        bad = delta < -allowed if rule.better == "higher" else delta > allowed
        if bad:
            regressions.append(
                f"{rule.path}: {base} -> {cur} "
                f"({'-' if delta < 0 else '+'}{abs(delta):.3g}, tol {allowed:.3g}, "
                f"{rule.better} is better)"
            )
    return regressions


# ----------------------------------------------------------------- rendering --

def render_markdown(card: dict) -> str:
    lines = ["# Eval scorecard", ""]
    meta = card.get("meta", {})
    if meta:
        lines.append(
            "**Window:** "
            f"{meta.get('start', '?')} → {meta.get('end', '?')} · "
            f"{meta.get('n_tickers', '?')} tickers · "
            f"{meta.get('n_ticker_days', '?')} ticker-days"
        )
        lines.append("")

    cal = card.get("calibration", {})
    lines += ["## Calibration & dispersion", "",
              "| series | mean | std | xs dispersion | |mean−50| |",
              "|---|---|---|---|---|"]
    named = [("composite_raw", cal.get("composite_raw")),
             ("composite_smoothed", cal.get("composite_smoothed")),
             ("exo", cal.get("exo"))]
    named += [(ly, cal.get("layers", {}).get(ly)) for ly in LAYERS]
    for name, st in named:
        if st:
            lines.append(
                f"| {name} | {st['mean']} | {st['std']} | "
                f"{st['xs_dispersion']} | {st['abs_dev_from_50']} |"
            )
    lines.append("")

    ll = card.get("leadlag", {})
    lines += ["## Lead-lag (mirror vs headlight)", ""]
    for key, label in [("raw_change", "raw score Δ"), ("exo_change", "exogenous Δ")]:
        s = ll.get(key)
        if s:
            lines.append(
                f"- **{label}**: peak at offset {s['peak_offset']} "
                f"(corr {s['peak_corr']:+.3f}); max future-side corr "
                f"{s['max_future_corr'] if s['max_future_corr'] is not None else 'n/a'}"
            )
    lines.append("")

    top = card.get("predictive", {}).get("top_by_abs_t", [])
    if top:
        lines += ["## Top ICs (by |t|)", "",
                  "| feature | h | target | IC | t | hit | days |",
                  "|---|---|---|---|---|---|---|"]
        for r in top:
            lines.append(
                f"| {r['feature']} | {r['horizon_d']} | {r['target']} | "
                f"{r['mean_IC']} | {r['IC_t']} | {r['hit_rate']} | {r['n_days']} |"
            )
        lines.append("")
        lines.append(
            "*hit = fraction of DAYS with positive daily cross-sectional IC "
            "(not per-trade accuracy).*"
        )
        lines.append("")

    ls = card.get("quintile_ls")
    if ls:
        cost = ls[0].get("cost_bps", "?")
        lines += [f"## Quintile L/S — gross vs net ({cost} bps round-trip on leg turnover)",
                  "",
                  "| feature | h | gross/period | net/period | t | t_net | "
                  "ann Sharpe | ann Sharpe net | leg turnover | days |",
                  "|---|---|---|---|---|---|---|---|---|---|"]
        for r in ls:
            lines.append(
                f"| {r['feature']} | {r['horizon_d']} | "
                f"{r['mean_LS_per_period']} | {r.get('mean_LS_net_per_period', 'n/a')} | "
                f"{r['t']} | {r.get('t_net', 'n/a')} | "
                f"{r['ann_sharpe']} | {r.get('ann_sharpe_net', 'n/a')} | "
                f"{r.get('avg_leg_turnover', 'n/a')} | {r['n_days']} |"
            )
        lines.append("")

    lat = card.get("information_latency")
    if lat:
        lines += ["## Information latency (created_at − published_at)", "",
                  "| source | median (min) | p90 (min) | n |", "|---|---|---|---|"]
        for src, st in sorted(lat.items()):
            lines.append(
                f"| {src} | {st['median_minutes']} | {st['p90_minutes']} | {st['n']} |"
            )
        lines.append("")
    return "\n".join(lines)


def save(card: dict, json_path: str, md_path: str | None = None) -> None:
    with open(json_path, "w") as f:
        json.dump(card, f, indent=2, default=str)
    if md_path:
        with open(md_path, "w") as f:
            f.write(render_markdown(card))


def load(json_path: str) -> dict:
    with open(json_path) as f:
        return json.load(f)
