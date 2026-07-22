"""Counterfactual replay scorer (Track A, 2026-07-22).

    python3 -m scripts.eval.replay --config <file.json> --window research|full
        [--out exports/eval/replay] [--identity]

Reconstructs the score series that a candidate scoring config WOULD have
produced over stored history, emitting per-tick columns (`replay_raw`,
`replay_smoothed`) that the standard harness consumes with no special-casing
(see `to_panel_input`, which returns a frame shaped exactly like
`data.load_sentiment_panel` output).

Config surface (v1 — deliberately small)
----------------------------------------
{
  "name":                "incumbent",
  "layers":              {"market": 0.35, "narrative": 0.30,
                          "influencer": 0.25, "macro": 0.10},
  "ema_half_life_hours": 4.0,          // null = no smoothing (raw passthrough)
  "divergence_cap":      true
}
Nothing else until an experiment needs it.

v1 data-source boundary (documented, load-bearing)
--------------------------------------------------
The replay reads stored PER-TICK SUB-INDICES (+ metadata) from
sentiment_history — fast and deterministic. That is sufficient for every knob
in the v1 config surface (layer selection/weights, EMA, divergence cap),
including E004. Configs that would change the sub-indices THEMSELVES
(normalizer weights, half-lives, z-score windows, new signals…) are OUT OF
SCOPE until a v2 that re-scores from raw_signals.

Fidelity & rounding
-------------------
The replay calls the production pure functions (`compute_composite`,
`compute_divergence`, `compute_ema`) and mirrors production rounding: raw and
smoothed are rounded to 2 dp per tick, and — like the production loop, whose
EMA reads the previous tick's ROUNDED smoothed value back from Redis — each
EMA step consumes the rounded predecessor. Two known, bounded deviation
sources vs stored history remain:
  * stored sub-indices are rounded to 2 dp while production composited from
    4 dp values → raw can differ by ≲0.01;
  * production reseeds the EMA (smoothed := raw) whenever Redis state was
    absent (deploys, >24 h key TTL expiry, the outage) — events invisible in
    sentiment_history. Replay instead applies the α→1 large-dt formula, so
    isolated post-gap ticks can deviate more, decaying immediately.
The identity check (`--identity`) quantifies both. It must pass before any
candidate config is trusted (see tests/test_replay.py for the synthetic
equivalence version that runs in CI).

Point-in-time: tick t's replayed value is a function of stored rows with
timestamp <= t only (strict per-ticker sequential fold; test-enforced).

EMA seeding: replay always starts at each ticker's FIRST stored tick (cold
start smoothed=raw, like production's first-ever score) regardless of
--window; the window filters the OUTPUT only. Replaying the window in
isolation would fabricate a cold start mid-history.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv(override=True)

from scripts.db.connection import close_pool  # noqa: E402
from scripts.eval import data  # noqa: E402
from scripts.eval.run import HOLDOUT_START, RESEARCH_END, RESEARCH_START, _utc  # noqa: E402
from pipeline.scoring.composite import LAYER_WEIGHTS, compute_composite  # noqa: E402
from pipeline.scoring.divergence import compute_divergence  # noqa: E402
from pipeline.scoring.ema import compute_ema  # noqa: E402

_LAYER_COLS = {
    "market": "market_index",
    "narrative": "narrative_index",
    "influencer": "influencer_index",
    "macro": "macro_index",
}

#: The production config — replaying it must reproduce stored history
#: (identity check). Keep in sync with pipeline.scoring.composite/ema.
PRODUCTION_CONFIG: dict = {
    "name": "production",
    "layers": dict(LAYER_WEIGHTS),
    "ema_half_life_hours": 4.0,
    "divergence_cap": True,
}

#: Documented identity tolerances (see module docstring for the two known
#: deviation sources). Measured 2026-07-22 over the full clean history.
IDENTITY_TOLERANCES = {
    "raw_p99": 0.01,       # rounding of stored sub-indices
    "raw_max": 0.05,
    "smoothed_p99": 0.05,  # rounding cascade through the EMA
    "exo_p99": 0.01,
    "exo_max": 0.05,
}


def load_config(path: str | None) -> dict:
    if path is None:
        return dict(PRODUCTION_CONFIG)
    cfg = json.loads(Path(path).read_text())
    unknown = set(cfg) - {"name", "layers", "ema_half_life_hours", "divergence_cap"}
    if unknown:
        raise SystemExit(f"unknown config keys (v1 surface is small on purpose): {unknown}")
    if not cfg.get("layers"):
        raise SystemExit("config must name at least one layer with a weight")
    bad = set(cfg["layers"]) - set(_LAYER_COLS)
    if bad:
        raise SystemExit(f"unknown layers: {bad}")
    cfg.setdefault("name", Path(path).stem)
    cfg.setdefault("ema_half_life_hours", None)
    cfg.setdefault("divergence_cap", False)
    return cfg


def apply_ema_series(
    dt_hours: list[float],
    raws: list[float],
    half_life_hours: float | None,
) -> list[float]:
    """Fold the production EMA over one ticker's raw series (sorted by time).

    Single source of truth for the replay EMA — used by replay_ticks and by
    experiment drivers that re-smooth a fixed raw series under many
    half-lives (E004). Mirrors production rounding: each step consumes the
    previous ROUNDED value. half_life_hours=None → raw passthrough.
    """
    if half_life_hours is None:
        return list(raws)
    out: list[float] = []
    prev: float | None = None
    for r, dth in zip(raws, dt_hours):
        s = round(compute_ema(r, prev, dth, half_life_hours=half_life_hours), 2)
        out.append(s)
        prev = s
    return out


def replay_ticks(raw_ticks: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Fold the candidate config over stored per-tick sub-indices.

    raw_ticks: load_sentiment_panel(granularity="raw") frame — must contain
    ticker, timestamp, and the four *_index columns. Returns a frame with
    ticker, timestamp, replay_raw, replay_smoothed (smoothed == raw when the
    config has no EMA).
    """
    weights: dict[str, float] = config["layers"]
    hl = config.get("ema_half_life_hours")
    cap = bool(config.get("divergence_cap"))

    # compute_composite renormalizes over present layers using LAYER_WEIGHTS;
    # for arbitrary config weights we renormalize here and feed values direct.
    def _raw(row) -> float:
        present = {
            ly: row[_LAYER_COLS[ly]]
            for ly in weights
            if row[_LAYER_COLS[ly]] is not None and pd.notna(row[_LAYER_COLS[ly]])
        }
        if not present:
            return 50.0
        total_w = sum(weights[ly] for ly in present)
        score = sum(weights[ly] / total_w * float(v) for ly, v in present.items())
        if cap:
            _, score = compute_divergence(
                {ly: float(v) for ly, v in present.items()}, score
            )
        return round(score, 2)

    out = []
    for ticker, g in raw_ticks.groupby("ticker", sort=False):
        g = g.sort_values("timestamp")
        # dt from a converted copy; emit the ORIGINAL timestamp values so the
        # output merges cleanly back onto the caller's frame (dtype-preserving)
        ts = pd.to_datetime(g["timestamp"], utc=True)
        dt_h = ts.diff().dt.total_seconds().fillna(0.0) / 3600.0

        raws = [_raw(row) for row in g.to_dict("records")]
        smooths = apply_ema_series(list(dt_h), raws, hl)
        out.append(pd.DataFrame({
            "ticker": ticker,
            "timestamp": g["timestamp"].array,  # .values would drop the tz
            "replay_raw": raws,
            "replay_smoothed": smooths,
        }))
    return pd.concat(out, ignore_index=True)


def to_panel_input(raw_ticks: pd.DataFrame, replayed: pd.DataFrame) -> pd.DataFrame:
    """Shape replay output exactly like load_sentiment_panel output, so
    analyze.prepare_daily / the whole harness consume it with no special-casing:
    composite_score := replay_raw, composite_score_smoothed := replay_smoothed."""
    merged = raw_ticks.merge(replayed, on=["ticker", "timestamp"], how="inner")
    merged["composite_score"] = merged["replay_raw"]
    merged["composite_score_smoothed"] = merged["replay_smoothed"]
    return merged.drop(columns=["replay_raw", "replay_smoothed"])


def window_bounds(window: str) -> tuple[str, str]:
    if window == "research":
        return RESEARCH_START, RESEARCH_END
    if window == "full":
        # full CLEAN history: everything stored, start of program → now+1d.
        from datetime import datetime, timedelta, timezone
        return RESEARCH_START, (
            datetime.now(timezone.utc) + timedelta(days=1)
        ).strftime("%Y-%m-%d")
    raise SystemExit(f"unknown window {window!r} (research|full)")


def identity_report(raw_ticks: pd.DataFrame, replayed: pd.DataFrame) -> dict:
    """Compare production-config replay vs stored raw/smoothed/exo series."""
    m = raw_ticks.merge(replayed, on=["ticker", "timestamp"], how="inner")

    def _stats(diff: pd.Series) -> dict:
        d = diff.abs().dropna()
        return {
            "n": int(len(d)),
            "mean": round(float(d.mean()), 5),
            "p95": round(float(d.quantile(0.95)), 4),
            "p99": round(float(d.quantile(0.99)), 4),
            "max": round(float(d.max()), 4),
            "share_gt_0p1": round(float((d > 0.1).mean()), 6),
        }

    rep: dict = {
        "raw": _stats(m["replay_raw"] - pd.to_numeric(m["composite_score"])),
        "smoothed": _stats(
            m["replay_smoothed"] - pd.to_numeric(m["composite_score_smoothed"])
        ),
    }

    # exo identity: the config surface itself must reproduce score_exo when
    # given the three exo layers and no EMA / no cap.
    if "composite_score_exo" in raw_ticks.columns:
        exo_cfg = {
            "name": "exo-identity",
            "layers": {"narrative": 0.30, "influencer": 0.25, "macro": 0.10},
            "ema_half_life_hours": None,
            "divergence_cap": False,
        }
        exo_rep = replay_ticks(raw_ticks, exo_cfg)
        me = raw_ticks.merge(exo_rep, on=["ticker", "timestamp"], how="inner")
        me = me[pd.to_numeric(me["composite_score_exo"], errors="coerce").notna()]
        rep["exo"] = _stats(
            me["replay_raw"] - pd.to_numeric(me["composite_score_exo"])
        )

    rep["tolerances"] = IDENTITY_TOLERANCES
    rep["pass"] = (
        rep["raw"]["p99"] <= IDENTITY_TOLERANCES["raw_p99"]
        and rep["raw"]["max"] <= IDENTITY_TOLERANCES["raw_max"]
        and rep["smoothed"]["p99"] <= IDENTITY_TOLERANCES["smoothed_p99"]
        and ("exo" not in rep or (
            rep["exo"]["p99"] <= IDENTITY_TOLERANCES["exo_p99"]
            and rep["exo"]["max"] <= IDENTITY_TOLERANCES["exo_max"]
        ))
    )
    return rep


async def _run(args) -> int:
    cfg = load_config(args.config)
    start_s, end_s = window_bounds(args.window)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    # Always LOAD from the start of history (EMA seeding), filter output later.
    from datetime import datetime, timedelta, timezone
    load_end = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"loading full tick history {RESEARCH_START}..{load_end} "
          f"(output window {start_s}..{end_s}) ...")
    raw = await data.load_sentiment_panel(_utc(RESEARCH_START), _utc(load_end),
                                          granularity="raw")
    if raw.empty:
        print("no data", file=sys.stderr)
        return 2
    raw["timestamp"] = pd.to_datetime(raw["timestamp"], utc=True)

    if args.identity:
        replayed = replay_ticks(raw, PRODUCTION_CONFIG)
        rep = identity_report(raw, replayed)
        json.dump(rep, open(out / "identity_report.json", "w"), indent=2)
        print(json.dumps(rep, indent=2))
        print(f"\nIDENTITY {'PASS' if rep['pass'] else 'FAIL'}")
        return 0 if rep["pass"] else 1

    replayed = replay_ticks(raw, cfg)
    mask = (replayed["timestamp"] >= _utc(start_s)) & (replayed["timestamp"] < _utc(end_s))
    windowed = replayed[mask]
    path = out / f"replay_{cfg['name']}_{args.window}.csv"
    windowed.to_csv(path, index=False)
    print(f"replayed {len(windowed)} ticks ({windowed['ticker'].nunique()} tickers) "
          f"under config '{cfg['name']}' → {path}")
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", help="candidate config JSON (default: production)")
    p.add_argument("--window", choices=["research", "full"], default="research")
    p.add_argument("--out", default="exports/eval/replay")
    p.add_argument("--identity", action="store_true",
                   help="validate: replay the production config and compare "
                        "against stored history (must pass before any candidate)")
    args = p.parse_args(argv)

    async def _wrapped():
        try:
            return await _run(args)
        finally:
            await close_pool()

    try:
        return asyncio.run(_wrapped())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
