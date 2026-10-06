"""
research/run.py — run a strategy on a snapshot and write a report.

    python -m research.run --snapshot data/snapshots/2026-10-06 \\
        --strategy research.examples.quintile_ls:QuintileLongShort \\
        --param feature=score_raw --cost-bps 15

Parameters are passed to the strategy's constructor; numeric values are parsed
automatically. Output: research/runs/<timestamp>_<strategy>/ (REPORT.md,
summary.json, returns.csv).
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys

from research.backtest import run_backtest
from research.dataset import Snapshot
from research.metrics import report


def _load_strategy(spec: str, params: dict):
    module, _, cls = spec.partition(":")
    if not cls:
        raise SystemExit("--strategy must look like package.module:ClassName")
    return getattr(importlib.import_module(module), cls)(**params)


def _parse_params(items: list[str]) -> dict:
    out = {}
    for item in items:
        key, _, raw = item.partition("=")
        try:
            out[key] = json.loads(raw)
        except json.JSONDecodeError:
            out[key] = raw
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--snapshot", required=True)
    p.add_argument("--strategy", required=True, help="package.module:ClassName")
    p.add_argument("--param", action="append", default=[], metavar="KEY=VALUE")
    p.add_argument("--start")
    p.add_argument("--end")
    p.add_argument("--lag", type=int, default=1)
    p.add_argument("--cost-bps", type=float, default=10.0)
    p.add_argument("--sp500-only", action="store_true")
    p.add_argument(
        "--live-only", action="store_true", help="exclude rows recomputed offline (replay_run)"
    )
    p.add_argument("--out", default="research/runs")
    args = p.parse_args(argv)

    snap = Snapshot.load(args.snapshot, include_replayed=not args.live_only)
    strategy = _load_strategy(args.strategy, _parse_params(args.param))
    result = run_backtest(
        snap,
        strategy,
        start=args.start,
        end=args.end,
        lag=args.lag,
        cost_bps=args.cost_bps,
        sp500_only=args.sp500_only,
    )
    out = report(result, args.out)
    print((out / "REPORT.md").read_text())
    print(f"written to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
