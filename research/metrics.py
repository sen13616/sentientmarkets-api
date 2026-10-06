"""
research/metrics.py — performance statistics and run reports.
"""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

TRADING_DAYS = 252


def summary(returns: pd.Series, periods: int = TRADING_DAYS) -> dict:
    """Standard statistics for a daily simple-return series."""
    r = returns.dropna()
    if r.empty:
        return {"days": 0}
    equity = (1 + r).cumprod()
    years = len(r) / periods
    vol = r.std(ddof=1) * math.sqrt(periods) if len(r) > 1 else float("nan")
    downside = r[r < 0].std(ddof=1) * math.sqrt(periods) if (r < 0).sum() > 1 else float("nan")
    mean_ann = r.mean() * periods
    drawdown = equity / equity.cummax() - 1
    return {
        "days": int(len(r)),
        "total_return": float(equity.iloc[-1] - 1),
        "cagr": float(equity.iloc[-1] ** (1 / years) - 1) if years > 0 else float("nan"),
        "vol": float(vol),
        "sharpe": float(mean_ann / vol) if vol and vol > 0 else float("nan"),
        "sortino": float(mean_ann / downside) if downside and downside > 0 else float("nan"),
        "max_drawdown": float(drawdown.min()),
        "hit_rate": float((r > 0).mean()),
    }


def information_coefficient(
    signals: pd.DataFrame, closes: pd.DataFrame, feature: str, horizon: int = 1
) -> dict:
    """
    Mean daily Spearman rank IC between ``feature`` on day t and the forward
    close-to-close return from t+1 to t+1+horizon (the backtester's lag-1 timing).
    """
    fwd = closes.shift(-(1 + horizon)) / closes.shift(-1) - 1
    rows = []
    for d, g in signals.groupby("date"):
        if d not in fwd.index:
            continue
        x = g.set_index("ticker")[feature].dropna()
        y = fwd.loc[d].reindex(x.index).dropna()
        x = x.reindex(y.index)
        if len(x) >= 20 and x.nunique() > 3:
            rows.append(x.rank().corr(y.rank()))
    ic = pd.Series(rows, dtype=float)
    return {
        "feature": feature,
        "horizon_days": horizon,
        "days": int(len(ic)),
        "mean_ic": float(ic.mean()) if len(ic) else float("nan"),
        "ic_tstat": float(ic.mean() / ic.std(ddof=1) * math.sqrt(len(ic)))
        if len(ic) > 2
        else float("nan"),
    }


def report(result, out_root: str | Path = "research/runs") -> Path:
    """Write ``summary.json``, ``returns.csv`` and ``REPORT.md`` for a BacktestResult."""
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out = Path(out_root) / f"{stamp}_{result.strategy}"
    out.mkdir(parents=True, exist_ok=True)
    rets = result.returns
    stats = {
        "strategy": result.strategy,
        "params": result.params,
        "config": result.config,
        "net": summary(rets["net"]),
        "gross": summary(rets["gross"]),
        "benchmark": summary(rets["benchmark"]),
        "avg_daily_turnover": float(rets["turnover"].mean()),
        "avg_gross_exposure": float(rets["gross_exposure"].mean()),
    }
    (out / "summary.json").write_text(json.dumps(stats, indent=2, default=str))
    rets.to_csv(out / "returns.csv")

    def row(name: str, s: dict) -> str:
        f = lambda k: "—" if k not in s or s[k] != s[k] else f"{s[k]:.4f}"  # noqa: E731
        return f"| {name} | {s.get('days', 0)} | {f('total_return')} | {f('sharpe')} | {f('max_drawdown')} | {f('hit_rate')} |"

    md = [
        f"# Backtest — {result.strategy}",
        "",
        f"Params: `{json.dumps(result.params, default=str)}`  ",
        f"Config: `{json.dumps(result.config, default=str)}`",
        "",
        "| series | days | total return | Sharpe | max DD | hit rate |",
        "|---|---|---|---|---|---|",
        row("net", stats["net"]),
        row("gross", stats["gross"]),
        row("benchmark (EW universe)", stats["benchmark"]),
        "",
        f"Average daily one-way turnover: {stats['avg_daily_turnover']:.3f}; "
        f"average gross exposure: {stats['avg_gross_exposure']:.2f}.",
    ]
    (out / "REPORT.md").write_text("\n".join(md) + "\n")
    return out
