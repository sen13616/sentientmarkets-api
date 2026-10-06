"""
research/backtest.py — daily, close-to-close backtester with no look-ahead.

Timeline for a signal date d (any calendar day, weekends included):
  1. The strategy sees signal rows dated <= d (a day's row is the last scoring
     tick of that US/Eastern day, so it can include news up to midnight ET).
  2. Its target weights are executed at the close of the lag-th trading day
     strictly after d (default lag = 1; lag 0 is rejected — a day's row is not
     known at that day's close). When several dates map to the same close
     (Fri/Sat/Sun → Mon) the latest is used.
  3. Those weights earn close-to-close returns from that close to the next.

Costs: ``cost_bps`` per unit of one-way turnover, sum(|w_new - w_old|), charged
on each execution day. A ticker that stops trading (past its last trading day
or missing a price) is closed at its last price: it earns 0 afterwards and its
weight is released at the next rebalance. Signal days that fall inside a
documented data gap (no scoring ticks) keep the previous weights.

Benchmark: equal-weight long-only portfolio of the point-in-time universe,
rebalanced daily, no costs.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from research.dataset import Snapshot
from research.strategy import SignalView, Strategy


@dataclass
class BacktestResult:
    strategy: str
    params: dict
    config: dict
    returns: (
        pd.DataFrame
    )  # index = trading day; gross, cost, net, benchmark, turnover, gross_exposure
    weights: pd.DataFrame  # executed weights: index = execution day, columns = tickers


def _in_gap(day: pd.Timestamp, gaps: list[tuple[pd.Timestamp, pd.Timestamp]]) -> bool:
    start, end = day.tz_localize("UTC"), day.tz_localize("UTC") + pd.Timedelta(days=1)
    return any(start < g_end and end > g_start for g_start, g_end in gaps)


def run_backtest(
    snap: Snapshot,
    strategy: Strategy,
    start: str | pd.Timestamp | None = None,
    end: str | pd.Timestamp | None = None,
    lag: int = 1,
    cost_bps: float = 10.0,
    sp500_only: bool = False,
    skip_gap_days: bool = True,
) -> BacktestResult:
    if lag < 1:
        raise ValueError("lag must be >= 1: a day's signal can't be traded at that day's close")

    closes = snap.closes
    rets = closes.pct_change(fill_method=None)
    days = closes.index
    signal_dates = pd.DatetimeIndex(sorted(pd.to_datetime(snap.signals["date"].unique())))
    if start is not None:
        signal_dates = signal_dates[signal_dates >= pd.Timestamp(start)]
    if end is not None:
        signal_dates = signal_dates[signal_dates < pd.Timestamp(end)]

    # Each calendar signal date d (weekends included — they carry weekend news)
    # executes at the close of the lag-th trading day strictly after d. When
    # several dates map to the same execution close (Fri/Sat/Sun → Mon), the
    # latest one is used: everything it contains was known before that close.
    schedule: dict[pd.Timestamp, pd.Timestamp] = {}
    for d in signal_dates:
        i_exec = days.searchsorted(d, side="right") + (lag - 1)
        if i_exec < len(days):
            schedule[days[i_exec]] = d

    sig = snap.signals.sort_values("date")
    gaps = snap.known_gaps

    targets: dict[pd.Timestamp, pd.Series] = {}
    prev = pd.Series(dtype=float)
    for exec_day, d in sorted(schedule.items()):
        if skip_gap_days and _in_gap(d, gaps):
            targets[exec_day] = prev
            continue
        universe = frozenset(snap.universe_on(d, sp500_only=sp500_only))
        hist = sig[(sig["date"] <= d) & sig["ticker"].isin(universe)]
        w = strategy.weights(SignalView(date=d, history=hist, universe=universe))
        w = pd.Series(w, dtype=float).dropna()
        w = w[w.index.isin(universe) & (w != 0)]
        # only tradable names: priced at the execution close
        priced = closes.loc[exec_day].dropna().index
        w = w[w.index.isin(priced)]
        targets[exec_day] = w
        prev = w

    if not targets:
        raise ValueError("no signal days in the requested window")

    exec_days = sorted(targets)
    tickers = sorted(set().union(*(t.index for t in targets.values())) or [])
    W = pd.DataFrame(0.0, index=exec_days, columns=tickers)
    for d, w in targets.items():
        W.loc[d, w.index] = w.values

    # Hold executed weights until the next execution; returns accrue on the
    # day after execution (close-to-close).
    first = exec_days[0]
    period = days[(days > first)]
    held = W.reindex(days).ffill().shift(1).loc[period].fillna(0.0)
    r = rets.reindex(columns=tickers).loc[period]
    contrib = (held * r).where(r.notna(), 0.0)  # stopped names earn 0
    gross = contrib.sum(axis=1)

    # One-way turnover on each execution day (weights held between executions).
    w_daily = W.reindex(days).ffill().loc[first:].fillna(0.0)
    turnover = w_daily.diff().abs().sum(axis=1)
    turnover.iloc[0] = w_daily.iloc[0].abs().sum()
    # Trades happen at the execution-day close; their cost lands in the next
    # day's P&L, alongside the first return those weights earn.
    traded = turnover.shift(1).reindex(period).fillna(0.0)
    cost = traded * cost_bps / 1e4

    bench = []
    for d in period:
        names = snap.universe_on(d, sp500_only=sp500_only)
        day_r = rets.loc[d].reindex(sorted(names)).dropna()
        bench.append(day_r.mean() if len(day_r) else np.nan)

    out = pd.DataFrame(
        {
            "gross": gross,
            "cost": cost,
            "net": gross - cost,
            "benchmark": bench,
            "turnover": traded,
            "gross_exposure": held.abs().sum(axis=1),
        },
        index=period,
    )
    config = {
        "start": str(min(schedule.values()).date()),
        "end": str(out.index[-1].date()),
        "lag": lag,
        "cost_bps": cost_bps,
        "sp500_only": sp500_only,
        "skip_gap_days": skip_gap_days,
        "snapshot": str(snap.path),
        "snapshot_commit": snap.manifest.get("git_commit"),
    }
    return BacktestResult(strategy.name, strategy.params(), config, out, W)
