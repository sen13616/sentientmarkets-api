"""
tests/test_research.py — the strategy-testing toolkit (research/).

Uses a synthetic snapshot (no DB). The central property is execution timing:
weights decided on signal day t are executed at the close of t+lag and earn
the return from t+lag to t+lag+1 — never a return the signal could not have
known about.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.backtest import run_backtest
from research.dataset import Snapshot
from research.examples.quintile_ls import QuintileLongShort
from research.examples.top_decile_long import TopDecileLong
from research.metrics import information_coefficient, report, summary
from research.snapshot import build_manifest, tidy_prices
from research.strategy import SignalView, Strategy

N_TICKERS, N_DAYS = 60, 40


def _synthetic(
    seed: int = 7, gaps=None, delisted=None, added=None, replayed=(), include_replayed=True
) -> Snapshot:
    rng = np.random.default_rng(seed)
    tickers = [f"T{i:02d}" for i in range(N_TICKERS)]
    days = pd.bdate_range("2026-09-01", periods=N_DAYS)
    rets = rng.normal(0, 0.02, size=(N_DAYS, N_TICKERS))
    closes = 100 * np.cumprod(1 + rets, axis=0)
    prices = pd.DataFrame(
        [
            {"ticker": t, "date": d, "open": closes[i, j], "close": closes[i, j], "volume": 1e6}
            for i, d in enumerate(days)
            for j, t in enumerate(tickers)
        ]
    )
    sent = []
    for d in days:
        ts = (d + pd.Timedelta(hours=22)).tz_localize("UTC")  # evening tick, same ET day
        for t in tickers:
            sent.append(
                {
                    "ticker": t,
                    "timestamp": ts,
                    "composite_score": float(rng.uniform(20, 80)),
                    "composite_score_smoothed": float(rng.uniform(20, 80)),
                    "composite_score_exo": None,
                    "market_index": 50.0,
                    "narrative_index": 50.0,
                    "influencer_index": 50.0,
                    "macro_index": 50.0,
                    "confidence_score": 80,
                    "divergence": "aligned",
                    "replay_run": "test-run" if t in replayed else None,
                }
            )
    universe = pd.DataFrame(
        {
            "ticker": tickers,
            "company_name": tickers,
            "sector": "Industrials",
            "in_sp500": True,
            "sp500_added": None,
            "added_at": pd.Timestamp("2026-04-24", tz="UTC"),
            "delisted_at": pd.Series([pd.NaT] * N_TICKERS, dtype="datetime64[ns, UTC]"),
            "successor_ticker": None,
            "delisted_reason": None,
        }
    )
    for t, when in (delisted or {}).items():
        universe.loc[universe.ticker == t, "delisted_at"] = pd.Timestamp(when, tz="UTC")
    for t, when in (added or {}).items():
        universe.loc[universe.ticker == t, "added_at"] = pd.Timestamp(when, tz="UTC")
    manifest = {"git_commit": "test", "known_gaps": gaps or []}
    return Snapshot.from_frames(
        manifest, pd.DataFrame(sent), prices, universe, include_replayed=include_replayed
    )


class Oracle(Strategy):
    """Long the names with the best return over a chosen future day (cheating on purpose)."""

    name = "oracle"

    def __init__(self, closes: pd.DataFrame, offset: int):
        self._fwd = closes.pct_change(fill_method=None).shift(-offset)
        self.offset = offset

    def weights(self, view: SignalView) -> pd.Series:
        r = self._fwd.loc[view.date].dropna()
        top = r.nlargest(10).index
        return pd.Series(0.1, index=top)


def test_execution_timing_is_signal_day_plus_lag():
    snap = _synthetic()
    # Knowing day t+2's return (earned by weights executed at t+1's close) is a
    # huge edge; knowing day t+1's return (already realised by execution) is none.
    exploitable = run_backtest(snap, Oracle(snap.closes, offset=2), cost_bps=0)
    unexploitable = run_backtest(snap, Oracle(snap.closes, offset=1), cost_bps=0)
    assert exploitable.returns["gross"].mean() > 0.02
    assert abs(unexploitable.returns["gross"].mean()) < 0.005


def test_lag_zero_rejected():
    with pytest.raises(ValueError, match="lag"):
        run_backtest(_synthetic(), QuintileLongShort(), lag=0)


class Constant(Strategy):
    name = "constant"

    def weights(self, view: SignalView) -> pd.Series:
        return pd.Series(0.5, index=["T00", "T01"])


def test_costs_charged_on_turnover_only():
    snap = _synthetic()
    res = run_backtest(snap, Constant(), cost_bps=100)
    r = res.returns
    assert r["turnover"].iloc[0] == pytest.approx(1.0)  # initial build: |0.5| + |0.5|
    assert (r["turnover"].iloc[1:] == 0).all()
    assert r["cost"].iloc[0] == pytest.approx(0.01)
    assert (r["cost"].iloc[1:] == 0).all()
    expected = 0.5 * snap.closes[["T00", "T01"]].pct_change(fill_method=None).loc[r.index].sum(
        axis=1
    )
    assert np.allclose(r["gross"].values, expected.values)


def test_universe_on_is_point_in_time():
    snap = _synthetic(delisted={"T00": "2026-09-10T21:00:00"}, added={"T01": "2026-09-15T00:00:00"})
    assert "T00" in snap.universe_on("2026-09-10") and "T00" not in snap.universe_on("2026-09-11")
    assert "T01" not in snap.universe_on("2026-09-14") and "T01" in snap.universe_on("2026-09-15")


def test_delisted_names_get_no_weight_after_last_trading_day():
    snap = _synthetic(delisted={"T00": "2026-09-10T21:00:00"})
    res = run_backtest(snap, Constant())
    w = res.weights
    assert (w.loc[w.index > pd.Timestamp("2026-09-11"), "T00"] == 0).all()


def test_gap_days_keep_previous_weights():
    snap = _synthetic(gaps=[{"start": "2026-09-08T00:00:00Z", "end": "2026-09-09T23:00:00Z"}])

    class DayDependent(Strategy):
        name = "day-dependent"
        calls: list = []

        def weights(self, view: SignalView) -> pd.Series:
            self.calls.append(view.date)
            return pd.Series(1.0, index=[f"T{view.date.day % N_TICKERS:02d}"])

    strat = DayDependent()
    run_backtest(snap, strat)
    assert pd.Timestamp("2026-09-08") not in strat.calls
    assert pd.Timestamp("2026-09-09") not in strat.calls
    assert pd.Timestamp("2026-09-10") in strat.calls


def test_view_never_shows_future_rows():
    snap = _synthetic()

    class Peek(Strategy):
        name = "peek"
        latest: list = []

        def weights(self, view: SignalView) -> pd.Series:
            self.latest.append((view.date, view.history["date"].max()))
            return pd.Series(dtype=float)

    strat = Peek()
    run_backtest(snap, strat)
    assert all(seen <= day for day, seen in strat.latest)


def test_example_strategies_and_report(tmp_path):
    snap = _synthetic()
    ls = run_backtest(snap, QuintileLongShort(feature="score_raw", min_names=10), cost_bps=15)
    assert ls.returns["gross_exposure"].iloc[-1] == pytest.approx(2.0)
    lo = run_backtest(snap, TopDecileLong(min_confidence=50))
    assert lo.returns["gross_exposure"].iloc[-1] == pytest.approx(1.0)
    out = report(ls, tmp_path)
    assert (out / "REPORT.md").exists() and (out / "summary.json").exists()


def test_summary_and_ic():
    r = pd.Series([0.01, -0.005, 0.02, 0.0, -0.01])
    s = summary(r)
    assert s["days"] == 5 and s["max_drawdown"] <= 0 and 0 <= s["hit_rate"] <= 1
    snap = _synthetic()
    ic = information_coefficient(snap.signals, snap.closes, "score_raw")
    assert ic["days"] > 0 and abs(ic["mean_ic"]) < 0.2  # random signal → ~0 IC


def test_snapshot_helpers():
    long = pd.DataFrame(
        {
            "ticker": ["A", "A", "A"],
            "date": ["2026-09-01"] * 3,
            "field": ["open", "close", "volume"],
            "value": [1.0, 2.0, 3.0],
        }
    )
    tidy = tidy_prices(long)
    assert tidy.iloc[0][["open", "close", "volume"]].tolist() == [1.0, 2.0, 3.0]
    m = build_manifest(
        pd.Timestamp("2026-05-21").date(), pd.Timestamp("2026-10-06").date(), {"x": 1}, [], "abc"
    )
    assert m["schema_version"] == 1 and m["window"]["end_exclusive"] == "2026-10-06"
    assert len(m["known_gaps"]) == 3


def test_live_only_drops_replayed_rows():
    both = _synthetic(replayed=("T00",))
    assert set(both.signals.loc[both.signals.ticker == "T00", "replay_run"]) == {"test-run"}
    assert both.signals.loc[both.signals.ticker != "T00", "replay_run"].isna().all()
    live = _synthetic(replayed=("T00",), include_replayed=False)
    assert "T00" not in set(live.signals.ticker)


def test_weekend_signal_feeds_monday_execution():
    snap = _synthetic()
    sun = pd.Timestamp("2026-09-06")  # Sunday; Monday 2026-09-07 is a trading day in the fixture
    extra = snap.signals[snap.signals["date"] == pd.Timestamp("2026-09-04")].copy()
    extra["date"] = sun
    snap.signals = pd.concat([snap.signals, extra], ignore_index=True)

    class Record(Strategy):
        name = "record"
        seen: dict = {}

        def weights(self, view: SignalView) -> pd.Series:
            self.seen[view.date] = True
            return pd.Series(1.0, index=["T00"])

    strat = Record()
    res = run_backtest(snap, strat)
    assert sun in strat.seen  # the Sunday row is used…
    assert pd.Timestamp("2026-09-04") not in strat.seen  # …instead of Friday's, for Monday's close
    assert pd.Timestamp("2026-09-07") in res.weights.index
