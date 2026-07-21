"""Unit tests for the eval harness (scripts/eval/) on synthetic frames.

These exercise the pure analysis functions only — no DB, no network.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scripts.eval import analyze, scorecard

# ---------------------------------------------------------------- fixtures --


def _sent_row(ticker, ts, raw, smoothed=None, market=None, narrative=None,
              influencer=None, macro=None, confidence=80):
    return {
        "ticker": ticker,
        "timestamp": pd.Timestamp(ts, tz="UTC"),
        "composite_score": raw,
        "composite_score_smoothed": smoothed,
        "market_index": market,
        "narrative_index": narrative,
        "influencer_index": influencer,
        "macro_index": macro,
        "confidence_score": confidence,
    }


def _closes(dates, tickers, base=100.0, drift=0.0):
    idx = pd.to_datetime(dates)
    data = {
        tk: base * np.exp(drift * np.arange(len(idx))) for tk in tickers
    }
    return pd.DataFrame(data, index=idx)


# ------------------------------------------------------------ prepare_daily --


class TestPrepareDaily:
    def test_exo_renormalized_weights_all_present(self):
        s = analyze.prepare_daily(pd.DataFrame([
            _sent_row("AAPL", "2026-01-05 21:00", 55, narrative=60,
                      influencer=70, macro=40),
        ]))
        expected = (0.30 * 60 + 0.25 * 70 + 0.10 * 40) / 0.65
        assert s["exo"].iloc[0] == pytest.approx(expected)

    def test_exo_renormalizes_over_present_layers(self):
        s = analyze.prepare_daily(pd.DataFrame([
            _sent_row("AAPL", "2026-01-05 21:00", 55, narrative=60,
                      influencer=70, macro=None),
        ]))
        expected = (0.30 * 60 + 0.25 * 70) / 0.55
        assert s["exo"].iloc[0] == pytest.approx(expected)

    def test_exo_none_when_all_exo_layers_missing(self):
        s = analyze.prepare_daily(pd.DataFrame([
            _sent_row("AAPL", "2026-01-05 21:00", 55, market=80),
        ]))
        assert pd.isna(s["exo"].iloc[0])

    def test_market_layer_never_enters_exo(self):
        with_mkt = analyze.prepare_daily(pd.DataFrame([
            _sent_row("AAPL", "2026-01-05 21:00", 55, market=99, narrative=60),
        ]))
        without_mkt = analyze.prepare_daily(pd.DataFrame([
            _sent_row("AAPL", "2026-01-05 21:00", 55, market=None, narrative=60),
        ]))
        assert with_mkt["exo"].iloc[0] == without_mkt["exo"].iloc[0] == 60

    def test_last_tick_per_et_day_wins(self):
        s = analyze.prepare_daily(pd.DataFrame([
            _sent_row("AAPL", "2026-01-05 15:00", 40),
            _sent_row("AAPL", "2026-01-05 21:00", 60),
        ]))
        assert len(s) == 1
        assert s["score_raw"].iloc[0] == 60

    def test_smoothed_falls_back_to_raw(self):
        s = analyze.prepare_daily(pd.DataFrame([
            _sent_row("AAPL", "2026-01-05 21:00", 42, smoothed=None),
        ]))
        assert s["score"].iloc[0] == 42


# -------------------------------------------------------------- build_panel --


class TestBuildPanel:
    def _one_signal_panel(self, rets_by_day):
        """One ticker; signal on Mon 2026-01-05; trading days Mon–Fri."""
        dates = pd.bdate_range("2026-01-05", periods=6)
        closes = pd.DataFrame(
            {"AAPL": 100 * np.exp(np.cumsum([0.0] + rets_by_day))},
            index=dates,
        )
        s = analyze.prepare_daily(pd.DataFrame([
            _sent_row("AAPL", "2026-01-05 21:00", 55, narrative=60),
        ]))
        return analyze.build_panel(s, closes, gaps=[])

    def test_t1_is_next_trading_day_strictly_after_signal(self):
        panel = self._one_signal_panel([0.01, 0.02, 0.03, 0.01, 0.01])
        assert panel["t1"].iloc[0] == pd.Timestamp("2026-01-06")

    def test_forward_return_excludes_entry_day(self):
        # signal Mon; enter Tue close; fwd_1 = Wed's log return only
        panel = self._one_signal_panel([0.01, 0.02, 0.03, 0.01, 0.01])
        assert panel["fwd_1"].iloc[0] == pytest.approx(0.02)
        assert panel["fwd_2"].iloc[0] == pytest.approx(0.02 + 0.03)

    def test_gap_window_dropped(self):
        dates = pd.bdate_range("2026-01-05", periods=6)
        closes = _closes(dates, ["AAPL"], drift=0.01)
        s = analyze.prepare_daily(pd.DataFrame([
            _sent_row("AAPL", "2026-01-05 21:00", 55, narrative=60),
        ]))
        gaps = [(pd.Timestamp("2026-01-07"), pd.Timestamp("2026-01-09"))]
        panel = analyze.build_panel(s, closes, gaps=gaps)
        assert pd.isna(panel["fwd_2"].iloc[0])  # window t1(Tue)->Thu spans gap

    def test_ticker_without_prices_skipped(self):
        dates = pd.bdate_range("2026-01-05", periods=6)
        closes = _closes(dates, ["AAPL"])
        s = analyze.prepare_daily(pd.DataFrame([
            _sent_row("ZZZZ", "2026-01-05 21:00", 55, narrative=60),
        ]))
        panel = analyze.build_panel(s, closes, gaps=[])
        assert panel.empty


# ----------------------------------------------------------------- ic_table --


class TestICTable:
    def test_perfect_monotone_signal_gives_ic_one(self):
        rng = np.random.default_rng(7)
        days = pd.bdate_range("2026-01-05", periods=10)
        rows = []
        for d in days:
            for i in range(30):
                sig = float(i) + rng.normal(0, 1e-9)
                rows.append({"date": d, "myfeat": sig, "fwd_1": sig * 2.0})
        panel = pd.DataFrame(rows)
        ic = analyze.ic_table(panel, feats=["myfeat"], horizons=[1])
        row = ic[(ic.feature == "myfeat") & (ic.target == "raw")].iloc[0]
        assert row["mean_IC"] == pytest.approx(1.0, abs=1e-6)
        assert row["hit_rate"] == 1.0

    def test_skips_features_below_min_rows(self):
        panel = pd.DataFrame(
            {"date": [pd.Timestamp("2026-01-05")] * 10,
             "f": range(10), "fwd_1": range(10)}
        )
        assert analyze.ic_table(panel, feats=["f"], horizons=[1]).empty


# ----------------------------------------------------------------- lead_lag --


class TestLeadLag:
    def test_signal_mirroring_yesterdays_return_peaks_at_minus_one(self):
        rng = np.random.default_rng(11)
        dates = pd.bdate_range("2026-01-05", periods=60)
        rets = rng.normal(0, 0.02, size=(len(dates), 8))
        closes = pd.DataFrame(
            100 * np.exp(np.cumsum(rets, axis=0)),
            index=dates, columns=[f"T{i}" for i in range(8)],
        )
        ret = analyze.daily_returns(closes)
        rows = []
        for tk in closes.columns:
            for d in dates[2:]:
                i = list(dates).index(d)
                rows.append({"ticker": tk, "date": d,
                             "sig": ret[tk].iloc[i - 1]})  # yesterday's return
        s = pd.DataFrame(rows)
        ll = analyze.lead_lag(s, closes, "sig", gaps=[], max_k=2, min_n=50)
        peak = ll.loc[ll["corr"].abs().idxmax()]
        assert peak["lag_k"] == -1
        assert peak["corr"] > 0.9


# ---------------------------------------------------------------- scorecard --


class TestScorecard:
    def _daily(self, raw_std):
        rng = np.random.default_rng(3)
        days = pd.bdate_range("2026-01-05", periods=5)
        rows = []
        for d in days:
            for i in range(20):
                raw = float(rng.normal(56, raw_std))
                rows.append({"ticker": f"T{i}", "date": d, "score_raw": raw,
                             "score": raw, "exo": raw, "market": 53.0,
                             "narrative": 59.0, "influencer": 56.0,
                             "macro": 58.0})
        return pd.DataFrame(rows)

    def _card(self, raw_std=8.0, peak_offset=0):
        ll = pd.DataFrame(
            {"lag_k": [-1, 0, 1],
             "corr": [0.1, 0.4, 0.0] if peak_offset == 0 else [0.4, 0.1, 0.0],
             "n": [1000] * 3}
        )
        return scorecard.build_scorecard(
            self._daily(raw_std), pd.DataFrame(), ll, ll,
            meta={"start": "a", "end": "b"},
        )

    def test_diff_passes_on_identical_cards(self):
        card = self._card()
        assert scorecard.diff(card, card) == []

    def test_diff_flags_dispersion_collapse(self):
        base, cur = self._card(raw_std=8.0), self._card(raw_std=4.0)
        msgs = scorecard.diff(base, cur)
        assert any("composite_raw.std" in m for m in msgs)

    def test_diff_flags_leadlag_peak_moving_into_past(self):
        base, cur = self._card(peak_offset=0), self._card(peak_offset=-1)
        msgs = scorecard.diff(base, cur)
        assert any("peak_offset" in m for m in msgs)

    def test_diff_ignores_metrics_missing_from_baseline(self):
        base, cur = self._card(), self._card()
        del base["leadlag"]["exo_change"]
        assert scorecard.diff(base, cur) == []

    def test_leadlag_summary_reports_peak_and_future_max(self):
        ll = pd.DataFrame({"lag_k": [-1, 0, 1, 2],
                           "corr": [0.34, 0.05, 0.01, -0.02],
                           "n": [900] * 4})
        s = scorecard._leadlag_summary(ll)
        assert s["peak_offset"] == -1
        assert s["max_future_corr"] == pytest.approx(0.01)
