"""Tests for the replay scorer (Track A).

The permanent identity check, in synthetic form: a stored history generated
with the PRODUCTION pure functions (including production rounding, i.e. what
the orchestrator writes) must be reproduced EXACTLY by replaying the
production config. Plus config-surface behavior and point-in-time discipline.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from pipeline.scoring.composite import compute_composite, compute_exo_composite
from pipeline.scoring.divergence import compute_divergence
from pipeline.scoring.ema import compute_ema
from scripts.eval.replay import (
    PRODUCTION_CONFIG,
    load_config,
    replay_ticks,
    to_panel_input,
)


def _simulate_production(ticks: list[dict]) -> pd.DataFrame:
    """Replicate what the orchestrator stores, tick by tick (incl. rounding)."""
    rows = []
    prev_smoothed: dict[str, float] = {}
    prev_ts: dict[str, datetime] = {}
    for t in ticks:
        subs = {ly: t.get(ly) for ly in ("market", "narrative", "influencer", "macro")}
        comp = compute_composite(subs)
        present = {k: v for k, v in subs.items() if v is not None}
        _, effective = compute_divergence(present, comp.score)
        tk, ts = t["ticker"], t["ts"]
        dt_h = (
            (ts - prev_ts[tk]).total_seconds() / 3600.0 if tk in prev_ts else 0.0
        )
        smoothed = round(
            compute_ema(effective, prev_smoothed.get(tk), dt_h), 2
        )
        prev_smoothed[tk] = smoothed  # production reads back the rounded value
        prev_ts[tk] = ts
        exo = compute_exo_composite(subs)
        rows.append({
            "ticker": tk,
            "timestamp": ts,
            "composite_score": round(effective, 2),
            "composite_score_smoothed": smoothed,
            "composite_score_exo": round(exo.score, 2) if exo else None,
            "market_index": subs["market"],
            "narrative_index": subs["narrative"],
            "influencer_index": subs["influencer"],
            "macro_index": subs["macro"],
        })
    return pd.DataFrame(rows)


def _ticks(n=40, tickers=("AAPL", "MSFT")):
    base = datetime(2026, 5, 1, 14, 30, tzinfo=timezone.utc)
    out = []
    for i in range(n):
        for j, tk in enumerate(tickers):
            if i % 7 == 0:
                # cap-biting tick: composite ≈ 85 > 75, one layer < 30
                mkt, narr, infl, mac = 92.0, 95.0, 90.0, 25.0
            else:
                mkt = 50.0 + 10 * ((i + j) % 5) - 20            # 30..70
                narr = 55.0
                infl = None if i % 3 == 0 else 60.0             # missing-layer path
                mac = 45.0
            out.append({
                "ticker": tk,
                "ts": base + timedelta(minutes=30 * i),
                "market": mkt, "narrative": narr,
                "influencer": infl, "macro": mac,
            })
    return out


class TestIdentitySynthetic:
    def test_production_config_reproduces_stored_series_exactly(self):
        stored = _simulate_production(_ticks())
        replayed = replay_ticks(stored, PRODUCTION_CONFIG)
        m = stored.merge(replayed, on=["ticker", "timestamp"])
        assert (m["replay_raw"] - m["composite_score"]).abs().max() == 0.0
        assert (m["replay_smoothed"] - m["composite_score_smoothed"]).abs().max() == 0.0

    def test_exo_config_reproduces_stored_exo(self):
        stored = _simulate_production(_ticks())
        cfg = {"name": "exo", "layers": {"narrative": 0.30, "influencer": 0.25,
                                         "macro": 0.10},
               "ema_half_life_hours": None, "divergence_cap": False}
        replayed = replay_ticks(stored, cfg)
        m = stored.merge(replayed, on=["ticker", "timestamp"])
        m = m[m["composite_score_exo"].notna()]
        assert (m["replay_raw"] - m["composite_score_exo"]).abs().max() == 0.0


class TestConfigSurface:
    def test_no_ema_means_smoothed_equals_raw(self):
        stored = _simulate_production(_ticks())
        cfg = dict(PRODUCTION_CONFIG, ema_half_life_hours=None)
        r = replay_ticks(stored, cfg)
        assert (r["replay_raw"] == r["replay_smoothed"]).all()

    def test_shorter_half_life_tracks_raw_more_closely(self):
        stored = _simulate_production(_ticks(n=60))
        gaps = {}
        for hl in (1.0, 4.0):
            r = replay_ticks(stored, dict(PRODUCTION_CONFIG, ema_half_life_hours=hl))
            gaps[hl] = (r["replay_smoothed"] - r["replay_raw"]).abs().mean()
        assert gaps[1.0] < gaps[4.0]

    def test_divergence_cap_toggle(self):
        # A tick with narrative 88 / macro 25 should be capped at 75 when on
        stored = _simulate_production(_ticks())
        on = replay_ticks(stored, dict(PRODUCTION_CONFIG, divergence_cap=True))
        off = replay_ticks(stored, dict(PRODUCTION_CONFIG, divergence_cap=False))
        assert on["replay_raw"].max() <= 75.0 or (on["replay_raw"] == off["replay_raw"]).all()
        assert (off["replay_raw"] >= on["replay_raw"] - 1e-9).all()
        assert (off["replay_raw"] > on["replay_raw"]).any()  # the cap actually bit

    def test_all_layers_missing_gives_neutral_50(self):
        stored = pd.DataFrame([{
            "ticker": "AAPL",
            "timestamp": datetime(2026, 5, 1, tzinfo=timezone.utc),
            "market_index": None, "narrative_index": None,
            "influencer_index": None, "macro_index": None,
        }])
        r = replay_ticks(stored, PRODUCTION_CONFIG)
        assert r["replay_raw"].iloc[0] == 50.0

    def test_unknown_config_key_rejected(self, tmp_path):
        p = tmp_path / "bad.json"
        p.write_text('{"layers": {"market": 1.0}, "zscore_window": 90}')
        with pytest.raises(SystemExit):
            load_config(str(p))

    def test_unknown_layer_rejected(self, tmp_path):
        p = tmp_path / "bad.json"
        p.write_text('{"layers": {"vibes": 1.0}}')
        with pytest.raises(SystemExit):
            load_config(str(p))


class TestPointInTime:
    def test_appending_future_ticks_never_changes_past_values(self):
        ticks = _ticks(n=30, tickers=("AAPL",))
        stored_short = _simulate_production(ticks[:20])
        stored_full = _simulate_production(ticks)
        r_short = replay_ticks(stored_short, PRODUCTION_CONFIG)
        r_full = replay_ticks(stored_full, PRODUCTION_CONFIG)
        m = r_short.merge(r_full, on=["ticker", "timestamp"], suffixes=("_a", "_b"))
        assert (m["replay_raw_a"] == m["replay_raw_b"]).all()
        assert (m["replay_smoothed_a"] == m["replay_smoothed_b"]).all()


class TestPanelInput:
    def test_to_panel_input_feeds_prepare_daily_unchanged(self):
        from scripts.eval import analyze

        stored = _simulate_production(_ticks())
        stored["confidence_score"] = 80
        replayed = replay_ticks(stored, dict(PRODUCTION_CONFIG, ema_half_life_hours=1.0))
        panel_in = to_panel_input(stored, replayed)
        s = analyze.prepare_daily(panel_in)
        assert {"score", "score_raw"} <= set(s.columns)
        # the harness's smoothed score IS the replayed series (last tick per day)
        last = replayed.sort_values("timestamp").groupby("ticker").last()
        got = s.sort_values("date").groupby("ticker").last()
        for tk in got.index:
            assert got.loc[tk, "score"] == last.loc[tk, "replay_smoothed"]
