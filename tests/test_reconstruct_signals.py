"""
tests/test_reconstruct_signals.py

June-gap fill support:
  - scripts/backfill/reconstruct_signals.py: hourly partial bars, mark
    schedule, per-mark signals equal the live pure functions
  - replay_scores.py settings flags (run id, EMA half-life, research flags)
  - news_backfill.py repair of stored-but-unscored articles
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from pipeline.sources.market import (
    _compute_order_flow,
    _compute_returns,
    _compute_rsi,
    _compute_volume_ratio,
)
from scripts.backfill import reconstruct_signals as rs

UTC = timezone.utc
DAY = date(2026, 6, 24)   # Wednesday, EDT: session 13:30–20:00 UTC


def _hbar(h, m, close, vol=100.0, hi=None, lo=None):
    start = datetime(2026, 6, 24, h, m, tzinfo=UTC)
    return {"start": start, "open": close - 0.5, "high": hi or close + 1, "low": lo or close - 1,
            "close": close, "volume": vol}


HOURLY = [_hbar(13, 30, 100), _hbar(14, 30, 101), _hbar(15, 30, 102, hi=110),
          _hbar(16, 30, 103), _hbar(17, 30, 104), _hbar(18, 30, 105), _hbar(19, 30, 106, lo=90)]


class TestBars:

    def test_last_hourly_bar_ends_at_the_close(self):
        assert rs.hourly_bar_end(HOURLY[0]["start"]) == datetime(2026, 6, 24, 14, 30, tzinfo=UTC)
        assert rs.hourly_bar_end(HOURLY[-1]["start"]) == datetime(2026, 6, 24, 20, 0, tzinfo=UTC)

    def test_partial_bar_uses_only_finished_bars(self):
        assert rs.partial_bar(HOURLY, datetime(2026, 6, 24, 14, 0, tzinfo=UTC)) is None
        # 16:00: the 13:30 and 14:30 bars have finished; 15:30 ends at 16:30.
        bar = rs.partial_bar(HOURLY, datetime(2026, 6, 24, 16, 0, tzinfo=UTC))
        assert bar == {"open": 99.5, "high": 102, "low": 99, "close": 101, "volume": 200.0}
        full = rs.partial_bar(HOURLY, datetime(2026, 6, 24, 20, 0, tzinfo=UTC))
        assert full["close"] == 106 and full["low"] == 90 and full["volume"] == 700.0

    def test_marks_follow_live_schedule(self):
        marks = rs.intraday_marks(DAY)
        assert [m.hour for m in marks] == list(range(14, 21))
        assert rs.intraday_marks(date(2026, 6, 27)) == []          # Saturday
        assert rs.eod_mark(DAY) == datetime(2026, 6, 24, 21, 15, tzinfo=UTC)


def test_market_rows_equal_live_pure_functions():
    prior = [(datetime(2026, 5, 1, tzinfo=UTC) + timedelta(days=i), 90.0 + i * 0.3) for i in range(60)]
    vols = [1000.0 + i for i in range(60)]
    bar = {"open": 100.0, "high": 109.0, "low": 99.0, "close": 107.0, "volume": 800.0}
    mark = datetime(2026, 6, 24, 16, tzinfo=UTC)

    got = {sig: v for _, sig, v, src, up, ts in rs.market_rows("AAPL", mark, bar, prior, vols)}
    want = dict(_compute_order_flow(bar))
    want["rsi_14"] = _compute_rsi([c for _, c in prior[-50:]] + [107.0])
    want.update(_compute_returns(107.0, prior[-50:]))
    want["volume_ratio"] = _compute_volume_ratio(800.0, vols[-20:])
    assert got == want
    row = rs.market_rows("AAPL", mark, bar, prior, vols)[0]
    assert row[3:] == ("computed", "manual_backfill", mark)


def test_build_ticker_rows_uses_prior_days_only_and_respects_range():
    daily = [{"start": datetime(2026, 6, d, tzinfo=UTC), "open": 1, "high": 2, "low": 0.5,
              "close": 90.0 + d, "volume": 1000.0} for d in (22, 23, 24)]
    start = datetime(2026, 6, 24, 15, tzinfo=UTC)
    end = datetime(2026, 6, 25, tzinfo=UTC)
    rows = rs.build_ticker_rows("AAPL", HOURLY, daily, start, end, eod=True)

    marks = sorted({r[5] for r in rows})
    assert marks[0] == start and marks[-1] == rs.eod_mark(DAY)
    ret_1d = {r[5]: r[2] for r in rows if r[1] == "return_1d"}
    # 16:00 partial close 101 vs Jun 23 final close 113 (never a same-day value)
    import math
    assert ret_1d[datetime(2026, 6, 24, 16, tzinfo=UTC)] == round(math.log(101 / 113.0), 6)
    # EOD uses the full daily bar (close 114)
    assert ret_1d[rs.eod_mark(DAY)] == round(math.log(114 / 113.0), 6)
    assert not rs.build_ticker_rows("AAPL", HOURLY, daily, start, end, eod=False)[-1][5].minute == 15


def test_build_macro_rows_vix_and_etf():
    vix = [{**b, "close": 15.0 + i} for i, b in enumerate(HOURLY)]
    etf_daily = {"XLK": [{"start": datetime(2026, 5, 1, tzinfo=UTC) + timedelta(days=i), "open": 1,
                          "high": 2, "low": 0.5, "close": 100.0 + i, "volume": 1.0} for i in range(30)]
                 + [{"start": datetime(2026, 6, 24, tzinfo=UTC), "open": 1, "high": 2, "low": 0.5,
                     "close": 200.0, "volume": 1.0}]}
    rows = rs.build_macro_rows(vix, {"XLK": HOURLY}, etf_daily,
                               datetime(2026, 6, 24, tzinfo=UTC), datetime(2026, 6, 25, tzinfo=UTC))
    vix_rows = {r[5].hour: r[2] for r in rows if r[1] == "vix"}
    assert vix_rows[16] == 16.0 and 14 not in vix_rows       # bars finished by 16:00: 13:30, 14:30
    etf = [r for r in rows if r[1] == "sector_etf_return_20d"]
    prior20 = 100.0 + 10            # 20th-from-last of the 30 prior closes
    # first mark with a finished bar is 15:00 (13:30 bar, close 100)
    assert etf[0][5].hour == 15 and etf[0][2] == round((100 - prior20) / prior20, 6)


# ---------------------------------------------------------------------------
# replay settings
# ---------------------------------------------------------------------------

def test_replay_settings_flags_set_env_and_import_leaves_env_alone(monkeypatch):
    for k in ("EMA_HALF_LIFE_HOURS", "ENABLE_NARRATIVE_SURPRISE", "ENABLE_POSITIONING_FEATURES"):
        monkeypatch.delenv(k, raising=False)
    import scripts.backfill.replay_scores as rp

    # Import must not apply scoring settings (.env may define the research
    # flags via load_dotenv, but EMA_HALF_LIFE_HOURS is only ever set here).
    assert "EMA_HALF_LIFE_HOURS" not in os.environ
    assert rp.RUN_ID == "news-backfill-2026-10"
    s = rp.apply_settings(["--run-id", "june-gap-2026-06", "--ema-half-life", "4",
                           "--no-surprise", "--no-positioning", "--start", "2026-06-23T15:00"])
    assert s.run_id == "june-gap-2026-06"
    assert os.environ["EMA_HALF_LIFE_HOURS"] == "4.0"
    assert os.environ["ENABLE_NARRATIVE_SURPRISE"] == "0"
    assert os.environ["ENABLE_POSITIONING_FEATURES"] == "0"


# ---------------------------------------------------------------------------
# news_backfill repair path
# ---------------------------------------------------------------------------

async def test_backfill_repairs_stored_unscored_articles_only():
    import scripts.backfill.news_backfill as nb

    t = datetime(2026, 6, 25, 12, tzinfo=UTC)
    def art(h, title):
        return {"ticker": "AAPL", "title": title, "summary": "earnings beat expectations strongly",
                "source": "finnhub", "source_url": f"u{h}", "published_at": t, "provider_sentiment": None,
                "relevance_score": 1.0, "content_hash": h}
    fetched = [art("new1", "Apple shares rise on strong iPhone demand"),
               art("unscored", "Apple beats quarterly revenue estimates easily"),
               art("scored", "Apple announces product event date today")]
    stored = {"unscored": {"id": 11, "scored": False, "clustered": False},
              "scored": {"id": 12, "scored": True, "clustered": True}}
    stats = dict.fromkeys(["new", "already_stored", "repairable", "repaired", "inserted",
                           "finbert_scored", "clustered", "failed_windows"], 0)

    def fake_scores(texts, batch_size=32):
        return [{"finbert_score": 0.5, "finbert_pos": 0.6, "finbert_neg": 0.1, "finbert_neu": 0.3}
                for _ in texts]

    with (
        patch.object(nb, "_fetch_ticker", new=AsyncMock(return_value=fetched)),
        patch.object(nb.ra, "existing_articles", new=AsyncMock(return_value=stored)),
        patch.object(nb.ra, "get_unclustered_articles_between", new=AsyncMock(return_value=[])),
        patch.object(nb.ra, "insert_scored_articles", new=AsyncMock()) as ins,
        patch.object(nb.ra, "update_finbert_scores", new=AsyncMock()) as upd,
        patch.object(nb.ra, "set_cluster_ids", new=AsyncMock()),
        patch("pipeline.nlp.finbert.score_batch", side_effect=fake_scores),
        patch("pipeline.nlp.dedup._get_model") as model,
    ):
        model.return_value.encode.side_effect = lambda titles, **k: [[1.0, 0.0]] * len(titles)
        await nb._backfill_ticker("AAPL", MagicMock(), t - timedelta(days=1), t + timedelta(days=1),
                                  dry_run=False, stats=stats)

    assert [a["content_hash"] for a in ins.call_args.args[0]] == ["new1"]
    assert upd.call_args.args[0] == [(11, 0.5, 0.6, 0.1, 0.3)]
    assert stats["repairable"] == 1 and stats["repaired"] == 1 and stats["already_stored"] == 2
