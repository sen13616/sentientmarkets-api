"""
tests/test_backfill_replay.py

News backfill + re-score after the 2026-08-10 → 10-02 outage:
  - as-of cutoff on scoring-path queries (scripts/db/queries/as_of.py)
  - compute_scored_state / _score_and_write split (live wrapper unchanged)
  - replay writer: sentiment_history only, tagged replay_run
  - windowed AV/Finnhub fetchers + the AV Note/Information warning
  - pure helpers: cluster_members, surprise_from_rows, and the
    rebuild_narrative in-memory selection / driver merge / staleness / EMA chain
  - news_backfill AV page bisection
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

T = datetime(2026, 9, 20, 15, 0, tzinfo=timezone.utc)


def _mock_pool_with(conn):
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    pool = MagicMock()
    pool.acquire.return_value = ctx
    return pool


def _sql(mock_call) -> str:
    return " ".join(mock_call.args[0].split())


# ---------------------------------------------------------------------------
# As-of cutoff
# ---------------------------------------------------------------------------


class TestAsOfCutoff:
    async def _signals_since(self, signal_types):
        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])
        with patch(
            "scripts.db.queries.raw_signals.get_pool",
            new=AsyncMock(return_value=_mock_pool_with(conn)),
        ):
            from scripts.db.queries.raw_signals import get_signals_since

            await get_signals_since("AAPL", T - timedelta(hours=1), signal_types)
        return conn.fetch.call_args

    async def test_live_signals_query_has_no_upper_bound(self):
        call = await self._signals_since(["rsi_14"])
        assert "timestamp <=" not in _sql(call)
        assert call.args[1:] == ("AAPL", T - timedelta(hours=1), ["rsi_14"])

    async def test_signals_query_bounded_with_types(self):
        from scripts.db.queries.as_of import scoring_as_of

        with scoring_as_of(T):
            call = await self._signals_since(["rsi_14"])
        assert "AND signal_type = ANY($3::text[]) AND timestamp <= $4" in _sql(call)
        assert call.args[-1] == T

    async def test_signals_query_bounded_without_types(self):
        from scripts.db.queries.as_of import scoring_as_of

        with scoring_as_of(T):
            call = await self._signals_since(None)
        assert "AND timestamp <= $3" in _sql(call)
        assert "signal_type" not in _sql(call).split("WHERE")[1]
        assert call.args[-1] == T

    async def test_signal_history_and_latest_close_bounded(self):
        from scripts.db.queries.as_of import scoring_as_of

        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])
        conn.fetchval = AsyncMock(return_value=None)
        with patch(
            "scripts.db.queries.raw_signals.get_pool",
            new=AsyncMock(return_value=_mock_pool_with(conn)),
        ):
            from scripts.db.queries.raw_signals import get_latest_close, get_signal_history

            await get_signal_history("AAPL", "rsi_14", limit=20)
            await get_latest_close("AAPL")
            assert "timestamp <=" not in _sql(conn.fetch.call_args)
            assert "timestamp <=" not in _sql(conn.fetchval.call_args)
            with scoring_as_of(T):
                await get_signal_history("AAPL", "rsi_14", limit=20)
                await get_latest_close("AAPL")
        assert "AND timestamp <= $4" in _sql(conn.fetch.call_args)
        assert conn.fetch.call_args.args[-1] == T
        assert "AND timestamp <= $2" in _sql(conn.fetchval.call_args)

    async def test_articles_since_bounded(self):
        from scripts.db.queries.as_of import scoring_as_of

        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])
        with patch(
            "scripts.db.queries.raw_articles.get_pool",
            new=AsyncMock(return_value=_mock_pool_with(conn)),
        ):
            from scripts.db.queries.raw_articles import get_articles_since

            await get_articles_since("AAPL", T - timedelta(days=3))
            assert "published_at <=" not in _sql(conn.fetch.call_args)
            with scoring_as_of(T):
                await get_articles_since("AAPL", T - timedelta(days=3))
        assert "AND published_at <= $3" in _sql(conn.fetch.call_args)
        assert conn.fetch.call_args.args[-1] == T

    async def test_baseline_scores_measured_from_cutoff(self):
        from scripts.db.queries.as_of import scoring_as_of

        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])
        with patch(
            "scripts.db.queries.sentiment_history.get_pool",
            new=AsyncMock(return_value=_mock_pool_with(conn)),
        ):
            from scripts.db.queries.sentiment_history import get_baseline_scores

            await get_baseline_scores()
            assert conn.fetch.call_args.args[-1] is None  # COALESCE($3, NOW()) → NOW()
            with scoring_as_of(T):
                await get_baseline_scores()
        assert "COALESCE($3, NOW())" in _sql(conn.fetch.call_args)
        assert conn.fetch.call_args.args[-1] == T

    async def test_cutoff_propagates_into_tasks_and_resets(self):
        import asyncio

        from scripts.db.queries.as_of import cutoff, scoring_as_of

        async def read():
            return cutoff()

        with scoring_as_of(T):
            seen = await asyncio.gather(*[read() for _ in range(3)])
        assert seen == [T, T, T]
        assert cutoff() is None


# ---------------------------------------------------------------------------
# Orchestrator split + replay writer
# ---------------------------------------------------------------------------


async def test_score_and_write_wraps_compute_with_wall_clock_and_redis_state():
    import pipeline.orchestrator as orch

    state = {"ticker": "AAPL"}
    result = MagicMock()
    last = {"timestamp": T}
    with (
        patch.object(orch, "read_scored_state", new=AsyncMock(return_value=last)),
        patch.object(
            orch, "compute_scored_state", new=AsyncMock(return_value=(state, result))
        ) as compute,
        patch.object(orch, "write_scored_state", new=AsyncMock()) as write_redis,
        patch.object(orch, "persist_scored_state", new=AsyncMock()) as write_pg,
    ):
        out = await orch._score_and_write("aapl", "Information Technology", 50.0)

    assert out is result
    args, kwargs = compute.call_args
    assert args == ("AAPL", "Information Technology", 50.0)
    assert kwargs["last_state"] is last
    assert abs((kwargs["now"] - datetime.now(timezone.utc)).total_seconds()) < 5
    write_redis.assert_awaited_once_with("AAPL", state)
    write_pg.assert_awaited_once_with(state)


async def test_persist_replay_row_tags_run_and_skips_price_snapshots():
    import pipeline.persistence.pg_writer as pgw

    conn = MagicMock()
    pool = _mock_pool_with(conn)
    state = {
        "ticker": "AAPL",
        "timestamp": T,
        "composite_score_raw": 55.0,
        "composite_score_smoothed": 54.0,
        "price": {"close": 190.0},
    }
    with (
        patch.object(pgw, "get_pool", new=AsyncMock(return_value=pool)),
        patch.object(pgw, "sh_queries") as sh,
        patch.object(pgw, "ps_queries") as ps,
    ):
        sh.insert_row = AsyncMock()
        ps.insert_row = AsyncMock()
        await pgw.persist_replay_row(state, "run-x")

    kwargs = sh.insert_row.call_args.kwargs
    assert kwargs["replay_run"] == "run-x"
    assert kwargs["composite_score"] == 55.0 and kwargs["timestamp"] == T
    ps.insert_row.assert_not_called()


def test_replay_state_from_row_matches_last_state_shape():
    from scripts.backfill.replay_scores import state_from_row, ticks

    row = {
        "ticker": "AAPL",
        "timestamp": T,
        "composite_score": 51.0,
        "composite_score_smoothed": 50.5,
        "ema_obs_count": 7,
        "market_index": 60.0,
        "narrative_index": None,
        "influencer_index": 55.0,
        "macro_index": 40.0,
        "market_as_of": T,
        "narrative_as_of": None,
        "influencer_as_of": T,
        "macro_as_of": T,
    }
    st = state_from_row(row)
    assert st["composite_score_smoothed"] == 50.5 and st["ema_obs_count"] == 7
    assert st["sub_indices"]["market"]["value"] == 60.0
    assert st["sub_indices"]["narrative"] is None
    assert st["freshness"]["macro_as_of"] == T
    assert ticks(T, T + timedelta(hours=1), timedelta(minutes=30)) == [T, T + timedelta(minutes=30)]


# ---------------------------------------------------------------------------
# Fetchers
# ---------------------------------------------------------------------------


def _resp(body, status=200):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = body
    return r


class TestFetchers:
    async def test_av_window_params(self):
        import pipeline.sources.narrative as nar

        with patch.object(nar, "guarded_get", new=AsyncMock(return_value=_resp({"feed": []}))) as g:
            await nar.fetch_av_news_window(
                "AAPL",
                MagicMock(),
                time_from=datetime(2026, 8, 7, tzinfo=timezone.utc),
                time_to=datetime(2026, 8, 8, 12, 30, tzinfo=timezone.utc),
                limit=1000,
                delay=2.0,
            )
        params = g.call_args.kwargs["params"]
        assert params["time_from"] == "20260807T0000"
        assert params["time_to"] == "20260808T1230"
        assert params["limit"] == 1000
        assert g.call_args.kwargs["delay"] == 2.0

    async def test_av_live_defaults_unchanged(self):
        import pipeline.sources.narrative as nar

        with patch.object(nar, "guarded_get", new=AsyncMock(return_value=_resp({"feed": []}))) as g:
            assert await nar._fetch_av_news("AAPL", MagicMock()) == []
        params = g.call_args.kwargs["params"]
        assert params["limit"] == 50
        assert "time_from" not in params and "time_to" not in params

    async def test_av_information_raises_in_window_and_warns_live(self, caplog):
        import pipeline.sources.narrative as nar

        body = {"Information": "Invalid API key."}
        with patch.object(nar, "guarded_get", new=AsyncMock(return_value=_resp(body))):
            with pytest.raises(nar.AVLimitError, match="Invalid API key"):
                await nar.fetch_av_news_window("AAPL", MagicMock())
            with caplog.at_level(logging.WARNING, logger="pipeline.sources.narrative"):
                assert await nar._fetch_av_news("AAPL", MagicMock()) == []
        assert "Invalid API key" in caplog.text

    async def test_finnhub_window_failure_is_none_live_is_empty(self):
        import pipeline.sources.narrative as nar

        with patch.object(
            nar, "guarded_get", new=AsyncMock(return_value=_resp([], status=500))
        ) as g:
            assert (
                await nar.fetch_finnhub_news_window(
                    "AAPL", MagicMock(), date(2026, 8, 7), date(2026, 8, 13)
                )
                is None
            )
            assert await nar._fetch_finnhub_news("AAPL", MagicMock()) == []
        assert g.call_args_list[0].kwargs["params"]["from"] == "2026-08-07"
        assert g.call_args_list[0].kwargs["params"]["to"] == "2026-08-13"


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_cluster_members_time_window_and_similarity():
    from pipeline.nlp.dedup import cluster_members

    v1 = np.array([1.0, 0.0])
    v2 = np.array([0.0, 1.0])
    arts = [
        {"published_at": T},
        {"published_at": T + timedelta(hours=1)},
        {"published_at": T + timedelta(hours=10)},  # same text, outside window
        {"published_at": T + timedelta(hours=1)},
    ]  # different text
    clusters = cluster_members(arts, [v1, v1, v1, v2], window_hours=4.0)
    assert clusters == [[0, 1]]


async def test_surprise_from_rows_matches_compute_narrative_surprise():
    from pipeline.features import surprise

    def row(day, score):
        return {
            "published_at": T - timedelta(days=day),
            "finbert_score": score,
            "relevance_score": 1.0,
        }

    baseline = [row(d, 0.1 * (d % 3)) for d in range(2, 16)]
    current = [row(0.1, 0.9), row(0.2, 0.8)]

    async def fake_between(ticker, since, until):
        return current if until == T else baseline

    with patch.object(surprise, "get_article_scores_between", side_effect=fake_between):
        via_db = await surprise.compute_narrative_surprise("AAPL", T)
    assert via_db is not None
    assert surprise.surprise_from_rows(current, baseline) == via_db
    assert surprise.surprise_from_rows([], baseline) is None
    assert surprise.surprise_from_rows(current, baseline[:3]) is None


def _art(i, hours_ago, rel=0.9, score=0.5, cluster=None, source="alpha_vantage"):
    return {
        "id": i,
        "published_at": T - timedelta(hours=hours_ago),
        "finbert_score": score,
        "relevance_score": rel,
        "source": source,
        "finbert_pos": 0.6,
        "finbert_neg": 0.1,
        "finbert_neu": 0.3,
        "event_cluster_id": cluster,
    }


class TestRebuildHelpers:
    def test_select_articles_dedups_clusters_and_bounds(self):
        from scripts.backfill.rebuild_narrative import select_articles

        arts = sorted(
            [
                _art(1, 10, rel=0.7, cluster="c1"),
                _art(2, 9, rel=0.95, cluster="c1"),  # best of c1
                _art(3, 8, rel=None, cluster="c2"),
                _art(4, 7, rel=0.6, cluster="c2"),  # non-null beats NULL
                _art(5, 0),  # exactly at t
                _art(6, 80),  # outside 3 days
            ],
            key=lambda a: a["published_at"],
        )
        pub = [a["published_at"] for a in arts]
        got = select_articles(arts, pub, T - timedelta(days=3), T, include_until=True)
        assert [a["id"] for a in got] == [2, 4, 5]
        half_open = select_articles(arts, pub, T - timedelta(days=3), T, include_until=False)
        assert [a["id"] for a in half_open] == [2, 4]

    def test_merge_drivers_replaces_narrative_and_reranks(self):
        from scripts.backfill.rebuild_narrative import merge_drivers

        stored = [
            ["RSI", "bullish", 0.4, 0.9, "market"],
            ["News", "bearish", 0.9, 0.9, "narrative"],
            ["VIX", "bearish", 0.2, 1.0, "macro"],
        ]
        new = [
            {
                "signal": "News",
                "direction": "bullish",
                "magnitude": 0.8,
                "confidence": 0.5,
                "source_layer": "narrative",
                "description": None,
            }
        ]
        got = merge_drivers(stored, new)
        # importance = magnitude × confidence: News 0.40 > RSI 0.36 > VIX 0.20;
        # the stale bearish News driver is gone.
        assert [d["signal"] for d in got] == ["News", "RSI", "VIX"]
        assert got[0]["direction"] == "bullish"
        assert merge_drivers(stored, new, top_n=2)[-1]["signal"] == "RSI"

    def test_rebuilt_stale_sources(self):
        from scripts.backfill.rebuild_narrative import rebuilt_stale_sources

        flags = ["missing_layer:narrative", "stale:news", "stale:insider", "low_signal_volume"]
        assert rebuilt_stale_sources(flags, T - timedelta(hours=1), T) == ["insider"]
        assert rebuilt_stale_sources(flags, None, T) == ["news", "insider"]
        assert rebuilt_stale_sources(["stale:market"], T - timedelta(hours=7), T) == [
            "market",
            "news",
        ]

    def test_rebuild_rows_adds_narrative_chains_ema_and_falls_back(self):
        from pipeline.scoring.ema import compute_ema
        from scripts.backfill.rebuild_narrative import RUN_ID, rebuild_rows

        def row(i, at):
            return {
                "id": i,
                "timestamp": at,
                "composite_score": 55.0,
                "market_index": 60.0,
                "narrative_index": None,
                "influencer_index": 55.0,
                "macro_index": 40.0,
                "confidence_flags": json.dumps(["missing_layer:narrative", "stale:news"]),
                "top_drivers": json.dumps([]),
                "divergence": "aligned",
                "narrative_as_of": None,
                "composite_score_smoothed": 55.0,
                "replay_run": None,
            }

        rows = [row(1, T), row(2, T + timedelta(hours=4)), row(3, T + timedelta(days=5))]
        seed = {
            "timestamp": T - timedelta(minutes=30),
            "composite_score_smoothed": 50.0,
            "narrative_index": None,
            "narrative_as_of": None,
        }
        arts = [_art(10 + k, 2 + k, score=0.8) for k in range(6)]

        out = rebuild_rows("AAPL", rows, seed, arts)
        assert [u[0] for u in out] == [1, 2, 3]
        assert all(u[-1] == RUN_ID for u in out)
        assert out[0][2] is not None and out[0][2] > 50  # bullish news present
        assert "missing_layer:narrative" not in json.loads(out[0][6])
        assert out[0][3] == round(compute_ema(out[0][1], 50.0, 0.5), 2)
        assert out[1][3] == round(compute_ema(out[1][1], out[0][3], 4.0), 2)
        # 5 days later every article is outside the 3-day lookback and the
        # previous narrative is older than the 6h fallback → layer missing.
        assert out[2][2] is None
        assert "missing_layer:narrative" in json.loads(out[2][6])


# ---------------------------------------------------------------------------
# news_backfill: AV page bisection
# ---------------------------------------------------------------------------


async def test_av_window_bisects_full_pages():
    import scripts.backfill.news_backfill as nb

    start = datetime(2026, 8, 7, tzinfo=timezone.utc)
    end = start + timedelta(days=4)
    calls = []

    async def fake(ticker, client, *, time_from, time_to, limit, delay):
        calls.append((time_from, time_to))
        full = (time_to - time_from) > timedelta(days=2)
        return [{"content_hash": f"{time_from}-{i}"} for i in range(limit if full else 3)]

    stats = dict.fromkeys(["av_calls", "av_max_page", "failed_windows"], 0)
    with patch.object(nb, "fetch_av_news_window", side_effect=fake):
        arts = await nb._av_window("AAPL", MagicMock(), start, end, stats)
    assert calls[0] == (start, end)
    assert calls[1:] == [(start, start + timedelta(days=2)), (start + timedelta(days=2), end)]
    assert len(arts) == 6 and stats["av_max_page"] == nb.AV_PAGE_LIMIT


async def test_av_window_aborts_on_invalid_key():
    import scripts.backfill.news_backfill as nb
    from pipeline.sources.narrative import AVLimitError

    stats = dict.fromkeys(["av_calls", "av_max_page", "failed_windows"], 0)
    with patch.object(
        nb, "fetch_av_news_window", side_effect=AVLimitError("the parameter apikey is invalid")
    ):
        with pytest.raises(nb.AbortBackfill):
            await nb._av_window("AAPL", MagicMock(), T, T + timedelta(days=1), stats)
