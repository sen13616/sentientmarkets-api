"""
tests/test_run_records.py

Run records behind GET /health/pipeline (pipeline/scheduler.py):
  - pipeline:last_run:{job} is written only after a SUCCESSFUL run
  - pipeline:last_start:{job} is written when a run starts
  - per-ticker jobs store {tickers_ok, tickers_total} with the success record
  - every scoring tick writes the pipeline:last_tick summary
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pipeline.scheduler as sched
from pipeline.orchestrator import ScoreResult


def _redis_capture() -> tuple[MagicMock, dict]:
    """Redis mock whose pipeline()/set() writes land in the returned dict."""
    written: dict[str, str] = {}

    def _set(key, value, ex=None):
        written[key] = value

    pipe = MagicMock()
    pipe.set = MagicMock(side_effect=_set)
    pipe.execute = AsyncMock()
    client = MagicMock()
    client.pipeline = MagicMock(return_value=pipe)
    client.set = AsyncMock(side_effect=_set)
    return client, written


# ---------------------------------------------------------------------------
# _record_run / _record_start / _with_timeout
# ---------------------------------------------------------------------------


async def test_record_run_stores_counts():
    client, written = _redis_capture()
    with patch.object(sched, "get_redis", return_value=client):
        await sched._record_run("market", tickers_ok=581, tickers_total=586)
    assert "pipeline:last_run:market" in written
    assert json.loads(written["pipeline:last_run_counts:market"]) == {
        "tickers_ok": 581,
        "tickers_total": 586,
    }


async def test_record_run_without_counts_writes_timestamp_only():
    client, written = _redis_capture()
    with patch.object(sched, "get_redis", return_value=client):
        await sched._record_run("macro_daily")
    assert set(written) == {"pipeline:last_run:macro_daily"}


async def test_with_timeout_records_start_before_running():
    client, written = _redis_capture()
    seen_start: list[bool] = []

    async def job() -> None:
        seen_start.append("pipeline:last_start:quick" in written)

    with (
        patch.object(sched, "get_redis", return_value=client),
        patch.dict(sched.JOB_TIMEOUTS_S, {"quick": 5}),
    ):
        await sched._with_timeout("quick", job)()
    assert seen_start == [True]
    assert "pipeline:last_run:quick" not in written  # wrapper never records success


async def test_timed_out_job_records_start_but_not_success():
    import asyncio

    client, written = _redis_capture()

    async def hung() -> None:
        await asyncio.Event().wait()
        await sched._record_run("hung")

    with (
        patch.object(sched, "get_redis", return_value=client),
        patch.dict(sched.JOB_TIMEOUTS_S, {"hung": 0.05}),
    ):
        await sched._with_timeout("hung", hung)()
    assert "pipeline:last_start:hung" in written
    assert "pipeline:last_run:hung" not in written


# ---------------------------------------------------------------------------
# Per-job success conditions
# ---------------------------------------------------------------------------


async def test_fetch_all_tickers_counts_successes():
    async def fetcher(ticker, client):
        if ticker == "BAD":
            raise RuntimeError("boom")

    ok, total = await sched._fetch_all_tickers(fetcher, ["A", "BAD", "C"], MagicMock())
    assert (ok, total) == (2, 3)


async def _run_market_job(fetch_result: tuple[int, int]) -> AsyncMock:
    with (
        patch.object(sched, "get_active_tickers", AsyncMock(return_value=["A", "B", "C"])),
        patch.object(sched, "_yf_batch_download", AsyncMock(return_value={})),
        patch.object(sched, "_fetch_all_tickers", AsyncMock(return_value=fetch_result)),
        patch.object(sched, "_record_run", new_callable=AsyncMock) as record,
    ):
        await sched.market_job()
    return record


async def test_market_job_records_success_with_counts():
    record = await _run_market_job((2, 3))
    record.assert_awaited_once_with("market", tickers_ok=2, tickers_total=3)


async def test_market_job_all_failed_not_recorded():
    record = await _run_market_job((0, 3))
    record.assert_not_awaited()


async def test_market_eod_job_records_counts():
    with (
        patch.object(sched, "get_active_tickers", AsyncMock(return_value=["A", "B"])),
        patch.object(sched, "_yf_batch_download", AsyncMock(return_value={})),
        patch.object(sched, "_fetch_all_tickers", AsyncMock(return_value=(2, 2))),
        patch.object(sched, "_record_run", new_callable=AsyncMock) as record,
    ):
        await sched.market_eod_job()
    record.assert_awaited_once_with("market_eod", tickers_ok=2, tickers_total=2)


async def test_influencer_job_all_failed_not_recorded():
    with (
        patch.object(sched, "get_active_tickers", AsyncMock(return_value=["A"])),
        patch.object(sched, "_fetch_all_tickers", AsyncMock(return_value=(0, 1))),
        patch.object(sched, "_record_run", new_callable=AsyncMock) as record,
    ):
        await sched.influencer_job()
    record.assert_not_awaited()


def _narrative_patches(fetch_result, unscored):
    return (
        patch.object(sched, "get_active_tickers", AsyncMock(return_value=["A", "B"])),
        patch.object(sched, "_fetch_all_tickers", AsyncMock(return_value=fetch_result)),
        patch.object(sched, "cluster_articles", AsyncMock(return_value=0)),
        patch.object(
            sched,
            "_get_cluster_telemetry",
            AsyncMock(
                return_value={
                    "cross_source_clusters": 0,
                    "same_source_clusters": 0,
                    "largest_cluster_size": 0,
                }
            ),
        ),
        patch(
            "scripts.db.queries.raw_articles.count_unclustered_articles",
            AsyncMock(return_value=0),
        ),
        patch("scripts.db.queries.raw_articles.get_unscored_articles", unscored),
    )


async def test_narrative_job_records_success_with_counts():
    p = _narrative_patches((2, 2), AsyncMock(return_value=[]))
    with p[0], p[1], p[2], p[3], p[4], p[5]:
        with patch.object(sched, "_record_run", new_callable=AsyncMock) as record:
            await sched.narrative_job()
    record.assert_awaited_once_with("narrative", tickers_ok=2, tickers_total=2)


async def test_narrative_job_finbert_failure_not_recorded():
    p = _narrative_patches((2, 2), AsyncMock(side_effect=RuntimeError("db gone")))
    with p[0], p[1], p[2], p[3], p[4], p[5]:
        with patch.object(sched, "_record_run", new_callable=AsyncMock) as record:
            await sched.narrative_job()
    record.assert_not_awaited()


async def test_narrative_job_all_fetches_failed_not_recorded():
    p = _narrative_patches((0, 2), AsyncMock(return_value=[]))
    with p[0], p[1], p[2], p[3], p[4], p[5]:
        with patch.object(sched, "_record_run", new_callable=AsyncMock) as record:
            await sched.narrative_job()
    record.assert_not_awaited()


async def test_macro_daily_failure_not_recorded():
    with (
        patch.object(sched, "fetch_fred_signals", AsyncMock(side_effect=RuntimeError("x"))),
        patch.object(sched, "_record_run", new_callable=AsyncMock) as record,
    ):
        await sched.macro_daily_job()
    record.assert_not_awaited()


async def test_macro_daily_success_recorded():
    with (
        patch.object(sched, "fetch_fred_signals", AsyncMock(return_value=3)),
        patch.object(sched, "_record_run", new_callable=AsyncMock) as record,
    ):
        await sched.macro_daily_job()
    record.assert_awaited_once_with("macro_daily")


async def test_macro_intraday_failure_not_recorded():
    with (
        patch.object(sched, "fetch_macro_signals", AsyncMock(side_effect=RuntimeError("x"))),
        patch.object(sched, "_record_run", new_callable=AsyncMock) as record,
    ):
        await sched.macro_intraday_job()
    record.assert_not_awaited()


async def test_short_volume_zero_tickers_not_recorded():
    with (
        patch.object(sched, "ingest_short_volume", AsyncMock(return_value=0)),
        patch.object(sched, "_record_run", new_callable=AsyncMock) as record,
    ):
        await sched.short_volume_job()
    record.assert_not_awaited()


async def test_short_volume_success_records_counts():
    with (
        patch.object(sched, "ingest_short_volume", AsyncMock(return_value=580)),
        patch.object(sched, "get_active_tickers", AsyncMock(return_value=["T"] * 586)),
        patch.object(sched, "_record_run", new_callable=AsyncMock) as record,
    ):
        await sched.short_volume_job()
    record.assert_awaited_once_with("short_volume", tickers_ok=580, tickers_total=586)


async def _run_scoring_tick(fetched: int) -> AsyncMock:
    with (
        patch.object(sched, "get_active_tickers", AsyncMock(return_value=["A", "B"])),
        patch.object(sched, "get_ticker_sector_map", AsyncMock(return_value={})),
        patch.object(sched, "_score_all", AsyncMock(return_value=(fetched, fetched * 4))),
        patch.object(sched, "_record_run", new_callable=AsyncMock) as record,
    ):
        await sched.scoring_tick_job()
    return record


async def test_scoring_tick_zero_scored_not_recorded():
    record = await _run_scoring_tick(0)
    record.assert_not_awaited()


async def test_scoring_tick_success_recorded():
    record = await _run_scoring_tick(2)
    record.assert_awaited_once_with("scoring_tick")


# ---------------------------------------------------------------------------
# Tick summary
# ---------------------------------------------------------------------------


def _result(narrative: bool) -> ScoreResult:
    return ScoreResult(4 if narrative else 3, 55.0, None, None, 55.0, 50.0, narrative)


async def test_score_all_writes_tick_summary():
    client, written = _redis_capture()
    results = {"AAA": _result(True), "BBB": _result(False), "CCC": _result(True)}

    async def _fake_score(ticker, sector, baseline):
        if ticker == "DDD":
            raise RuntimeError("scoring failed")
        return results[ticker]

    with (
        patch.object(sched, "_score_and_write", side_effect=_fake_score),
        patch.object(sched, "get_baseline_scores", AsyncMock(return_value={})),
        patch.object(sched, "_publish_universe_stats", new_callable=AsyncMock),
        patch.object(sched, "get_redis", return_value=client),
    ):
        await sched._score_all(["AAA", "BBB", "CCC", "DDD"], "TEST")

    summary = json.loads(written[sched.TICK_SUMMARY_KEY])
    assert summary["tickers_scored"] == 3
    assert summary["active_universe"] == 4
    assert summary["missing_narrative"] == 1
    assert "at" in summary


async def test_tick_summary_written_even_when_nothing_scored():
    client, written = _redis_capture()
    with patch.object(sched, "get_redis", return_value=client):
        await sched._publish_tick_summary({}, n_active=586)
    summary = json.loads(written[sched.TICK_SUMMARY_KEY])
    assert (summary["tickers_scored"], summary["active_universe"]) == (0, 586)


async def test_tick_summary_redis_failure_is_swallowed():
    with patch.object(sched, "get_redis", side_effect=RuntimeError("no redis")):
        await sched._publish_tick_summary({"A": _result(True)}, n_active=1)
