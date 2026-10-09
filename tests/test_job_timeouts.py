"""
tests/test_job_timeouts.py

Hang protection added after the 2026-08-10 / 2026-09-17 outages, where jobs
awaited dead DB connections forever and max_instances=1 then skipped every
later run:
  - app DB pool gets a per-query command_timeout
  - _score_all skips a ticker that exceeds SCORE_TICKER_TIMEOUT_S
  - every scheduled job is wrapped in a job-level timeout
  (GET /health/pipeline, which reports stale jobs, is tested in
  tests/test_pipeline_health.py)
"""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient

# ---------------------------------------------------------------------------
# DB pool command_timeout
# ---------------------------------------------------------------------------


async def test_init_pool_passes_command_timeout(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost/db")
    import scripts.db.connection as conn

    with (
        patch.object(conn, "_pool", None),
        patch.object(conn.asyncpg, "create_pool", new_callable=AsyncMock) as mock_create,
    ):
        await conn.init_pool(command_timeout=42)
    assert mock_create.call_args.kwargs["command_timeout"] == 42


async def test_get_pool_auto_init_has_no_command_timeout(monkeypatch):
    """Scripts auto-init via get_pool(); long eval/backfill queries must not time out."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost/db")
    import scripts.db.connection as conn

    with (
        patch.object(conn, "_pool", None),
        patch.object(conn.asyncpg, "create_pool", new_callable=AsyncMock) as mock_create,
    ):
        await conn.get_pool()
    assert mock_create.call_args.kwargs["command_timeout"] is None


# ---------------------------------------------------------------------------
# Per-ticker timeout in _score_all
# ---------------------------------------------------------------------------


async def test_score_all_skips_hung_ticker_and_completes(caplog):
    import pipeline.scheduler as sched

    async def _fake_score(ticker, sector, baseline):
        if ticker == "HANG":
            await asyncio.Event().wait()  # never returns
        return MagicMock(n_populated=4)

    with (
        patch.object(sched, "SCORE_TICKER_TIMEOUT_S", 0.05),
        patch.object(sched, "_score_and_write", side_effect=_fake_score),
        patch.object(sched, "get_baseline_scores", new_callable=AsyncMock, return_value={}),
        patch.object(sched, "_publish_universe_stats", new_callable=AsyncMock) as mock_publish,
        caplog.at_level(logging.ERROR, logger="pipeline.scheduler"),
    ):
        fetched, total_layers = await asyncio.wait_for(
            sched._score_all(["AAA", "HANG", "BBB"], "TEST"), timeout=5
        )

    assert (fetched, total_layers) == (2, 8)
    assert set(mock_publish.call_args.args[0]) == {"AAA", "BBB"}
    assert "scoring timed out for HANG" in caplog.text


# ---------------------------------------------------------------------------
# Job-level timeout wrapper
# ---------------------------------------------------------------------------


async def test_with_timeout_cancels_hung_job(caplog):
    import pipeline.scheduler as sched

    async def hung_job() -> None:
        await asyncio.Event().wait()

    with (
        patch.dict(sched.JOB_TIMEOUTS_S, {"hung": 0.05}),
        caplog.at_level(logging.ERROR, logger="pipeline.scheduler"),
    ):
        wrapped = sched._with_timeout("hung", hung_job)
        await asyncio.wait_for(wrapped(), timeout=5)  # returns instead of hanging

    assert "hung job exceeded" in caplog.text


async def test_with_timeout_runs_normal_job():
    import pipeline.scheduler as sched

    job = AsyncMock()
    job.__name__ = "quick_job"
    with patch.dict(sched.JOB_TIMEOUTS_S, {"quick": 5}):
        await sched._with_timeout("quick", job)()
    job.assert_awaited_once()


def test_every_registered_job_is_timeout_wrapped():
    import pipeline.scheduler as sched

    jobs = sched.scheduler.get_jobs()
    assert {j.id for j in jobs} == set(sched.JOB_TIMEOUTS_S)
    for j in jobs:
        # functools.wraps keeps the original name; the wrapper is a distinct object
        original = getattr(sched, j.func.__name__)
        assert j.func is not original, f"{j.id} is not timeout-wrapped"
        assert j.func.__wrapped__ is original


# ---------------------------------------------------------------------------
# /health stays independent of pipeline state
# ---------------------------------------------------------------------------


def test_plain_health_unaffected_by_stale_pipeline():
    """Railway gates deploys on /health — it must stay 200 regardless."""
    from main import app

    r = TestClient(app).get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}
