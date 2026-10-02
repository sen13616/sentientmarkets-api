"""
tests/test_job_timeouts.py

Hang protection added after the 2026-08-10 / 2026-09-17 outages, where jobs
awaited dead DB connections forever and max_instances=1 then skipped every
later run:
  - app DB pool gets a per-query command_timeout
  - _score_all skips a ticker that exceeds SCORE_TICKER_TIMEOUT_S
  - every scheduled job is wrapped in a job-level timeout
  - GET /health/pipeline reports 503 when scoring/narrative go stale
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# DB pool command_timeout
# ---------------------------------------------------------------------------

async def test_init_pool_passes_command_timeout():
    import scripts.db.connection as conn

    with (
        patch.object(conn, "_pool", None),
        patch.object(conn.asyncpg, "create_pool", new_callable=AsyncMock) as mock_create,
    ):
        await conn.init_pool(command_timeout=42)
    assert mock_create.call_args.kwargs["command_timeout"] == 42


async def test_get_pool_auto_init_has_no_command_timeout():
    """Scripts auto-init via get_pool(); long eval/backfill queries must not time out."""
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
# GET /health/pipeline
# ---------------------------------------------------------------------------

def _redis_with(last_runs: dict[str, datetime | None]) -> MagicMock:
    async def _get(key: str):
        ts = last_runs.get(key.rsplit(":", 1)[-1])
        return ts.isoformat() if ts else None

    client = MagicMock()
    client.get = AsyncMock(side_effect=_get)
    return client


def _get_pipeline_health(redis_client) -> tuple[int, dict]:
    from main import app

    with patch("api.routes.health.get_redis", return_value=redis_client):
        r = TestClient(app).get("/health/pipeline")
    return r.status_code, r.json()


def test_health_pipeline_ok_when_fresh():
    now = datetime.now(timezone.utc)
    status, body = _get_pipeline_health(_redis_with({
        "scoring_tick": now - timedelta(minutes=10),
        "narrative": now - timedelta(minutes=40),
    }))
    assert status == 200
    assert body["status"] == "ok"
    assert body["checks"]["scoring_tick"]["ok"] is True


def test_health_pipeline_503_when_scoring_stale():
    now = datetime.now(timezone.utc)
    status, body = _get_pipeline_health(_redis_with({
        "scoring_tick": now - timedelta(minutes=90),
        "narrative": now - timedelta(minutes=10),
    }))
    assert status == 503
    assert body["status"] == "stale"
    assert body["checks"]["scoring_tick"]["ok"] is False
    assert body["checks"]["narrative"]["ok"] is True


def test_health_pipeline_503_when_never_run():
    status, body = _get_pipeline_health(_redis_with({}))
    assert status == 503
    assert body["checks"]["narrative"]["last_run"] is None


def test_health_pipeline_503_when_redis_down():
    client = MagicMock()
    client.get = AsyncMock(side_effect=ConnectionError("redis down"))
    status, body = _get_pipeline_health(client)
    assert status == 503
    assert body["status"] == "stale"


def test_plain_health_unaffected_by_stale_pipeline():
    """Railway gates deploys on /health — it must stay 200 regardless."""
    from main import app

    with patch("api.routes.health.get_redis", return_value=_redis_with({})):
        r = TestClient(app).get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}
