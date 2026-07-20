"""
api/routes/status.py

GET /v1/status

Returns API health and last pipeline run timestamps. Requires a valid API
key (any tier) and counts against the caller's rate limit — use /health for
unauthenticated liveness checks.

Run timestamps are stored in Redis by the scheduler jobs under keys:
    pipeline:last_run:{job_name}   →  ISO-8601 string (UTC)

If Redis has no record for a job, the key is absent from the response (None).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends

from api.rate_limit import rate_limited
from api.response.schemas import ErrorResponse, StatusResponse
from scripts.db.redis import get_redis
from pipeline.confidence.staleness import is_market_hours

router = APIRouter()
_log   = logging.getLogger(__name__)

_JOB_KEYS = {
    "last_market_run":     "pipeline:last_run:market",
    "last_narrative_run":  "pipeline:last_run:narrative",
    "last_influencer_run": "pipeline:last_run:influencer",
    "last_eod_run":          "pipeline:last_run:market_eod",
    "last_scoring_tick_run": "pipeline:last_run:scoring_tick",
}

# The single macro job was split into two (daily FRED + intraday VIX/ETF),
# each recording its own run key. `last_macro_run` reports the most recent of
# the two so it reflects the freshest macro refresh regardless of cadence.
_MACRO_KEYS = (
    "pipeline:last_run:macro_daily",
    "pipeline:last_run:macro_intraday",
)


def _parse_ts(raw) -> datetime | None:
    if raw is None:
        return None
    try:
        return datetime.fromisoformat(raw.decode() if isinstance(raw, bytes) else raw)
    except (ValueError, AttributeError):
        return None


async def _read_all_ts(keys: list[str]) -> dict[str, datetime | None]:
    """One Redis MGET for every run-timestamp key; None per key on any failure."""
    try:
        raws = await get_redis().mget(keys)
    except Exception as exc:
        _log.warning("status: failed to read run timestamps: %s", exc)
        return {k: None for k in keys}
    return {k: _parse_ts(raw) for k, raw in zip(keys, raws)}


@router.get(
    "/status",
    response_model=StatusResponse,
    responses={
        401: {"model": ErrorResponse},
        429: {"model": ErrorResponse},
    },
)
async def get_status(
    tier: str = Depends(rate_limited),
) -> StatusResponse:
    # Single MGET for all job keys + the two macro keys.
    all_keys = list(_JOB_KEYS.values()) + list(_MACRO_KEYS)
    by_key = await _read_all_ts(all_keys)

    timestamps = {field: by_key[key] for field, key in _JOB_KEYS.items()}

    # Macro: most recent of the daily (FRED) and intraday (VIX/ETF) jobs.
    macro_runs = [by_key[k] for k in _MACRO_KEYS if by_key[k] is not None]
    timestamps["last_macro_run"] = max(macro_runs) if macro_runs else None

    return StatusResponse(
        status         = "operational",
        market_is_open = is_market_hours(datetime.now(timezone.utc)),
        **timestamps,
    )
