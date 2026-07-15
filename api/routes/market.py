"""
GET /v1/market/overview

Universe-level statistics for the latest scoring tick. Pro tier only.

Serves a single per-tick blob computed at the end of the global scoring
pass (pipeline/scheduler.py `_publish_universe_stats`) and cached in Redis
— the request itself performs one Redis GET and no DB work, matching the
architecture of the sentiment endpoint.

Contents: average composite score, breadth (% above 50, % improving
day-over-day), top/bottom 10 movers by 1-day change, per-sector average
scores, and within-sector rank + sector size for every ticker. The
`timestamp` field is the scoring tick the blob was computed at.

Gap safety: 1-day changes inside the blob are computed against a bounded
24-48h-old baseline and are null across data gaps; movers and the
improving-breadth figure exclude tickers with a null change.
"""
from __future__ import annotations

import json
import logging

from fastapi import APIRouter, Depends, HTTPException

from api.rate_limit import rate_limited
from api.response.schemas import ErrorResponse, MarketOverviewResponse
from scripts.db.redis import get_redis
from pipeline.scoring.market_overview import OVERVIEW_KEY

router = APIRouter()
_log   = logging.getLogger(__name__)


@router.get(
    "/market/overview",
    response_model=MarketOverviewResponse,
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        429: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
)
async def get_market_overview(
    tier: str = Depends(rate_limited),
) -> MarketOverviewResponse:
    # Pro tier only
    if tier != "pro":
        raise HTTPException(
            status_code=403,
            detail={"error": "forbidden", "message": "Market overview requires Pro tier"},
        )

    try:
        raw = await get_redis().get(OVERVIEW_KEY)
    except Exception as exc:
        _log.warning("market overview: Redis read failed: %s", exc)
        raw = None

    if raw is None:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "temporarily_unavailable",
                "message": "Market overview has not been computed yet — try again after the next scoring tick",
            },
        )

    return MarketOverviewResponse(**json.loads(raw))
