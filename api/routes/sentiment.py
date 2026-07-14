"""
api/routes/sentiment.py

GET /v1/sentiment/{ticker}

Returns the latest pre-computed sentiment score for a ticker.
System B is read-only — scores are served from Redis cache only.

Query parameters
----------------
detail  : 'summary' (default) or 'full'.  Full is only available on Pro tier.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, Query

from api.rate_limit import rate_limited
from api.response.assembler import assemble
from api.response.schemas import ErrorResponse, FreeTierResponse, NoDataResponse, ProTierResponse
from scripts.db.queries.universe import is_supported_ticker

router = APIRouter()
_log   = logging.getLogger(__name__)


@router.get(
    "/sentiment/{ticker}",
    response_model=None,
    responses={
        401: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        429: {"model": ErrorResponse},
    },
)
async def get_sentiment(
    ticker:  str,
    detail:  str = Query(default="summary", pattern="^(summary|full)$"),
    tier:    str = Depends(rate_limited),
) -> FreeTierResponse | ProTierResponse | NoDataResponse:
    # ── Ticker validation ─────────────────────────────────────────────────────
    ticker = ticker.upper()
    if not await is_supported_ticker(ticker):
        return NoDataResponse(
            ticker  = ticker,
            status  = "ticker_not_found",
            message = f"{ticker} is not in the supported universe",
        )

    # ── Assemble and return ───────────────────────────────────────────────────
    return await assemble(ticker, tier, detail)
