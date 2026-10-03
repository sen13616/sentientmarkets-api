"""
api/routes/tickers.py

GET /v1/tickers

Returns all active tickers in the supported universe (retired symbols —
delisted, renamed or merged — are omitted; see ticker_universe.delisted_at).
"""
from __future__ import annotations

from fastapi import APIRouter, Depends

from api.rate_limit import rate_limited
from api.response.schemas import TickerItem, TickersResponse
from scripts.db.queries.universe import get_all_tickers

router = APIRouter()


@router.get("/tickers", response_model=TickersResponse)
async def list_tickers(
    tier: str = Depends(rate_limited),
) -> TickersResponse:
    rows = await get_all_tickers()
    items = [
        TickerItem(ticker=r["ticker"], name=r["company_name"], sector=r["sector"],
                   in_sp500=r.get("in_sp500"))
        for r in rows
    ]
    return TickersResponse(universe_size=len(items), tickers=items)
