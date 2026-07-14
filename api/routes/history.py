"""
api/routes/history.py

GET /v1/sentiment/{ticker}/history

Returns historical sentiment scores for a ticker.  Pro tier only.

Query parameters
----------------
days     : Lookback window in days.  Default 30, max 365.
interval : 'daily'  (one record per day),
           'hourly' (one record per hour),
           'raw'    (every scoring cycle).
           When omitted, defaults to 'raw' for days=1 and 'daily' otherwise.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from api.rate_limit import rate_limited
from api.response.labels import score_to_label
from api.response.schemas import ErrorResponse, HistoryEntry, HistoryResponse, HistorySubIndices
from scripts.db.queries.sentiment_history import get_history
from scripts.db.queries.universe import is_supported_ticker

router = APIRouter()


@router.get(
    "/sentiment/{ticker}/history",
    response_model=HistoryResponse,
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        429: {"model": ErrorResponse},
    },
)
async def get_sentiment_history(
    ticker:   str,
    days:     int        = Query(default=30, ge=1, le=365),
    interval: str | None = Query(default=None, pattern="^(daily|hourly|raw)$"),
    tier:     str        = Depends(rate_limited),
) -> HistoryResponse:
    # When interval is not explicitly provided, default depends on the window:
    # a 1-day window gets every scoring tick; longer windows collapse to daily.
    if interval is None:
        interval = "raw" if days == 1 else "daily"

    # Pro tier only
    if tier != "pro":
        raise HTTPException(
            status_code=403,
            detail={"error": "forbidden", "message": "History endpoint requires Pro tier"},
        )

    ticker = ticker.upper()
    if not await is_supported_ticker(ticker):
        raise HTTPException(
            status_code=404,
            detail={"error": "ticker_not_found", "message": f"Ticker {ticker!r} is not in the supported universe"},
        )

    rows = await get_history(ticker, days=days, interval=interval)

    entries = []
    for row in rows:
        layer_values = {
            "market":     row.get("market_index"),
            "narrative":  row.get("narrative_index"),
            "influencer": row.get("influencer_index"),
            "macro":      row.get("macro_index"),
        }
        # Prefer smoothed score; fall back to raw for pre-EMA rows
        smoothed = row.get("composite_score_smoothed")
        raw_score = row["composite_score"]
        display_score = smoothed if smoothed is not None else raw_score
        entries.append(
            HistoryEntry(
                timestamp      = row["timestamp"],
                score          = int(round(display_score)),
                score_raw      = int(round(raw_score)),
                label          = score_to_label(int(round(display_score))),
                confidence     = int(row["confidence_score"]),
                sub_indices    = HistorySubIndices(**layer_values),
                missing_layers = [l for l, v in layer_values.items() if v is None],
            )
        )

    return HistoryResponse(ticker=ticker, history=entries)
