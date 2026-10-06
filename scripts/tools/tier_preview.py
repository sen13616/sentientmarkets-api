#!/usr/bin/env python3
"""
scripts/tools/tier_preview.py

Interactive terminal tool: enter a ticker, print the free-tier and
pro-tier API responses assembled from the current DB / Redis state.

Run from the project root:
    python3 -m scripts.tools.tier_preview
"""
from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime

from dotenv import load_dotenv

load_dotenv(override=True)

from api.response.assembler import assemble
from api.response.labels import score_to_label
from scripts.db.connection import close_pool, init_pool
from scripts.db.queries.sentiment_history import get_history
from scripts.db.queries.universe import is_supported_ticker
from scripts.db.redis import close_redis, init_redis

_HISTORY_LOOKBACKS = (
    (1,  "raw"),
    (1,  "hourly"),
    (5,  "daily"),
    (30, "daily"),
)


def _json_default(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _dump(response) -> str:
    if hasattr(response, "model_dump"):
        data = response.model_dump()
    else:
        data = dict(response)
    return json.dumps(data, indent=2, default=_json_default)


def _format_history_entry(row: dict) -> dict:
    smoothed = row.get("composite_score_smoothed")
    raw_score = row["composite_score"]
    display_score = smoothed if smoothed is not None else raw_score
    score = int(round(display_score))
    layer_values = {
        "market":     row.get("market_index"),
        "narrative":  row.get("narrative_index"),
        "influencer": row.get("influencer_index"),
        "macro":      row.get("macro_index"),
    }
    return {
        "timestamp":      row["timestamp"],
        "score":          score,
        "score_raw":      int(round(raw_score)),
        "label":          score_to_label(score),
        "confidence":     int(row["confidence_score"]),
        "sub_indices":    layer_values,
        "missing_layers": [layer for layer, v in layer_values.items() if v is None],
    }


async def _print_history(ticker: str, days: int, interval: str) -> None:
    rows = await get_history(ticker, days=days, interval=interval)
    entries = [_format_history_entry(r) for r in rows]
    print(f"\n========== {ticker} — HISTORY ({days}d, {interval}, {len(entries)} entries) ==========")
    print(json.dumps(entries, indent=2, default=_json_default))


async def _preview(ticker: str) -> None:
    ticker = ticker.upper().strip()
    if not ticker:
        print("No ticker entered.")
        return

    if not await is_supported_ticker(ticker):
        print(f"{ticker} is not in the supported universe.")
        return

    free_response = await assemble(ticker, tier="free", detail="summary")
    pro_response = await assemble(ticker, tier="pro", detail="full")

    print(f"\n========== {ticker} — FREE TIER ==========")
    print(_dump(free_response))
    print(f"\n========== {ticker} — PRO TIER ==========")
    print(_dump(pro_response))

    for days, interval in _HISTORY_LOOKBACKS:
        await _print_history(ticker, days, interval)


async def _main() -> None:
    await init_pool()
    await init_redis()
    try:
        if len(sys.argv) > 1:
            ticker = sys.argv[1]
        else:
            ticker = input("Enter ticker: ")
        await _preview(ticker)
    finally:
        await close_redis()
        await close_pool()


if __name__ == "__main__":
    asyncio.run(_main())
