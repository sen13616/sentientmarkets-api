"""
pipeline/persistence/redis_writer.py

Serializes the full scored state for a ticker to JSON and writes it to Redis.

Key schema
----------
    Key   : sentiment:{ticker}          e.g. "sentiment:AAPL"
    Value : JSON string of scored_state
    TTL   : 86400 seconds (24 hours)

The TTL acts as a safety net — the background pipeline overwrites the key on
every scoring cycle, so in normal operation it never expires.  24 hours ensures
stale keys are eventually evicted if a ticker leaves the active universe.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from scripts.db.redis import get_redis

_TTL_SECONDS = 86_400  # 24 hours


def _default_serializer(obj: object) -> str:
    """JSON serializer for datetime objects."""
    if isinstance(obj, datetime):
        if obj.tzinfo is None:
            obj = obj.replace(tzinfo=timezone.utc)
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


async def write_scored_state(ticker: str, state: dict) -> None:
    """
    Serialize `state` to JSON and write it to Redis under ``sentiment:{ticker}``.

    Parameters
    ----------
    ticker : str
        Uppercase ticker symbol (e.g. "AAPL").
    state : dict
        Fully assembled scored state.  Must be JSON-serializable; datetime
        values are automatically converted to ISO-8601 strings.
        Expected top-level keys (all optional — writer is schema-agnostic):
            ticker, timestamp, composite_score, label, sub_indices,
            confidence, top_drivers, explanation, freshness,
            confidence_flags, divergence.

    Raises
    ------
    RuntimeError
        If init_redis() has not been called before this function.
    """
    client = get_redis()
    key = f"sentiment:{ticker.upper()}"
    payload = json.dumps(state, default=_default_serializer)
    await client.set(key, payload, ex=_TTL_SECONDS)


async def read_scored_state(ticker: str) -> dict | None:
    """
    Read and deserialize the scored state for `ticker` from Redis.

    Returns None if the key does not exist (cache miss).
    """
    client = get_redis()
    key = f"sentiment:{ticker.upper()}"
    raw = await client.get(key)
    if raw is None:
        return None
    return json.loads(raw)
