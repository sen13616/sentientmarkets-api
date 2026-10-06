"""
db/queries/as_of.py

Point-in-time cutoff for scoring-path reads, used by offline replays
(scripts/backfill/replay_scores.py) to score "as of" a past tick.

Live code never sets it: ``cutoff()`` is None and every query behaves exactly
as before. A replay wraps each tick in ``scoring_as_of(t)``; the ContextVar is
copied into the asyncio tasks it spawns, so concurrent per-ticker scoring sees
the same cutoff. When set, the scoring-path queries add an upper bound so no
row timestamped after t can leak into a score for t.
"""
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime

_AS_OF: ContextVar[datetime | None] = ContextVar("scoring_as_of", default=None)


def cutoff() -> datetime | None:
    """The active as-of cutoff, or None for live scoring."""
    return _AS_OF.get()


@contextmanager
def scoring_as_of(ts: datetime) -> Iterator[None]:
    """Bound scoring-path reads to rows timestamped at or before ``ts``."""
    token = _AS_OF.set(ts)
    try:
        yield
    finally:
        _AS_OF.reset(token)
