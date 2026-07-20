"""
pipeline/scoring/driver_codec.py

Compact encoding for `sentiment_history.top_drivers` on rows older than the
retention window (`pipeline.scheduler.DRIVER_COMPACT_DAYS`).

Storage formats
---------------
Verbose (all new writes; rows within the retention window) — array of objects
as emitted by `DriverRecord.to_dict()`:

    [{"signal": ..., "description": ..., "direction": ..., "magnitude": ...,
      "source_layer": ..., "confidence": ...}, ...]

Compact (rows past the retention window) — array of fixed-order arrays:

    [[signal, direction, magnitude, confidence, source_layer], ...]

`description` is dropped by compaction (it embeds the raw signal value, which
is not otherwise stored — originals are archived to gzip CSV before the
one-off backfill; see scripts/tools/compact_drivers_backfill.py).

The DB-side transform in `compact_drivers_before()`
(scripts/db/queries/sentiment_history.py) must emit the same field order as
COMPACT_FIELDS. The two formats are distinguished by the type of the first
element (dict → verbose, list → compact); empty/None values are unchanged by
compaction.
"""
from __future__ import annotations

COMPACT_FIELDS: tuple[str, ...] = (
    "signal",
    "direction",
    "magnitude",
    "confidence",
    "source_layer",
)


def compact_drivers(drivers: list[dict]) -> list[list]:
    """Encode verbose driver dicts into compact fixed-order arrays."""
    return [[d.get(f) for f in COMPACT_FIELDS] for d in drivers]


def expand_drivers(drivers: list[list]) -> list[dict]:
    """Decode compact arrays back into driver dicts (description=None)."""
    return [
        {**dict(zip(COMPACT_FIELDS, d)), "description": None}
        for d in drivers
    ]


def is_compact(drivers: list | None) -> bool:
    """True if `drivers` is a non-empty list in the compact encoding."""
    return bool(drivers) and isinstance(drivers[0], list)
