"""tests/test_driver_codec.py — compact/expand round-trip for top_drivers.

The compact encoding is what compact_drivers_before() produces in SQL and
what db_exports re-expands; the Python codec must mirror the SQL field order
exactly.
"""
from __future__ import annotations

from pipeline.scoring.driver_codec import (
    COMPACT_FIELDS,
    compact_drivers,
    expand_drivers,
    is_compact,
)

VERBOSE = [
    {
        "signal": "VIX",
        "description": "VIX at 19.3 (normal)",
        "direction": "bearish",
        "magnitude": 1.0,
        "source_layer": "macro",
        "confidence": 0.7903,
    },
    {
        "signal": "Analyst price target",
        "description": "Analyst consensus price target for AAPL: $318.25",
        "direction": "bullish",
        "magnitude": 0.8012,
        "source_layer": "influencer",
        "confidence": 0.7967,
    },
]


def test_compact_field_order():
    """Field order is the contract shared with the SQL transform."""
    assert COMPACT_FIELDS == ("signal", "direction", "magnitude", "confidence", "source_layer")


def test_compact_drops_description_keeps_values():
    compact = compact_drivers(VERBOSE)
    assert compact == [
        ["VIX", "bearish", 1.0, 0.7903, "macro"],
        ["Analyst price target", "bullish", 0.8012, 0.7967, "influencer"],
    ]


def test_round_trip_matches_original_minus_description():
    expanded = expand_drivers(compact_drivers(VERBOSE))
    for original, restored in zip(VERBOSE, expanded):
        assert restored["description"] is None
        for field in COMPACT_FIELDS:
            assert restored[field] == original[field]
        assert set(restored) == set(COMPACT_FIELDS) | {"description"}


def test_empty_list_round_trip():
    assert compact_drivers([]) == []
    assert expand_drivers([]) == []


def test_is_compact_discrimination():
    assert is_compact(compact_drivers(VERBOSE)) is True
    assert is_compact(VERBOSE) is False
    assert is_compact([]) is False
    assert is_compact(None) is False


def test_compact_tolerates_missing_keys():
    """A driver dict missing a field encodes as None, not KeyError."""
    compact = compact_drivers([{"signal": "RSI", "direction": "bullish"}])
    assert compact == [["RSI", "bullish", None, None, None]]
    assert expand_drivers(compact)[0]["magnitude"] is None
