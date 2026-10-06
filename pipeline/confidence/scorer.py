"""
pipeline/confidence/scorer.py

Computes an integer confidence score (0–100) by applying a penalty system.

Penalty table (spec §Layer 09)
--------------------------------
    Missing layer               −15  per layer
    Stale data                  −10  per source
    Low signal volume (< 5)     −20
    High divergence             −15

Rules
-----
- Start at 100
- Subtract each active penalty (no bonuses)
- Clip result to [0, 100]
- Return as integer

The caller is responsible for determining which penalties apply (see
staleness.py for stale-source detection and divergence.py for the
divergence flag).
"""

from __future__ import annotations

from dataclasses import dataclass, field

LOW_VOLUME_THRESHOLD = 5

PENALTIES: dict[str, int] = {
    "missing_layer": 15,
    "stale_source": 10,
    "low_signal_volume": 20,
    "high_divergence": 15,
}


@dataclass(frozen=True)
class ConfidenceResult:
    score: int  # 0–100 integer, clipped
    flags: list[str] = field(default_factory=list, compare=False)


def compute_confidence(
    missing_layers: list[str],
    stale_sources: list[str],
    n_signals: int,
    divergence_flag: str,
) -> ConfidenceResult:
    """
    Compute confidence score by accumulating penalties.

    Parameters
    ----------
    missing_layers  : names of layers with no sub-index (e.g. ["macro"]).
    stale_sources   : names of data sources whose as_of is past threshold.
    n_signals       : total number of signals across all present layers.
    divergence_flag : "high_divergence" | "moderate_divergence" | "aligned".

    Returns
    -------
    ConfidenceResult(score, flags)
        score : clipped integer in [0, 100]
        flags : list of active penalty strings for transparency
    """
    raw = 100
    flags: list[str] = []

    # --- Missing layers: −15 each ---
    for layer in missing_layers:
        raw -= PENALTIES["missing_layer"]
        flags.append(f"missing_layer:{layer}")

    # --- Stale data: −10 per source ---
    for source in stale_sources:
        raw -= PENALTIES["stale_source"]
        flags.append(f"stale:{source}")

    # --- Low signal volume: −20 ---
    if n_signals < LOW_VOLUME_THRESHOLD:
        raw -= PENALTIES["low_signal_volume"]
        flags.append("low_signal_volume")

    # --- High divergence: −15 ---
    if divergence_flag == "high_divergence":
        raw -= PENALTIES["high_divergence"]
        flags.append("high_divergence")

    return ConfidenceResult(score=max(0, min(100, raw)), flags=flags)
