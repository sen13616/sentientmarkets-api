"""
pipeline/scoring/divergence.py

Detects disagreement between layer sub-indices and applies the spec's
extreme-imbalance guardrail.

Divergence flags (spec §Layer 08)
-----------------------------------
    spread > 40  →  "high_divergence"
    spread > 20  →  "moderate_divergence"
    otherwise    →  "aligned"

where spread = max(sub_index_values) − min(sub_index_values).

Cap rule
--------
If ANY layer sub-index > 85 AND ANY layer sub-index < 30, cap the
composite score at 75.  This prevents runaway optimism when one strong
bearish signal contradicts an otherwise bullish picture.

The cap is DELIBERATELY one-directional: it only restrains bullish
composites (min with 75); a bearish-divergent composite is intentionally
left with no symmetric floor. Rationale — an over-optimistic score that
ignores a strong bearish outlier is the costlier error for a sentiment
nowcast to make, so only that side is guarded. (Reviewed 2026-07-24: kept
asymmetric by design, not an oversight.)

Fewer than two available layers: spread is 0, flag is "aligned",
no cap is applied.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DivergenceResult:
    spread: float            # max − min across available sub-indices
    flag: str                # "high_divergence" | "moderate_divergence" | "aligned"
    cap_applied: bool        # True when composite was capped at 75


def compute_divergence(
    sub_indices: dict[str, float],
    composite: float,
) -> tuple[DivergenceResult, float]:
    """
    Compute divergence metrics and apply the extreme-imbalance cap.

    Parameters
    ----------
    sub_indices : dict mapping layer name → sub-index value (0–100).
                  Pass only present (non-None) layers.
    composite   : raw composite score before any capping.

    Returns
    -------
    (DivergenceResult, effective_composite)
        effective_composite is min(composite, 75) when cap_applied else composite.
    """
    values = list(sub_indices.values())

    if len(values) < 2:
        return DivergenceResult(spread=0.0, flag="aligned", cap_applied=False), round(composite, 4)

    spread = max(values) - min(values)

    if spread > 40:
        flag = "high_divergence"
    elif spread > 20:
        flag = "moderate_divergence"
    else:
        flag = "aligned"

    cap_applied = any(v > 85 for v in values) and any(v < 30 for v in values)
    # One-directional by design (see module docstring): cap bullish composites
    # only; no symmetric floor for bearish-divergent ones.
    effective   = min(composite, 75.0) if cap_applied else composite

    return (
        DivergenceResult(spread=round(spread, 4), flag=flag, cap_applied=cap_applied),
        round(effective, 4),
    )
