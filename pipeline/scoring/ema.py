"""
pipeline/scoring/ema.py

Exponential Moving Average (EMA) smoothing for the composite sentiment index.

Paper specification (Execution Architecture § Data Collection and Computation Layer):

    S_t^smoothed = α * raw_t + (1 - α) * smoothed_{t-1}

    α = 1 - 0.5^(dt / T½)

where T½ = 4 hours (default half-life).

Cold-start: when prev_smoothed is None (first-ever score for a ticker),
smoothed_t = raw_t.  This seeds the EMA with the first raw composite.

Gap behavior: large dt values are handled naturally by the α formula.
As dt → ∞, α → 1, so smoothed → raw.  After a 24-hour gap, α ≈ 0.984,
effectively resetting the EMA.  No threshold-based reset logic is used.

ema_obs_count: a monotonically increasing counter that tracks how many
EMA updates a ticker has received.  It starts at 1 on cold-start and
increments by 1 on every subsequent scoring tick.  It NEVER resets,
regardless of gap length or any other condition.
"""
from __future__ import annotations

import math
import os

# Tunable via env for offline experiments and a future gated change; the 4h
# default only changes if scripts/eval/experiments/ema_halflife.py wins on
# the eval scorecard. Read once at import (uvicorn restarts pick up changes).
_HALF_LIFE_HOURS: float = float(os.getenv("EMA_HALF_LIFE_HOURS", "4.0"))


def compute_ema(
    raw_t: float,
    prev_smoothed: float | None,
    dt_hours: float,
    half_life_hours: float = _HALF_LIFE_HOURS,
) -> float:
    """
    Compute the EMA-smoothed composite score.

    Parameters
    ----------
    raw_t           : Current raw (unsmoothed) composite score.
    prev_smoothed   : Previous smoothed value, or None for cold-start.
    dt_hours        : Time elapsed since the previous scoring tick, in hours.
    half_life_hours : EMA half-life in hours (default 4.0).

    Returns
    -------
    float — the smoothed composite score.
    """
    if prev_smoothed is None:
        return raw_t

    # Defensive: clamp negative dt to 0 (should never happen)
    dt_hours = max(0.0, dt_hours)

    if dt_hours == 0.0:
        return prev_smoothed

    alpha = 1.0 - math.pow(0.5, dt_hours / half_life_hours)
    return alpha * raw_t + (1.0 - alpha) * prev_smoothed
