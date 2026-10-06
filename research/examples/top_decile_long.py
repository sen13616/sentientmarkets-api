"""
Long-only top decile: hold the 10% of names with the highest signal among
those whose confidence clears a floor, equal-weighted, fully invested.
"""

from __future__ import annotations

import pandas as pd

from research.strategy import SignalView, Strategy


class TopDecileLong(Strategy):
    name = "top-decile-long"

    def __init__(self, feature: str = "score", min_confidence: float = 70.0):
        self.feature = feature
        self.min_confidence = min_confidence

    def weights(self, view: SignalView) -> pd.Series:
        today = view.today()
        eligible = today[today["confidence"] >= self.min_confidence][self.feature].dropna()
        if len(eligible) < 10:
            return pd.Series(dtype=float)
        top = eligible.nlargest(max(1, len(eligible) // 10)).index
        return pd.Series(1.0 / len(top), index=top)
