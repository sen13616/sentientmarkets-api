"""
Quintile long-short: long the top fifth of the universe by a signal, short the
bottom fifth, equal-weighted, dollar-neutral (gross exposure 2.0).
"""

from __future__ import annotations

import pandas as pd

from research.strategy import SignalView, Strategy


class QuintileLongShort(Strategy):
    name = "quintile-long-short"

    def __init__(self, feature: str = "score_raw", min_names: int = 50):
        self.feature = feature
        self.min_names = min_names

    def weights(self, view: SignalView) -> pd.Series:
        x = view.today()[self.feature].dropna()
        if len(x) < self.min_names:
            return pd.Series(dtype=float)
        q = pd.qcut(x.rank(method="first"), 5, labels=False)
        longs, shorts = x.index[q == 4], x.index[q == 0]
        return pd.concat(
            [
                pd.Series(1.0 / len(longs), index=longs),
                pd.Series(-1.0 / len(shorts), index=shorts),
            ]
        )
