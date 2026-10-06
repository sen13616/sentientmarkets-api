"""
research/strategy.py — the interface every strategy implements.

A strategy is asked, once per signal day, for target portfolio weights:

    class MyStrategy(Strategy):
        name = "my-strategy"

        def weights(self, view: SignalView) -> pd.Series:
            today = view.today()                       # one row per ticker, latest day
            ...
            return pd.Series({ticker: weight, ...})

Rules (enforced by the backtester, see research/backtest.py):
  - ``view`` exposes only signal rows dated on or before the signal day,
    restricted to tickers in the point-in-time universe — no look-ahead.
  - weights decided on day t are executed at the close of t + lag (lag ≥ 1).
  - positive = long, negative = short; the sum of |w| is gross exposure.
    Tickers absent from the returned Series get weight 0.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class SignalView:
    """Read-only view of the signal history up to and including ``date``."""

    date: pd.Timestamp
    history: pd.DataFrame  # signal rows with date <= self.date, universe-filtered
    universe: frozenset[str]

    def today(self) -> pd.DataFrame:
        """Rows dated exactly on the signal day (one per ticker that has a score)."""
        return self.history[self.history["date"] == self.date].set_index("ticker")

    def lookback(self, days: int) -> pd.DataFrame:
        """Rows from the last ``days`` calendar days, inclusive of today."""
        start = self.date - pd.Timedelta(days=days)
        return self.history[self.history["date"] > start]


class Strategy(ABC):
    """Subclass and implement ``weights``; set ``name`` for reports."""

    name: str = "strategy"

    def params(self) -> dict:
        """Parameters recorded in the run report (override if not plain attributes)."""
        return {k: v for k, v in vars(self).items() if not k.startswith("_")}

    @abstractmethod
    def weights(self, view: SignalView) -> pd.Series:
        """Target weights for the next execution, indexed by ticker."""
