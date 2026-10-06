"""
research/dataset.py — load a research snapshot and answer point-in-time questions.

    snap = Snapshot.load("data/snapshots/2026-10-06")
    snap.signals          # daily signal frame (same derived features as the eval harness)
    snap.closes           # wide adjusted closes: index = trading day, columns = tickers
    snap.universe_on(d)   # tickers listed in our universe on day d (added_at / delisted_at)
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from scripts.eval.analyze import prepare_daily


@dataclass
class Snapshot:
    path: Path
    manifest: dict
    signals: pd.DataFrame  # one row per (ticker, date); see prepare_daily for derived columns
    prices: pd.DataFrame  # long: ticker, date, open, close, volume
    universe: pd.DataFrame
    closes: pd.DataFrame = field(init=False)
    opens: pd.DataFrame = field(init=False)

    def __post_init__(self) -> None:
        self.closes = self._wide("close")
        self.opens = self._wide("open")

    # ------------------------------------------------------------------ build
    @classmethod
    def load(cls, path: str | Path, include_replayed: bool = True) -> Snapshot:
        """
        Load a snapshot directory. ``include_replayed=False`` drops rows that
        were recomputed offline (``replay_run`` not null) — i.e. keeps only
        scores that were actually served live.
        """
        path = Path(path)
        manifest = json.loads((path / "manifest.json").read_text())
        raw = pd.read_parquet(path / "sentiment_daily.parquet")
        prices = pd.read_parquet(path / "prices_daily.parquet")
        universe = pd.read_parquet(path / "universe.parquet")
        return cls.from_frames(manifest, raw, prices, universe, path, include_replayed)

    @classmethod
    def from_frames(
        cls,
        manifest: dict,
        sentiment_raw: pd.DataFrame,
        prices: pd.DataFrame,
        universe: pd.DataFrame,
        path: str | Path = "<memory>",
        include_replayed: bool = True,
    ) -> Snapshot:
        raw = sentiment_raw
        if not include_replayed and "replay_run" in raw:
            raw = raw[raw["replay_run"].isna()]
        replay = raw[["ticker", "timestamp", "replay_run"]] if "replay_run" in raw else None
        signals = prepare_daily(raw.drop(columns=["replay_run", "divergence"], errors="ignore"))
        if replay is not None and len(signals):
            # carry the replay tag through (prepare_daily keeps the last tick per day)
            signals = signals.merge(replay, on=["ticker", "timestamp"], how="left")
        prices = prices.copy()
        prices["date"] = pd.to_datetime(prices["date"])
        uni = universe.copy()
        for col in ("added_at", "delisted_at"):
            uni[col] = pd.to_datetime(uni[col], utc=True)
        return cls(Path(path), manifest, signals.reset_index(drop=True), prices, uni)

    def _wide(self, col: str) -> pd.DataFrame:
        if self.prices.empty:
            return pd.DataFrame()
        return self.prices.pivot(index="date", columns="ticker", values=col).sort_index()

    # ------------------------------------------------------------- questions
    @property
    def trading_days(self) -> pd.DatetimeIndex:
        return self.closes.index

    def universe_on(self, day: pd.Timestamp | str, sp500_only: bool = False) -> set[str]:
        """
        Tickers in our universe on ``day`` (point-in-time): added on or before
        that day and not yet past their last trading day. ``sp500_only``
        additionally requires the current S&P 500 flag — note that flag is a
        snapshot as of the export date, not historical membership.
        """
        d = (
            pd.Timestamp(day).tz_localize("UTC")
            if pd.Timestamp(day).tzinfo is None
            else pd.Timestamp(day)
        )
        end_of_day = d.normalize() + pd.Timedelta(days=1)
        u = self.universe
        live = (u["added_at"] < end_of_day) & (
            u["delisted_at"].isna() | (u["delisted_at"] >= d.normalize())
        )
        if sp500_only:
            live &= u["in_sp500"].fillna(False).astype(bool)
        return set(u.loc[live, "ticker"])

    def last_trading_day(self) -> dict[str, pd.Timestamp]:
        """Retired tickers → their last trading day (naive date)."""
        u = self.universe.dropna(subset=["delisted_at"])
        return {
            t: ts.tz_convert(None).normalize()
            for t, ts in zip(u["ticker"], u["delisted_at"], strict=False)
        }

    @property
    def known_gaps(self) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
        return [
            (pd.Timestamp(g["start"]), pd.Timestamp(g["end"]))
            for g in self.manifest.get("known_gaps", [])
        ]
