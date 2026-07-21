"""Intraday lead-lag analysis — port of the external analyze_intraday.py.

Does a sentiment change lead price within the trading day? Prices come from
price_snapshots by default (tick closes bucketed to 15-minute bars) or from
yfinance for true exchange bars. Returns never cross the overnight gap or a
data-gap window. Pure functions over DataFrames except fetch_yf_intraday.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from scripts.eval.analyze import DEFAULT_GAPS, EXO_WEIGHTS, GapList, window_spans_gap

SIG_COLS = ["score_raw", "narrative", "market", "exo"]


def prepare_raw_sentiment(sent: pd.DataFrame) -> pd.DataFrame:
    """DB raw-tick rows -> (ticker, dt, score_raw, narrative, market, exo).

    exo (E001 plumbing): the exogenous narrative/influencer/macro composite,
    weights renormalized over present layers — same construction as
    analyze.prepare_daily, computed per scoring tick so intraday exo-change
    lead-lag can be measured.
    """
    s = sent.rename(
        columns={
            "composite_score": "score_raw",
            "narrative_index": "narrative",
            "influencer_index": "influencer",
            "macro_index": "macro",
            "market_index": "market",
        }
    ).copy()
    for col in ["score_raw", "narrative", "influencer", "macro", "market"]:
        s[col] = pd.to_numeric(s[col], errors="coerce")
    num = pd.Series(0.0, index=s.index)
    den = pd.Series(0.0, index=s.index)
    for layer, w in EXO_WEIGHTS.items():
        present = s[layer].notna()
        num += s[layer].fillna(0.0) * w * present
        den += w * present
    s["exo"] = (num / den).where(den > 0)
    s["dt"] = pd.to_datetime(s["timestamp"], utc=True)
    return s[["ticker", "dt"] + SIG_COLS].sort_values("dt")


def snapshot_bars(snapshots: pd.DataFrame, freq: str = "15min") -> pd.DataFrame:
    """Long price_snapshots rows -> wide (bar x ticker) close matrix.

    Snapshot timestamps differ per ticker by seconds within a scoring tick, so
    bucket to `freq` bars and keep the last close per (ticker, bar).
    """
    if snapshots.empty:
        return pd.DataFrame()
    p = snapshots.copy()
    p["dt"] = pd.to_datetime(p["timestamp"], utc=True).dt.floor(freq)
    p = p.sort_values("timestamp").groupby(["ticker", "dt"], as_index=False).last()
    return p.pivot(index="dt", columns="ticker", values="close").sort_index()


def fetch_yf_intraday(tickers: list[str], interval: str = "15m", period: str = "60d") -> pd.DataFrame:
    """True exchange bars via yfinance (optional cross-check price source)."""
    import yfinance as yf

    yf_names = [t.replace(".", "-") for t in tickers]
    data = yf.download(
        yf_names, interval=interval, period=period,
        auto_adjust=True, progress=False, prepost=False, group_by="column",
    )
    closes = data["Close"] if "Close" in data else data
    closes.columns = [str(c).replace("-", ".") for c in closes.columns]
    closes.index = pd.to_datetime(closes.index, utc=True)
    return closes.sort_index()


def build_long(
    prices: pd.DataFrame, sent: pd.DataFrame, gaps: GapList = DEFAULT_GAPS
) -> pd.DataFrame:
    """Long frame: ticker, dt, date, ret, dscore_raw, dnarrative, dmarket.

    merge_asof attaches the latest sentiment tick <= each price bar (no
    look-ahead); the first bar of each ET day is nulled so overnight gaps
    never count as intraday returns.
    """
    out = []
    for tk in prices.columns:
        px = prices[tk].dropna()
        if len(px) < 30:
            continue
        g = pd.DataFrame({"dt": px.index, "px": px.values})
        g["ret"] = np.log(g["px"]).diff()
        g["date"] = g["dt"].dt.tz_convert("US/Eastern").dt.date
        g["ret"] = g["ret"].mask(g["date"] != g["date"].shift(1), np.nan)
        st = (
            sent[sent["ticker"] == tk][["dt"] + SIG_COLS]
            .dropna(subset=["dt"])
            .sort_values("dt")
        )
        if len(st) < 30:
            continue
        m = pd.merge_asof(g.sort_values("dt"), st, on="dt", direction="backward")
        for c in SIG_COLS:
            m[f"d{c}"] = m[c].diff().mask(m["date"] != m["date"].shift(1), np.nan)
        m["ticker"] = tk
        gap_mask = m["dt"].apply(lambda t: window_spans_gap(t, t, _utc_gaps(gaps)))
        m = m[~gap_mask]
        out.append(m[["ticker", "dt", "date", "ret"] + [f"d{c}" for c in SIG_COLS]])
    if not out:
        return pd.DataFrame()
    return pd.concat(out, ignore_index=True)


def _utc_gaps(gaps: GapList) -> GapList:
    return [
        (g0.tz_localize("UTC") if g0.tzinfo is None else g0,
         g1.tz_localize("UTC") if g1.tzinfo is None else g1)
        for g0, g1 in gaps
    ]


def leadlag(long: pd.DataFrame, sig: str, max_k: int = 8, min_n: int = 500) -> pd.DataFrame:
    """corr(dSignal_t, return_{t+k}) for k in -max_k..max_k, shifting the
    return WITHIN each (ticker, day) group so k never crosses overnight."""
    rows = []
    grp = long.groupby(["ticker", "date"], sort=False)
    for k in range(-max_k, max_k + 1):
        retk = grp["ret"].shift(-k)
        d = pd.DataFrame({"x": long[sig], "y": retk}).dropna()
        if len(d) > min_n:
            rows.append({"lag_bars": k, "corr": round(d["x"].corr(d["y"]), 4), "n": len(d)})
    return pd.DataFrame(rows)


def overnight(
    sent: pd.DataFrame, closes: pd.DataFrame, gaps: GapList = DEFAULT_GAPS, min_n: int = 500
) -> pd.DataFrame:
    """End-of-day sentiment change vs next-day close-to-close return."""
    ret = np.log(closes.sort_index()).diff()
    trad = list(ret.index)
    rows = []
    for sig in ["score_raw", "narrative", "exo"]:
        xs, ys = [], []
        for tk, gg in sent.groupby("ticker"):
            if tk not in ret.columns:
                continue
            gg = gg.dropna(subset=[sig]).sort_values("dt")
            if len(gg) < 20:
                continue
            etdate = (
                gg["dt"].dt.tz_convert("US/Eastern").dt.normalize().dt.tz_localize(None)
            )
            last = gg.assign(etdate=etdate).groupby("etdate")[sig].last()
            chg = last.diff()
            for d, v in chg.dropna().items():
                idx = pd.Index(trad).searchsorted(d, side="right")
                if idx >= len(trad):
                    continue
                t1 = trad[idx]
                if window_spans_gap(d, t1, gaps):
                    continue
                r = ret[tk].get(t1)
                if pd.notna(r):
                    xs.append(v)
                    ys.append(r)
        if len(xs) > min_n:
            rows.append(
                {"signal": sig, "corr_nextday": round(np.corrcoef(xs, ys)[0, 1], 4), "n": len(xs)}
            )
    return pd.DataFrame(rows)
