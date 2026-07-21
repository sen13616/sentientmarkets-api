"""Daily forward-return analysis — port of the validated external backtest engine.

Design choices that keep this honest (unchanged from the original study,
docs/SUMMARYOFTESTING.md):
  * No look-ahead: signal as of ET-day d -> we ENTER at the close of the NEXT
    trading day (t1, strictly after every day-d tick) and measure forward
    returns close_t1 -> close_{t1+h}.
  * EMA confound: `score` (smoothed) lags by design; the honest forward signal
    is the unsmoothed score_raw and its changes.
  * Reflexivity: the `market` layer is price-derived, so an EXOGENOUS composite
    (narrative/influencer/macro, weights renormalized) is tested separately.
  * Headline metrics are daily cross-sectional Spearman IC (t-stat on the IC
    series) and demeaned (market-neutral) quintile long-short spreads.
  * Forward windows spanning a data-gap are dropped, never interpolated.

Pure functions over DataFrames — no I/O, no imports from pipeline/.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

HORIZONS = [1, 2, 3, 5]
EXO_WEIGHTS = {"narrative": 0.30, "influencer": 0.25, "macro": 0.10}

# The 2026 pipeline outage; run.py lets callers add windows for future gaps.
DEFAULT_GAPS = [(pd.Timestamp("2026-06-23"), pd.Timestamp("2026-07-03"))]

GapList = list[tuple[pd.Timestamp, pd.Timestamp]]


def window_spans_gap(t0, t1, gaps: GapList) -> bool:
    """True if the interval [t0, t1] overlaps any gap window."""
    lo, hi = (t0, t1) if t0 <= t1 else (t1, t0)
    return any(lo < g_end and hi > g_start for g_start, g_end in gaps)


def prepare_daily(sent: pd.DataFrame) -> pd.DataFrame:
    """DB sentiment rows -> the daily signal frame the analysis consumes.

    Input columns (from data.load_sentiment_panel): ticker, timestamp,
    composite_score (RAW), composite_score_smoothed, {market,narrative,
    influencer,macro}_index, confidence_score.
    """
    s = sent.rename(
        columns={
            "composite_score": "score_raw",
            "composite_score_smoothed": "score",
            "market_index": "market",
            "narrative_index": "narrative",
            "influencer_index": "influencer",
            "macro_index": "macro",
            "confidence_score": "confidence",
        }
    ).copy()
    # all-None columns arrive as object dtype (both from asyncpg and test
    # fixtures) and would poison the arithmetic below
    for col in ["score_raw", "score", "market", "narrative", "influencer",
                "macro", "confidence"]:
        s[col] = pd.to_numeric(s[col], errors="coerce")
    s["score"] = s["score"].fillna(s["score_raw"])
    s["ts"] = pd.to_datetime(s["timestamp"], utc=True)
    # ET calendar date of each tick, then keep the last tick per (ticker, date)
    s["date"] = (
        s["ts"].dt.tz_convert("US/Eastern").dt.normalize().dt.tz_localize(None)
    )
    s = s.sort_values("ts").groupby(["ticker", "date"], as_index=False).last()

    # cross-sectional percentile of score_raw within each day
    s["xs_pct"] = s.groupby("date")["score_raw"].rank(pct=True) * 100

    # EXOGENOUS composite: narrative/influencer/macro, weights renormalized
    # over present layers (drops the price-derived market layer)
    num = pd.Series(0.0, index=s.index)
    den = pd.Series(0.0, index=s.index)
    for layer, w in EXO_WEIGHTS.items():
        present = s[layer].notna()
        num += s[layer].fillna(0.0) * w * present
        den += w * present
    s["exo"] = (num / den).where(den > 0)
    s["exo_pct"] = s.groupby("date")["exo"].rank(pct=True) * 100

    # per-ticker changes in the unsmoothed / exogenous signals
    s = s.sort_values(["ticker", "date"])
    for col in ["score", "score_raw", "narrative", "influencer", "macro", "market", "exo"]:
        s[f"d{col}_1"] = s.groupby("ticker")[col].diff(1)
    for k in [3, 5, 7]:
        s[f"dscore_raw_{k}"] = s.groupby("ticker")["score_raw"].diff(k)
        s[f"dexo_{k}"] = s.groupby("ticker")["exo"].diff(k)
    return s


def daily_returns(closes: pd.DataFrame) -> pd.DataFrame:
    """Wide close panel -> daily log returns (index = trading days)."""
    return np.log(closes.sort_index()).diff()


def build_panel(
    s: pd.DataFrame,
    closes: pd.DataFrame,
    gaps: GapList = DEFAULT_GAPS,
    horizons: list[int] = HORIZONS,
) -> pd.DataFrame:
    """Align signals with strictly-forward returns (t1-entry convention)."""
    ret = daily_returns(closes)
    trad = ret.index
    pos = {d: i for i, d in enumerate(trad)}

    def next_trading(d):
        idx = trad.searchsorted(d, side="right")
        return trad[idx] if idx < len(trad) else pd.NaT

    s = s.copy()
    s["t1"] = s["date"].map(next_trading)
    s = s.dropna(subset=["t1"])

    # market-neutral (cross-sectionally demeaned) returns per day
    ret_xs = ret.sub(ret.mean(axis=1), axis=0)

    keep = [
        "ticker", "date", "score", "score_raw", "xs_pct", "confidence",
        "narrative", "influencer", "macro", "market", "exo", "exo_pct",
        "dscore_raw_1", "dscore_raw_3", "dscore_raw_5", "dscore_raw_7",
        "dexo_1", "dexo_3", "dexo_5",
        "dnarrative_1", "dinfluencer_1", "dmacro_1",
    ]
    rows = []
    for _, r in s.iterrows():
        tk = r["ticker"]
        if tk not in ret.columns:
            continue
        t1 = r["t1"]
        i = pos.get(t1)
        if i is None:
            continue
        rec = {c: r[c] for c in keep if c in r}
        rec["t1"] = t1
        for h in horizons:
            j = i + h
            if j >= len(trad):
                rec[f"fwd_{h}"] = np.nan
                rec[f"fwdN_{h}"] = np.nan
                continue
            end = trad[j]
            if window_spans_gap(t1, end, gaps):
                rec[f"fwd_{h}"] = np.nan
                rec[f"fwdN_{h}"] = np.nan
                continue
            # returns realized strictly after entry at t1's close
            rec[f"fwd_{h}"] = ret.loc[t1:end, tk].iloc[1:].sum()
            rec[f"fwdN_{h}"] = ret_xs.loc[t1:end, tk].iloc[1:].sum()
        rows.append(rec)
    return pd.DataFrame(rows)


DEFAULT_FEATURES = [
    "score", "score_raw", "xs_pct", "exo", "exo_pct", "confidence",
    "narrative", "influencer", "macro", "market",
    "dscore_raw_1", "dscore_raw_3", "dscore_raw_5", "dscore_raw_7",
    "dexo_1", "dexo_3", "dexo_5",
    "dnarrative_1", "dinfluencer_1", "dmacro_1",
]


def ic_table(
    panel: pd.DataFrame,
    feats: list[str] = DEFAULT_FEATURES,
    horizons: list[int] = HORIZONS,
    min_rows: int = 200,
    min_days: int = 5,
) -> pd.DataFrame:
    """Daily cross-sectional Spearman IC (Pearson of ranks) with a t-stat on
    the IC time series, per feature x horizon x {raw, market-neutral}.

    Field definitions (E001 clarification — these describe the DAILY IC
    SERIES, not individual trades):
      mean_IC  : mean of the per-day cross-sectional ICs.
      IC_t     : t-stat of that daily IC series against zero.
      hit_rate : fraction of DAYS whose cross-sectional IC was positive —
                 "on how many days did ranking by this feature rank forward
                 returns in the right direction". It is NOT per-trade
                 accuracy and does not translate to a win rate on positions.
    """
    out = []
    for f in feats:
        if f not in panel.columns:
            continue
        for h in horizons:
            for tgt, lab in [(f"fwd_{h}", "raw"), (f"fwdN_{h}", "mktneutral")]:
                if tgt not in panel.columns:
                    continue
                sub = panel[[f, tgt, "date"]].dropna()
                if len(sub) < min_rows:
                    continue
                ics = sub.groupby("date")[[f, tgt]].apply(
                    lambda g: g[f].rank().corr(g[tgt].rank())
                    if g[f].nunique() > 3
                    else np.nan
                )
                ics = ics.dropna()
                if len(ics) < min_days:
                    continue
                sd = ics.std(ddof=1)
                t = (
                    np.sign(ics.mean()) * np.inf
                    if sd == 0
                    else ics.mean() / (sd / np.sqrt(len(ics)))
                )
                out.append(
                    {
                        "feature": f,
                        "horizon_d": h,
                        "target": lab,
                        "mean_IC": round(ics.mean(), 4),
                        "IC_t": round(t, 2),
                        "hit_rate": round((ics > 0).mean(), 2),
                        "n_days": len(ics),
                    }
                )
    df = pd.DataFrame(out)
    if df.empty:
        return df
    return df.sort_values("IC_t", key=abs, ascending=False)


def quintile_ls(
    panel: pd.DataFrame,
    feat: str,
    h: int = 1,
    neutral: bool = True,
    cost_bps: float = 15.0,
) -> dict | None:
    """Daily Q5-Q1 spread on (market-neutral) forward returns, gross and net.

    Cost overlay (E001): ``cost_bps`` is a round-trip transaction cost in
    basis points charged on one-way leg turnover at each rebalance. Each
    signal day's Q5/Q1 memberships are compared with the memberships of the
    tranche being rolled (h signal-days earlier); the fraction of names
    replaced in each leg, times cost_bps/1e4, is subtracted from that day's
    gross spread. The first h days (no prior tranche) are charged full
    turnover (1.0 per leg) — conservative by design: a promotion decision
    must not lean on an inception-cost discount. Gross fields are computed
    exactly as before the overlay; ``cost_bps=0`` makes net == gross.
    """
    tgt = f"fwdN_{h}" if neutral else f"fwd_{h}"
    sub = panel[["ticker", feat, tgt, "date"]].dropna()

    gross_by_day: dict = {}
    memb_by_day: dict = {}
    for date, g in sub.groupby("date"):
        if len(g) < 20:
            continue
        q = pd.qcut(g[feat].rank(method="first"), 5, labels=False)
        gross_by_day[date] = g[tgt][q == 4].mean() - g[tgt][q == 0].mean()
        memb_by_day[date] = (set(g["ticker"][q == 4]), set(g["ticker"][q == 0]))

    dates = sorted(gross_by_day)
    if len(dates) < 5:
        return None

    cost_rt = cost_bps / 1e4
    turnovers: list[float] = []      # per-day sum of both legs' one-way turnover
    for i, d in enumerate(dates):
        long_d, short_d = memb_by_day[d]
        if i < h:
            to = 2.0                 # inception: both legs fully established
        else:
            prev_l, prev_s = memb_by_day[dates[i - h]]
            to_l = 1.0 - len(long_d & prev_l) / len(long_d) if long_d else 1.0
            to_s = 1.0 - len(short_d & prev_s) / len(short_d) if short_d else 1.0
            to = to_l + to_s
        turnovers.append(to)

    daily = pd.Series([gross_by_day[d] for d in dates], index=dates)
    net = daily - cost_rt * pd.Series(turnovers, index=dates)

    def _stats(series: pd.Series) -> tuple[float, float, float, float]:
        t = series.mean() / (series.std(ddof=1) / np.sqrt(len(series)))
        ann = series.mean() / h * 252
        sharpe = (series.mean() / h) / (series.std(ddof=1) / np.sqrt(h)) * np.sqrt(252)
        return t, ann, sharpe, (series > 0).mean()

    t, ann, sharpe, win = _stats(daily)
    t_net, ann_net, sharpe_net, win_net = _stats(net)
    return {
        "feature": feat,
        "horizon_d": h,
        "neutral": neutral,
        "mean_LS_per_period": round(daily.mean(), 5),
        "t": round(t, 2),
        "ann_return": round(ann, 3),
        "ann_sharpe": round(sharpe, 2),
        "win_rate": round(win, 2),
        "n_days": len(daily),
        "cost_bps": cost_bps,
        "avg_leg_turnover": round(float(np.mean(turnovers)) / 2.0, 3),
        "mean_LS_net_per_period": round(net.mean(), 5),
        "t_net": round(t_net, 2),
        "ann_return_net": round(ann_net, 3),
        "ann_sharpe_net": round(sharpe_net, 2),
        "win_rate_net": round(win_net, 2),
    }


def lead_lag(
    s: pd.DataFrame,
    closes: pd.DataFrame,
    col: str,
    gaps: GapList = DEFAULT_GAPS,
    max_k: int = 5,
    min_n: int = 200,
) -> pd.DataFrame:
    """Diagnostic: corr(<col>_d, return_{d+k}) for k in -max_k..max_k, pooled.

    k<0 = returns BEFORE the sentiment change (sentiment lagging price).
    k>0 = returns AFTER (sentiment leading price / potentially exploitable).
    The peak's location is the mirror-vs-headlight diagnostic.
    """
    ret = daily_returns(closes)
    trad = list(ret.index)
    pos = {d: i for i, d in enumerate(trad)}
    rows = []
    sig = s.dropna(subset=[col])
    for k in range(-max_k, max_k + 1):
        xs, ys = [], []
        for _, r in sig.iterrows():
            tk = r["ticker"]
            if tk not in ret.columns:
                continue
            i = pos.get(r["date"])
            if i is None or i + k < 0 or i + k >= len(trad):
                continue
            end = trad[i + k]
            if window_spans_gap(r["date"], end, gaps):
                continue
            rv = ret.iloc[i + k][tk]
            if pd.notna(rv):
                xs.append(r[col])
                ys.append(rv)
        if len(xs) > min_n:
            rows.append(
                {"lag_k": k, "corr": round(np.corrcoef(xs, ys)[0, 1], 4), "n": len(xs)}
            )
    return pd.DataFrame(rows)
