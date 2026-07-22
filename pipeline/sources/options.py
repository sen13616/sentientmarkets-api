"""
pipeline/sources/options.py — daily options-flow snapshots (research data only).

Commissioned 2026-07-22. NOTHING reads these signals yet — no normalizer, no
sub-index, no serving path. They accumulate in raw_signals as unbackfillable
research raw material for a future registered experiment (~Sept 2026, see
scripts/eval/EXPERIMENTS.md § Queued). All four types are in
RESEARCH_RETAIN_SIGNAL_TYPES: never purged.

Source: yfinance option chains (free, no key). One snapshot per ticker per
trading day at ~21:20 UTC (after the close, staggered off market_eod_job's
batch download).

Signals produced (source='yfinance_options', stamped at snapshot time)
----------------------------------------------------------------------
pcr_volume  — put/call volume ratio, whole chain at the chosen expiry
pcr_oi      — put/call open-interest ratio, same chain
atm_iv_30d  — at-the-money implied vol: mean of the call and put IV at the
              strike nearest spot (single-leg if only one side is sane)
iv_skew_25d — 25-delta put IV minus 25-delta call IV, APPROXIMATED BY
              MONEYNESS (documented choice): yfinance provides no greeks, and
              computing deltas would require a pricing model + rate/dividend
              inputs — more machinery than day-one research data justifies.
              We use the standard rough mapping "25-delta ≈ 5% OTM at ~30
              days": IV at the put strike nearest 0.95·spot minus IV at the
              call strike nearest 1.05·spot. Positive = downside protection
              bid (the normal equity smile).

Expiry rule (documented choice): the single listed expiry nearest to 30
calendar days out (min |days_to_expiry − 30|, future expiries only). No
interpolation across two expiries — that doubles the API calls and failure
modes for a second-order improvement; revisit only if the ~Sept experiment
needs a constant-maturity series.

Spot: the ticker's latest stored close (DB), not a live quote — one fewer
yfinance call per ticker, and at 21:20 UTC the stored close IS today's close.

Guards (missing = absent row, never zero; thresholds tuned on the first
manual sweep, 2026-07-22, which ran after hours and surfaced every failure
mode at once):
  * pcr_volume / pcr_oi require REAL DEPTH on both sides (≥50 contracts
    volume / ≥100 contracts OI per side) — one-sided zeroed or thin data is
    an after-hours artifact (observed: pcr_oi of 104–343 on mega-caps)
  * IVs must lie in (0.05, 5.0) — yfinance serves TWO placeholder IV levels
    after hours (0.00001 and ~0.0156); no real 30-day equity IV is below ~5%
  * the chain must show IV DISPERSION (≥3 distinct sane IVs): flat
    placeholder surfaces (all puts 0.250007 — observed) mean no real IV data
  * ATM needs at least one sane leg; skew needs both legs AND both located
    strikes within 10% of their moneyness target
  * no expiries / no chain / no spot → ticker skipped (logged at debug)
"""
from __future__ import annotations

import asyncio
import logging
import math
from datetime import datetime, timezone

import pandas as pd

from scripts.db.queries.raw_signals import get_latest_close, insert_signals
from pipeline.rate_limits import YF_OPTIONS_DELAY, YF_OPTIONS_SEM

_log = logging.getLogger(__name__)

_SOURCE = "yfinance_options"
_TARGET_DAYS = 30
# Plausibility floors, tightened after the first manual sweep (2026-07-22):
# after-hours yfinance serves a SECOND placeholder IV ~0.0156 (above the old
# 0.01 floor) — no real 30-day equity IV sits below ~5%. And one-sided OI
# zeroing produced pcr_oi artifacts of 100-343x on mega-caps; require real
# depth on both sides before trusting either ratio.
_IV_MIN, _IV_MAX = 0.05, 5.0
_MIN_SIDE_VOLUME = 50    # contracts per side for pcr_volume
_MIN_SIDE_OI = 100       # contracts per side for pcr_oi
_SKEW_MONEYNESS = 0.05          # "25-delta ≈ 5% OTM" approximation
_SKEW_STRIKE_TOLERANCE = 0.10   # located strike must be within 10% of target

OPTIONS_SIGNAL_TYPES: list[str] = [
    "pcr_volume", "pcr_oi", "atm_iv_30d", "iv_skew_25d",
]


# ---------------------------------------------------------------------------
# Pure derivation (unit-tested with plain DataFrames)
# ---------------------------------------------------------------------------

def pick_expiry(
    expiries: list[str],
    now: datetime,
    target_days: int = _TARGET_DAYS,
) -> str | None:
    """The listed expiry nearest `target_days` calendar days out (future only)."""
    best: tuple[float, str] | None = None
    today = now.date()
    for e in expiries:
        try:
            d = datetime.strptime(e, "%Y-%m-%d").date()
        except ValueError:
            continue
        dte = (d - today).days
        if dte <= 0:
            continue
        score = abs(dte - target_days)
        if best is None or score < best[0]:
            best = (score, e)
    return best[1] if best else None


def _sane_iv(iv) -> float | None:
    try:
        iv = float(iv)
    except (TypeError, ValueError):
        return None
    if math.isnan(iv) or not (_IV_MIN < iv < _IV_MAX):
        return None
    return iv


def _has_iv_dispersion(calls: pd.DataFrame, puts: pd.DataFrame, min_distinct: int = 3) -> bool:
    """True when the chain shows a real IV surface (≥ min_distinct sane values)."""
    seen: set[float] = set()
    for df in (calls, puts):
        if df is None or df.empty or "impliedVolatility" not in df:
            continue
        for iv in df["impliedVolatility"]:
            s = _sane_iv(iv)
            if s is not None:
                seen.add(round(s, 4))
            if len(seen) >= min_distinct:
                return True
    return False


def _iv_at_strike_nearest(df: pd.DataFrame, target: float) -> tuple[float | None, float | None]:
    """(iv, strike) at the row whose strike is nearest `target`; sane IV only."""
    if df is None or df.empty or "strike" not in df or "impliedVolatility" not in df:
        return None, None
    d = df.dropna(subset=["strike"])
    if d.empty:
        return None, None
    idx = (d["strike"] - target).abs().idxmin()
    return _sane_iv(d.loc[idx, "impliedVolatility"]), float(d.loc[idx, "strike"])


def derive_options_signals(
    calls: pd.DataFrame,
    puts: pd.DataFrame,
    spot: float,
) -> dict[str, float]:
    """Derive the four snapshot signals from one expiry's chain.

    Returns only the signals that computed — missing means absent key,
    never a zero-filled value.
    """
    out: dict[str, float] = {}
    if spot is None or spot <= 0:
        return out

    def _total(df: pd.DataFrame, col: str) -> float:
        if df is None or df.empty or col not in df:
            return 0.0
        return float(pd.to_numeric(df[col], errors="coerce").fillna(0).clip(lower=0).sum())

    call_vol, put_vol = _total(calls, "volume"), _total(puts, "volume")
    if call_vol >= _MIN_SIDE_VOLUME and put_vol >= _MIN_SIDE_VOLUME:
        out["pcr_volume"] = round(put_vol / call_vol, 4)

    # Both sides must carry REAL depth: one-sided zero/thin OI on a large-cap
    # chain is an after-hours data artifact, not a market state.
    call_oi, put_oi = _total(calls, "openInterest"), _total(puts, "openInterest")
    if call_oi >= _MIN_SIDE_OI and put_oi >= _MIN_SIDE_OI:
        out["pcr_oi"] = round(put_oi / call_oi, 4)

    # Placeholder-surface detection: after hours yfinance serves FLAT IVs
    # (every strike identical). Require real dispersion before trusting IVs.
    if not _has_iv_dispersion(calls, puts):
        return out

    # ATM IV: mean of both legs at the strike nearest spot; single leg is ok
    civ, _ = _iv_at_strike_nearest(calls, spot)
    piv, _ = _iv_at_strike_nearest(puts, spot)
    legs = [v for v in (civ, piv) if v is not None]
    if legs:
        out["atm_iv_30d"] = round(sum(legs) / len(legs), 4)

    # Skew: 5%-OTM-put IV minus 5%-OTM-call IV (moneyness proxy for 25Δ).
    # Both legs required, and each located strike must be near its target —
    # a sparse chain whose "nearest" strike is far away would fake the skew.
    put_target, call_target = spot * (1 - _SKEW_MONEYNESS), spot * (1 + _SKEW_MONEYNESS)
    p_iv, p_strike = _iv_at_strike_nearest(puts, put_target)
    c_iv, c_strike = _iv_at_strike_nearest(calls, call_target)
    if (p_iv is not None and c_iv is not None
            and p_strike is not None and abs(p_strike - put_target) / put_target <= _SKEW_STRIKE_TOLERANCE
            and c_strike is not None and abs(c_strike - call_target) / call_target <= _SKEW_STRIKE_TOLERANCE):
        out["iv_skew_25d"] = round(p_iv - c_iv, 4)

    return out


# ---------------------------------------------------------------------------
# Fetch (blocking yfinance work, run in a thread under the shared semaphore)
# ---------------------------------------------------------------------------

def _fetch_chain(ticker: str, now: datetime):
    """Blocking: (calls, puts, expiry) for the nearest-30d expiry, or None."""
    import yfinance as yf

    from pipeline.sources.market import to_yahoo_symbol

    t = yf.Ticker(to_yahoo_symbol(ticker))
    expiries = list(t.options or ())
    expiry = pick_expiry(expiries, now)
    if expiry is None:
        return None
    chain = t.option_chain(expiry)
    return chain.calls, chain.puts, expiry


async def snapshot_ticker(ticker: str, now: datetime) -> bool:
    """Fetch + derive + write one ticker's snapshot. True on any rows written."""
    spot = await get_latest_close(ticker)
    if spot is None or spot <= 0:
        _log.debug("options %s: no stored close — skipping", ticker)
        return False

    async with YF_OPTIONS_SEM:
        try:
            fetched = await asyncio.to_thread(_fetch_chain, ticker, now)
        except Exception as exc:
            _log.debug("options %s: chain fetch failed: %s", ticker, exc)
            return False
        finally:
            await asyncio.sleep(YF_OPTIONS_DELAY)

    if fetched is None:
        _log.debug("options %s: no usable expiry", ticker)
        return False
    calls, puts, expiry = fetched

    signals = derive_options_signals(calls, puts, float(spot))
    if not signals:
        _log.debug("options %s: chain at %s yielded no sane signals", ticker, expiry)
        return False

    rows = [
        (ticker, sig_type, value, _SOURCE, "live", now)
        for sig_type, value in signals.items()
    ]
    await insert_signals(rows)
    return True


async def ingest_options(tickers: list[str]) -> tuple[int, int]:
    """Run the daily sweep. Per-ticker failures skip and log — never abort.

    Returns (succeeded, attempted). Writes happen per ticker as the sweep
    progresses, so a partially failed sweep still banks what it got.
    """
    now = datetime.now(timezone.utc)
    succeeded = 0

    async def _one(tk: str) -> None:
        nonlocal succeeded
        try:
            if await snapshot_ticker(tk, now):
                succeeded += 1
        except Exception as exc:
            _log.warning("options %s: unexpected error: %s", tk, exc)

    await asyncio.gather(*[_one(t) for t in tickers])
    return succeeded, len(tickers)
