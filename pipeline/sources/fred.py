"""
pipeline/sources/fred.py  — Layer 06 (Macro)  · Sprint P4.3

FRED (St. Louis Fed) Treasury / yield-curve signals. Implements the three
rates-based macro inputs called for in paper §Macroeconomic Signals (paper
§Data Collection — Macroeconomic Signals row). They feed the macro
sub-index alongside VIX and the per-ticker sector-ETF return:

    treasury_yield_10y  →  FRED series  DGS10   (10-year constant maturity)
    treasury_yield_2y   →  FRED series  DGS2    (2-year constant maturity)
    ted_spread          →  FRED series  T10Y2Y  (10y − 2y yield-curve slope)

TED-substitute decision (paper alignment)
-----------------------------------------
The paper's §Macroeconomic Signals table names a "TED spread (3-month LIBOR
− 3-month T-bill)" as one of the five macro inputs. That series was
discontinued in 2022 alongside LIBOR's retirement and is no longer
published. The agreed substitute is the **10y − 2y Treasury yield-curve
slope (`T10Y2Y`)** — same role in the paper (credit-stress / yield-curve
proxy), same sign convention (negative slope = recession signal = bearish
for equities), available on FRED with daily updates back to 1976.

Decision made 2026-05-12 during Sprint P4.3 planning. The substitution is
load-bearing: every macro composite the API has emitted since P4.3 deployed
uses `T10Y2Y`, not LIBOR-TED. The paper text needs to be edited to reflect
the substitution; this is queued for the next paper-edit cycle and tracked
in `docs/masterchecklist.md` § Phase 4 paper edits. Until then the deployed
code is canonical and the paper text describes the *intended* signal that
this substitute fulfils.

Storage: all three written under `ticker='_MACRO_'`, matching the existing
VIX convention. Scoring picks them up via the single `_MACRO_TICKER` fetch
in `_score_macro`. Per-signal weights and aggregation are in
`pipeline/scoring/subindices.py:_MACRO_SIGNAL_WEIGHTS`.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone

import httpx
from dotenv import load_dotenv

from scripts.db.queries.raw_signals import insert_signals
from pipeline.rate_limits import FRED_SEM, FRED_DELAY, guarded_get

_log = logging.getLogger(__name__)

load_dotenv(override=False)

_FRED_KEY  = os.environ.get("FRED_API_KEY", "")
_FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

#: Mapping from our signal_type to the FRED series id.
SERIES_MAP: dict[str, str] = {
    "treasury_yield_10y": "DGS10",
    "treasury_yield_2y":  "DGS2",
    "ted_spread":         "T10Y2Y",
}

_MACRO_TICKER = "_MACRO_"


async def _fetch_observation(
    client: httpx.AsyncClient,
    series_id: str,
    *,
    observation_start: str | None = None,
    observation_end: str | None = None,
    limit: int = 1,
    sort_order: str = "desc",
) -> list[tuple[float, datetime]]:
    """Fetch raw observations for `series_id` from FRED.

    Parameters mirror the FRED Observations endpoint. By default returns
    the single most recent observation. The backfill path overrides
    ``limit`` and date range to pull 90 days at once.

    Returns a list of ``(value, observation_date)`` tuples, newest first.
    Missing values (FRED uses '.' for holidays / not-yet-published days)
    are silently filtered.
    """
    if not _FRED_KEY:
        _log.warning("FRED_API_KEY not set — skipping %s", series_id)
        return []

    params: dict[str, str] = {
        "series_id":  series_id,
        "api_key":    _FRED_KEY,
        "file_type":  "json",
        "sort_order": sort_order,
        "limit":      str(limit),
    }
    if observation_start is not None:
        params["observation_start"] = observation_start
    if observation_end is not None:
        params["observation_end"] = observation_end

    resp = await guarded_get(
        client, _FRED_BASE, params=params,
        sem=FRED_SEM, delay=FRED_DELAY,
        label=f"FRED {series_id}",
    )
    if resp is None or resp.status_code != 200:
        _log.warning("FRED %s unavailable (status=%s)",
                     series_id, resp.status_code if resp else None)
        return []

    try:
        body = resp.json()
    except Exception as exc:
        _log.warning("FRED %s JSON parse error: %s", series_id, exc)
        return []

    observations = body.get("observations", []) or []
    parsed: list[tuple[float, datetime]] = []
    for obs in observations:
        raw_value = obs.get("value", "")
        if not raw_value or raw_value == ".":
            # FRED encodes "no data" as the literal string "."
            continue
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            continue
        date_str = obs.get("date", "")
        try:
            obs_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except (TypeError, ValueError):
            continue
        parsed.append((value, obs_date))
    return parsed


#: How many observations to request per series. Must cover the worst
#: realistic run of unusable head observations ('.'-padded holidays /
#: not-yet-published days over a long weekend) so the newest real value
#: is always inside the page.
_OBS_FETCH_LIMIT = 10

#: One retry after a transient failure. The daily 02:00 UTC run showed a
#: positional failure pattern (first series fetched failed for days in a
#: row while later ones succeeded — DGS10 stalled 12 days in July 2026),
#: consistent with a transient network/rate-limit burst at the top of the
#: hour that guarded_get's own retries did not outlast.
_RETRY_DELAY_S = 15.0


async def fetch_fred_signals(client: httpx.AsyncClient) -> int:
    """Daily ingest: write the latest real observation for each FRED series.

    Requests a page of recent observations (not just the newest row) so a
    '.'-padded head observation cannot blank the series for the day —
    _fetch_observation already filters '.' rows, so obs[0] is the newest
    observation that carries an actual value. Retries each series once
    after _RETRY_DELAY_S on a failed/empty first attempt.

    Returns the number of rows written.
    """
    rows: list[tuple] = []
    for sig_type, series_id in SERIES_MAP.items():
        obs = await _fetch_observation(
            client, series_id, limit=_OBS_FETCH_LIMIT, sort_order="desc",
        )
        if not obs:
            _log.warning("FRED %s empty on first attempt — retrying in %.0fs",
                         series_id, _RETRY_DELAY_S)
            await asyncio.sleep(_RETRY_DELAY_S)
            obs = await _fetch_observation(
                client, series_id, limit=_OBS_FETCH_LIMIT, sort_order="desc",
            )
        if not obs:
            _log.warning("FRED %s returned no usable observation this tick", series_id)
            continue
        value, obs_date = obs[0]
        rows.append((_MACRO_TICKER, sig_type, value, "fred", "live", obs_date))

    if rows:
        await insert_signals(rows)
        _log.info("fetch_fred_signals: wrote %d rows (%s)",
                  len(rows), ", ".join(r[1] for r in rows))
    return len(rows)
