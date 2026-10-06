"""
pipeline/sources/short_volume.py

Fetches FINRA daily short volume data from REGSHO reports and writes to
raw_signals.

Data source
-----------
FINRA publishes daily short-volume files for each market centre at:
    https://cdn.finra.org/equity/regsho/daily/{MARKET}shvol{YYYYMMDD}.txt

The files are pipe-delimited with columns:
    Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market

IMPORTANT: This is off-exchange (OTC / TRF) short volume only — NOT total
consolidated short interest.  Signal names use the '_otc' suffix to make this
explicit and prevent misinterpretation.

Markets
-------
CNMS (consolidated NMS) is preferred because it aggregates all three TRFs.
If CNMS fails (404 / error), we fall back to summing the three individual
TRF files: FNYX (NYSE), FNSQ (NASDAQ), FNQC (FINRA Chicago).

Signals produced (per ticker)
-----------------------------
short_volume_otc         — daily short volume (off-exchange)
short_volume_total_otc   — daily total volume (off-exchange)
short_volume_ratio_otc   — short_volume / total_volume (0.0–1.0)

All written with source='finra_regsho', upload_type='live'.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone

import httpx

from scripts.db.queries.raw_signals import insert_signals
from scripts.db.queries.universe import get_active_tickers

_log = logging.getLogger(__name__)

_REGSHO_BASE = "https://cdn.finra.org/equity/regsho/daily"

# Markets: CNMS (consolidated), or individual TRFs
_CNMS_MARKET = "CNMSshvol"
_TRF_MARKETS = ("FNYXshvol", "FNSQshvol", "FNQCshvol")


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _parse_short_volume(text: str) -> dict[str, dict[str, int]]:
    """
    Parse a FINRA REGSHO pipe-delimited short-volume file.

    Parameters
    ----------
    text : Raw text content of the file.

    Returns
    -------
    dict mapping ticker → {"short_volume": int, "total_volume": int}.
    Rows with zero total_volume or invalid data are silently skipped.
    The trailer line (starts with non-date / non-numeric) is skipped.
    """
    result: dict[str, dict[str, int]] = {}

    for line in text.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("Date"):
            continue

        parts = line.split("|")
        if len(parts) < 5:
            continue

        # Trailer line: "Total|..." or non-date first field
        if not parts[0][:1].isdigit():
            continue

        try:
            symbol = parts[1].strip()
            short_vol = int(float(parts[2].strip()))   # FINRA switched to float format ~2026-02-24
            # parts[3] is ShortExemptVolume — skip
            total_vol = int(float(parts[4].strip()))   # FINRA switched to float format ~2026-02-24
        except (ValueError, IndexError):
            continue

        if total_vol <= 0 or not symbol:
            continue

        # Aggregate in case of duplicate rows (shouldn't happen, but safe)
        if symbol in result:
            result[symbol]["short_volume"] += short_vol
            result[symbol]["total_volume"] += total_vol
        else:
            result[symbol] = {
                "short_volume": short_vol,
                "total_volume": total_vol,
            }

    return result


def _combine_trf_data(
    datasets: list[dict[str, dict[str, int]]],
) -> dict[str, dict[str, int]]:
    """
    Combine data from multiple TRF files by summing volumes per ticker.

    Parameters
    ----------
    datasets : List of parsed TRF results from ``_parse_short_volume()``.

    Returns
    -------
    Combined dict mapping ticker → {"short_volume": int, "total_volume": int}.
    """
    combined: dict[str, dict[str, int]] = {}
    for ds in datasets:
        for symbol, vols in ds.items():
            if symbol in combined:
                combined[symbol]["short_volume"] += vols["short_volume"]
                combined[symbol]["total_volume"] += vols["total_volume"]
            else:
                combined[symbol] = {
                    "short_volume": vols["short_volume"],
                    "total_volume": vols["total_volume"],
                }
    return combined


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

def _url_for(market_prefix: str, target_date: date) -> str:
    """Build the REGSHO URL for a given market and date."""
    return f"{_REGSHO_BASE}/{market_prefix}{target_date.strftime('%Y%m%d')}.txt"


async def fetch_short_volume_for_date(
    target_date: date,
    client: httpx.AsyncClient,
) -> dict[str, dict[str, int]] | None:
    """
    Fetch short-volume data for a specific date.

    Tries CNMS (consolidated) first.  If that fails (404 or error), falls
    back to summing the three individual TRF files (FNYX, FNSQ, FNQC).

    Parameters
    ----------
    target_date : The trading date to fetch.
    client      : Shared httpx async client.

    Returns
    -------
    Parsed dict mapping ticker → volumes, or None if all sources fail.
    """
    # Try CNMS first
    url = _url_for(_CNMS_MARKET, target_date)
    try:
        resp = await client.get(url)
        if resp.status_code == 200:
            data = _parse_short_volume(resp.text)
            if data:
                _log.info(
                    "FINRA CNMS %s: %d tickers", target_date.isoformat(), len(data),
                )
                return data
            _log.warning("FINRA CNMS %s: parsed 0 tickers", target_date.isoformat())
        else:
            _log.debug(
                "FINRA CNMS %s: HTTP %d", target_date.isoformat(), resp.status_code,
            )
    except Exception as exc:
        _log.warning("FINRA CNMS %s fetch error: %s", target_date.isoformat(), exc)

    # Fallback: fetch all three TRFs
    _log.info("FINRA: falling back to individual TRFs for %s", target_date.isoformat())
    trf_datasets: list[dict[str, dict[str, int]]] = []
    for market in _TRF_MARKETS:
        url = _url_for(market, target_date)
        try:
            resp = await client.get(url)
            if resp.status_code == 200:
                parsed = _parse_short_volume(resp.text)
                if parsed:
                    trf_datasets.append(parsed)
                    _log.debug("FINRA %s %s: %d tickers", market, target_date.isoformat(), len(parsed))
            else:
                _log.debug(
                    "FINRA %s %s: HTTP %d", market, target_date.isoformat(), resp.status_code,
                )
        except Exception as exc:
            _log.warning("FINRA %s %s fetch error: %s", market, target_date.isoformat(), exc)

    if not trf_datasets:
        _log.warning("FINRA: all sources failed for %s", target_date.isoformat())
        return None

    combined = _combine_trf_data(trf_datasets)
    _log.info(
        "FINRA TRF combined %s: %d tickers from %d TRFs",
        target_date.isoformat(), len(combined), len(trf_datasets),
    )
    return combined


async def latest_short_volume(
    client: httpx.AsyncClient,
    ref_date: date | None = None,
) -> tuple[dict[str, dict[str, int]], date] | None:
    """
    Fetch the most recent available short-volume data.

    FINRA publishes after market close; today's file may not be available
    yet, or the most recent trading day may be earlier (weekends/holidays).
    Walks back up to 4 calendar days from *ref_date* (default: today UTC).

    Parameters
    ----------
    client   : Shared httpx async client.
    ref_date : Starting date to try (default: today UTC).

    Returns
    -------
    (data_dict, actual_date) on success, or None if all attempts fail.
    """
    if ref_date is None:
        ref_date = datetime.now(timezone.utc).date()

    for offset in range(5):
        candidate = ref_date - timedelta(days=offset)
        # Skip weekends (Saturday=5, Sunday=6)
        if candidate.weekday() >= 5:
            continue
        data = await fetch_short_volume_for_date(candidate, client)
        if data:
            return data, candidate

    _log.warning("FINRA: no data found in last 5 days from %s", ref_date.isoformat())
    return None


# ---------------------------------------------------------------------------
# Ingestion (called by scheduler)
# ---------------------------------------------------------------------------

async def ingest_short_volume(client: httpx.AsyncClient) -> int:
    """
    Fetch the latest FINRA short volume, filter to active universe, and
    write to raw_signals.

    Returns the number of tickers written, or 0 on failure.
    """
    result = await latest_short_volume(client)
    if result is None:
        _log.warning("ingest_short_volume: no data available")
        return 0

    data, actual_date = result
    universe = set(await get_active_tickers())

    # Use end-of-day (21:00 UTC) of the actual data date as the signal timestamp
    ts = datetime(
        actual_date.year, actual_date.month, actual_date.day,
        21, 0, 0, tzinfo=timezone.utc,
    )

    rows: list[tuple] = []
    for ticker, vols in data.items():
        if ticker not in universe:
            continue

        short_vol = vols["short_volume"]
        total_vol = vols["total_volume"]
        ratio = short_vol / total_vol if total_vol > 0 else 0.0

        rows.append((ticker, "short_volume_otc",       float(short_vol), "finra_regsho", "live", ts))
        rows.append((ticker, "short_volume_total_otc",  float(total_vol), "finra_regsho", "live", ts))
        rows.append((ticker, "short_volume_ratio_otc",  ratio,            "finra_regsho", "live", ts))

    if rows:
        await insert_signals(rows)

    n_tickers = len(rows) // 3
    if n_tickers == 0:
        _log.warning(
            "ingest_short_volume: 0 tickers written for %s (parsed %d raw tickers, none in universe)",
            actual_date.isoformat(), len(data),
        )
    else:
        _log.info(
            "ingest_short_volume: %d tickers written for %s (from %d raw tickers)",
            n_tickers, actual_date.isoformat(), len(data),
        )
    return n_tickers
