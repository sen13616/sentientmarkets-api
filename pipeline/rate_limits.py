"""
pipeline/rate_limits.py

Shared rate-limiting primitives for all external API sources.

Each external provider gets one asyncio.Semaphore that caps the number of
concurrent in-flight requests, plus a minimum inter-request delay enforced
while the semaphore is held.  Because asyncio is single-threaded the
singletons here are shared safely across every source module, so
market_job and narrative_job compete for the same AV quota rather than
each launching independent burst storms.

Provider limits (conservative targets)
---------------------------------------
    Alpha Vantage premium : 75 req/min  → Semaphore(1) + 0.85 s/req  ≈ 70/min
    Finnhub free          : 30 req/min  → Semaphore(1) + 2.10 s/req  ≈ 28/min
    SEC EDGAR             : 10 req/sec  → Semaphore(5) + 0.50 s/slot ≈  8/sec
    Polygon               : 5 req/min   → Semaphore(1) + 0.85 s/req  ≈ 70/min

Note: 403 / 404 responses (auth failures) do not consume provider quota.
They get a brief 50 ms courtesy delay instead of the full spacing.

Retry policy (applied inside guarded_get)
------------------------------------------
    HTTP 429  : wait 2 s → 4 s → 8 s, max 3 retries, then skip.
    Timeout / ConnectError : wait 2 s, 1 retry, then skip.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import httpx

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Per-source semaphores and inter-request delays
# ---------------------------------------------------------------------------

#: Alpha Vantage premium: 75 req/min → safe at 1 concurrent + 0.85 s gap.
AV_SEM    = asyncio.Semaphore(1)
AV_DELAY  = 0.85

#: Finnhub free: 30 req/min → safe at 1 concurrent + 2.1 s gap.
FINNHUB_SEM   = asyncio.Semaphore(1)
FINNHUB_DELAY = 2.1

#: SEC EDGAR: 10 req/sec → safe at 5 concurrent + 0.5 s spacing per slot.
EDGAR_SEM   = asyncio.Semaphore(5)
EDGAR_DELAY = 0.5

#: Polygon free: ~5 req/min for aggregates → treat same as AV.
POLYGON_SEM   = asyncio.Semaphore(1)
POLYGON_DELAY = 0.85

#: yfinance Ticker.info: no hard rate limit, but each call is a blocking
#: HTTP round-trip.  Cap at 10 concurrent to avoid overwhelming yfinance
#: servers or the event loop's thread pool.
YF_INFO_SEM = asyncio.Semaphore(10)

#: yfinance option chains (options_job): two blocking HTTP round-trips per
#: ticker (expiry list + chain). yfinance is fragile at 500-ticker scale —
#: throttle politely: 4 concurrent + 0.15 s spacing ≈ full sweep in ~10 min.
YF_OPTIONS_SEM = asyncio.Semaphore(4)
YF_OPTIONS_DELAY = 0.15

#: FRED (St. Louis Fed): documented 120 req/min with API key. The macro
#: daily job makes 3 calls; the optional backfill makes 3 × 90 = 270 calls
#: spread over ~5 minutes. Conservative settings keep us well clear.
FRED_SEM   = asyncio.Semaphore(2)
FRED_DELAY = 0.5

# Auth-failure status codes: do not consume the provider's rate-limit quota.
_AUTH_FAIL_CODES: frozenset[int] = frozenset((403, 404))
_BRIEF_DELAY: float = 0.05     # courtesy pause for auth failures

# 429 back-off schedule (applied outside the semaphore, max 3 retries).
_429_BACKOFF: tuple[int, ...] = (2, 4, 8)


# ---------------------------------------------------------------------------
# Job-level counters (reset at the start of each scheduler job)
# ---------------------------------------------------------------------------

@dataclass
class _JobCounters:
    rate_limit_skips: int = 0
    net_error_skips:  int = 0

    def reset(self) -> None:
        self.rate_limit_skips = 0
        self.net_error_skips  = 0


job_counters = _JobCounters()


# ---------------------------------------------------------------------------
# Rate-limited HTTP helper
# ---------------------------------------------------------------------------

async def guarded_get(
    client:  httpx.AsyncClient,
    url:     str,
    params:  dict | None = None,
    *,
    sem:     asyncio.Semaphore,
    delay:   float,
    label:   str,
    headers: dict | None = None,
) -> httpx.Response | None:
    """
    Rate-limited GET with automatic retry.

    Concurrency is bounded by ``sem``.  The semaphore is held for the full
    duration of the HTTP call **plus** ``delay`` seconds so back-pressure
    propagates to all waiting coroutines and the provider's request rate is
    honoured.  For 403/404 auth-failure responses the hold time is shortened
    to 50 ms because those responses don't count against the rate limit.

    Parameters
    ----------
    client  : Shared httpx.AsyncClient from the scheduler.
    url     : Full request URL.
    params  : Optional query-string parameters.
    sem     : Provider semaphore (AV_SEM, FINNHUB_SEM, …).
    delay   : Minimum seconds to hold the semaphore after a successful call.
    label   : Human-readable name for log messages (e.g. "AV RSI AAPL").
    headers : Optional extra headers (used for EDGAR User-Agent).

    Returns
    -------
    httpx.Response on any HTTP response (caller checks status_code).
    None on:
      - 429 exhausted after 3 retries
      - Timeout / ConnectError after 1 retry
      - Any unexpected exception
    """
    retries_429: int   = 0
    retries_net: int   = 0
    wait_before: float = 0.0

    while True:
        if wait_before > 0:
            await asyncio.sleep(wait_before)
            wait_before = 0.0

        # ── acquire semaphore, make request, hold delay ───────────────────
        async with sem:
            try:
                resp = await client.get(url, params=params, headers=headers)
                _hold = (
                    _BRIEF_DELAY
                    if resp.status_code in _AUTH_FAIL_CODES
                    else delay
                )
                await asyncio.sleep(_hold)

            except (httpx.TimeoutException, httpx.ConnectError) as exc:
                await asyncio.sleep(_BRIEF_DELAY)
                if retries_net < 1:
                    retries_net += 1
                    _log.warning(
                        "%s: %s — retrying in 2s", label, type(exc).__name__
                    )
                    wait_before = 2.0
                    continue   # releases sem, then sleeps 2s, then re-acquires
                job_counters.net_error_skips += 1
                _log.warning(
                    "%s: %s after retry, skipping", label, type(exc).__name__
                )
                return None

            except Exception as exc:
                await asyncio.sleep(_BRIEF_DELAY)
                _log.error("%s: unexpected error, skipping: %s", label, exc)
                return None

        # ── outside semaphore — handle 429 ────────────────────────────────
        if resp.status_code == 429:
            if retries_429 < len(_429_BACKOFF):
                wait = _429_BACKOFF[retries_429]
                retries_429 += 1
                _log.warning(
                    "%s: 429 rate limited — waiting %ds (retry %d/%d)",
                    label, wait, retries_429, len(_429_BACKOFF),
                )
                wait_before = float(wait)
                continue
            job_counters.rate_limit_skips += 1
            _log.warning("%s: 429 after %d retries, skipping", label, len(_429_BACKOFF))
            return None

        return resp
