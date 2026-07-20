"""
pipeline/scheduler.py

APScheduler configuration for the background pipeline (System A).

Architecture (Sprint 3: global scoring tick)
---------------------------------------------
Ingestion jobs fetch data from external APIs and write to the database.
They do NOT score.  A dedicated ``scoring_tick_job`` runs every 15 minutes
during market hours (weekdays 14:30-21:00 UTC) and every 30 minutes outside
market hours, recomputing all four layers for every ticker from current DB
state.

Ingestion jobs:

    market_job       — weekdays 14:30–21:00 UTC, every 15 minutes (data only)
    market_eod_job   — weekdays 21:15 UTC (data only, captures closing prices)
    narrative_job    — every 30 minutes (data only)
    influencer_job   — every 6 hours (data only)
    macro_daily_job    — daily at 02:00 UTC (FRED Treasury signals, data only)
    macro_intraday_job — hourly weekdays 14:00–20:00 UTC (VIX + sector ETFs, data only)
    short_volume_job — weekdays at 21:30 UTC (FINRA REGSHO, data only)

Scoring job:

    scoring_tick_job — every 15 min in market hours / 30 min off-hours; recomputes composite scores for all tickers

Usage
-----
    from pipeline.scheduler import scheduler
    scheduler.start()   # call from FastAPI lifespan
    scheduler.shutdown()
"""
from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone

import httpx
import yfinance as yf
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.combining import OrTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from tqdm import tqdm as _tqdm
from tqdm.asyncio import tqdm as _atqdm

from scripts.db.queries.raw_articles import purge_articles_before
from scripts.db.queries.raw_signals import OHLCV_SIGNAL_TYPES, purge_signals_before
from scripts.db.queries.sentiment_history import compact_drivers_before, get_baseline_scores
from scripts.db.queries.universe import get_active_tickers, get_ticker_sector_map
from scripts.db.redis import get_redis
from pipeline.features.normalize import log_scoring_telemetry, reset_scoring_telemetry
from pipeline.orchestrator import ScoreResult, _score_and_write
from pipeline.scoring.market_overview import (
    OVERVIEW_KEY,
    PERCENTILE_KEY,
    build_overview,
    compute_percentiles,
)
from pipeline.rate_limits import job_counters
from pipeline.sources.fred import fetch_fred_signals
from pipeline.sources.influencer import fetch_influencer_signals
from pipeline.sources.macro import fetch_macro_signals
from pipeline.sources.market import fetch_market_signals, to_yahoo_symbol
from pipeline.nlp.dedup import cluster_articles
from pipeline.sources.narrative import fetch_narrative_signals
from pipeline.sources.short_volume import ingest_short_volume

_log = logging.getLogger(__name__)

_RUN_KEY_TTL = 7 * 24 * 3600  # 7 days

# tqdm bar format used for both fetch and score phases
_BAR_FMT = "{desc}: {bar:20} {n}/{total} [{elapsed}<{remaining}, {rate_fmt}]{postfix}"


async def _get_cluster_telemetry(since_hours: float = 48.0) -> dict:
    """
    Query the DB for cluster source breakdown and compute telemetry summary.

    Returns dict with keys: cross_source_clusters, same_source_clusters,
    largest_cluster_size.  Called after the clustering phase completes.
    """
    from scripts.db.queries.raw_articles import get_cluster_source_breakdown

    cross_source = 0
    same_source = 0
    largest = 0

    try:
        clusters = await get_cluster_source_breakdown(since_hours)
        for c in clusters:
            sources = c.get("sources") or []
            count = c.get("article_count") or 0
            if len(sources) > 1:
                cross_source += 1
            else:
                same_source += 1
            if count > largest:
                largest = count
    except Exception as exc:
        _log.warning("_get_cluster_telemetry failed: %s", exc)

    return {
        "cross_source_clusters": cross_source,
        "same_source_clusters": same_source,
        "largest_cluster_size": largest,
    }


async def _record_run(job_id: str) -> None:
    """Write the current UTC timestamp to Redis for /v1/status."""
    try:
        now = datetime.now(timezone.utc).isoformat()
        await get_redis().set(f"pipeline:last_run:{job_id}", now, ex=_RUN_KEY_TTL)
    except Exception as exc:
        _log.warning("_record_run failed for %s: %s", job_id, exc)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _fetch_all_tickers(
    fetcher,
    tickers: list[str],
    client: httpx.AsyncClient,
    job_name: str = "",
) -> None:
    """
    Fetch signals for all tickers in parallel, throttled by per-source semaphores.

    A tqdm progress bar (written to stderr) shows live completion count, elapsed
    time, ETA, and rate-limit / network-error skip counts from job_counters.
    """
    desc = f"[{job_name}] fetch" if job_name else "fetch"
    pbar = _atqdm(
        total=len(tickers),
        desc=desc,
        unit="tkr",
        file=sys.stderr,
        dynamic_ncols=True,
        bar_format=_BAR_FMT,
    )

    async def _tracked(ticker: str) -> None:
        try:
            await fetcher(ticker, client)
        except Exception as exc:
            _log.warning("fetch error for %s: %s", ticker, exc)
        finally:
            pbar.update(1)
            pbar.set_postfix_str(
                f"✗ rl={job_counters.rate_limit_skips} net={job_counters.net_error_skips}",
                refresh=False,
            )

    await asyncio.gather(*[_tracked(t) for t in tickers])
    pbar.close()


_SCORE_SEM = asyncio.Semaphore(10)  # bound concurrent DB connections (asyncpg default pool=10)


async def _score_all(
    tickers: list[str],
    job_name: str,
    sector_map: dict[str, str] | None = None,
) -> tuple[int, int]:
    """
    Score all tickers concurrently, bounded by _SCORE_SEM to stay within
    the asyncpg connection pool limit.

    All four layers are recomputed from current DB state for every ticker.

    Parameters
    ----------
    sector_map : dict[str, str] | None
        Sprint P4.2 — preloaded ``{ticker: GICS sector}`` map. The macro
        layer routes per-ticker via the sector. ``None`` is allowed
        (each ticker is scored without sector context, producing a
        VIX-only macro sub-index — degraded behaviour but won't crash).

    Returns (fetched_count, total_layers_populated).
    """
    desc = f"[{job_name}] score" if job_name else "score"
    fetched = 0
    total_layers = 0

    # 1-day-change baselines (24-48h-old smoothed scores), one query for the
    # whole universe. A missing entry leaves that ticker's change fields null
    # (new ticker or data gap) — the change is never computed across a gap.
    try:
        baselines = await get_baseline_scores()
    except Exception as exc:
        _log.warning("%s: get_baseline_scores failed: %s — 1d changes null this tick",
                     job_name, exc)
        baselines = {}

    results: dict[str, ScoreResult] = {}

    pbar = _tqdm(
        total=len(tickers),
        desc=desc,
        unit="tkr",
        file=sys.stderr,
        dynamic_ncols=True,
        bar_format=_BAR_FMT,
    )

    async def _bounded(ticker: str) -> None:
        nonlocal fetched, total_layers
        sector = (sector_map or {}).get(ticker)
        async with _SCORE_SEM:
            try:
                result = await _score_and_write(ticker, sector, baselines.get(ticker.upper()))
                fetched += 1
                total_layers += result.n_populated
                results[ticker.upper()] = result
            except Exception as exc:
                _log.warning(
                    "%s: scoring failed for %s: %s", job_name, ticker, exc, exc_info=True
                )
            finally:
                pbar.update(1)
                avg = total_layers / fetched if fetched else 0
                pbar.set_postfix_str(f"✓ {fetched} avg={avg:.1f}/4", refresh=True)

    await asyncio.gather(*[_bounded(t) for t in tickers])
    pbar.close()

    await _publish_universe_stats(results, sector_map or {})

    return fetched, total_layers


async def _publish_universe_stats(
    results: dict[str, ScoreResult],
    sector_map: dict[str, str],
) -> None:
    """
    End-of-tick universe pass: percentiles + market-overview blob, computed
    from the in-memory results of the scoring pass and cached in Redis for
    the API (pro sentiment ``universe_percentile``; ``/v1/market/overview``).

    Only tickers scored THIS tick are included, so a ticker absent from the
    latest tick has no percentile (API returns null). Best-effort: a Redis
    failure logs a warning and never fails the scoring job.
    """
    if not results:
        return
    now = datetime.now(timezone.utc)
    scores      = {t: r.smoothed_score for t, r in results.items()}
    changes     = {t: r.score_change_1d for t, r in results.items()}
    change_pcts = {t: r.score_change_1d_pct for t, r in results.items()}

    pct_map  = compute_percentiles(scores)
    overview = build_overview(scores, changes, change_pcts, sector_map, now)

    try:
        client = get_redis()
        pipe = client.pipeline(transaction=True)
        pipe.delete(PERCENTILE_KEY)
        if pct_map:
            pipe.hset(PERCENTILE_KEY, mapping=pct_map)
        pipe.set(OVERVIEW_KEY, json.dumps(overview))
        await pipe.execute()
        _log.info(
            "universe stats published: %d percentiles, overview avg=%s",
            len(pct_map), overview.get("average_score"),
        )
    except Exception as exc:
        _log.warning("_publish_universe_stats: Redis write failed: %s", exc)


def _fmt_elapsed(seconds: float) -> str:
    """Format elapsed seconds as 'Xm Ys'."""
    mins, secs = divmod(int(seconds), 60)
    return f"{mins}m {secs}s"


# ---------------------------------------------------------------------------
# yfinance batch OHLCV download
# ---------------------------------------------------------------------------

async def _yf_batch_download(tickers: list[str]) -> dict[str, dict]:
    """
    Download OHLCV data for all tickers in a single yfinance batch call.

    Runs in a thread executor since yfinance is synchronous.

    Returns
    -------
    dict mapping ticker → {open, high, low, close, volume, timestamp, source}.
    Tickers that fail parsing are silently omitted (logged at WARNING).
    """
    loop = asyncio.get_running_loop()

    # Yahoo uses dashed class-share symbols (BRK-B); the universe uses dots
    # (BRK.B). Download under Yahoo symbols, key results by universe symbol.
    yahoo_symbols = [to_yahoo_symbol(t) for t in tickers]

    def _download():
        return yf.download(
            tickers=yahoo_symbols,
            period="5d",
            interval="1d",
            group_by="ticker",
            auto_adjust=True,
            threads=True,
            progress=False,
        )

    try:
        raw = await loop.run_in_executor(None, _download)
    except Exception as exc:
        _log.warning("yfinance batch download failed: %s", exc)
        return {}

    if raw.empty:
        _log.warning("yfinance batch download returned empty DataFrame")
        return {}

    result: dict[str, dict] = {}
    for ticker in tickers:
        try:
            if len(tickers) == 1:
                df = raw
            else:
                df = raw[to_yahoo_symbol(ticker)]

            if df.empty:
                continue

            valid = df.dropna(subset=["Close"])
            if valid.empty:
                continue

            last = valid.iloc[-1]
            ts = last.name.to_pydatetime()
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)

            result[ticker] = {
                "open":      float(last["Open"]),
                "high":      float(last["High"]),
                "low":       float(last["Low"]),
                "close":     float(last["Close"]),
                "volume":    float(last["Volume"]),
                "timestamp": ts,
                "source":    "yfinance",
            }
        except Exception as exc:
            _log.warning("yfinance parse error for %s: %s", ticker, exc)

    _log.info("yfinance batch: %d/%d tickers parsed", len(result), len(tickers))
    return result


# ---------------------------------------------------------------------------
# Job functions
# ---------------------------------------------------------------------------

async def market_job() -> None:
    """
    Market layer job — weekdays 14:30–21:00 UTC, every 15 minutes.

    Data-only: batch-downloads OHLCV for all tickers via yfinance in one
    call, then fetches RSI and derived signals per-ticker in parallel.
    Scoring is handled by the global scoring_tick_job.
    """
    job_counters.reset()
    t_start = time.monotonic()
    _log.info("market_job: starting")
    tickers = await get_active_tickers()
    n = len(tickers)
    _log.info("market_job: %d tickers", n)

    # Batch-download OHLCV for all tickers in one yfinance call
    ohlcv_batch = await _yf_batch_download(tickers)

    async def _market_fetcher(ticker: str, client: httpx.AsyncClient) -> None:
        await fetch_market_signals(ticker, client, ohlcv_batch=ohlcv_batch)

    async with httpx.AsyncClient(timeout=30) as client:
        await _fetch_all_tickers(_market_fetcher, tickers, client, job_name="MARKET")

    await _record_run("market")

    elapsed = time.monotonic() - t_start
    _log.info(
        "market_job complete: %d tickers fetched in %.1fs, %d rate-limit skips, %d net-error skips",
        n, elapsed, job_counters.rate_limit_skips, job_counters.net_error_skips,
    )


async def market_eod_job() -> None:
    """
    End-of-day market job — weekdays at 21:15 UTC (15 min after close).

    Data-only: captures definitive closing prices for all tickers.  The
    next scoring_tick_job (~21:30 UTC) picks these up and produces the EOD
    scored state so Redis holds a valid market sub-index before the
    overnight / weekend period begins.
    """
    job_counters.reset()
    t_start = time.monotonic()
    _log.info("market_eod_job: starting")
    tickers = await get_active_tickers()
    n = len(tickers)
    _log.info("market_eod_job: %d tickers", n)

    ohlcv_batch = await _yf_batch_download(tickers)

    async def _market_fetcher(ticker: str, client: httpx.AsyncClient) -> None:
        await fetch_market_signals(ticker, client, ohlcv_batch=ohlcv_batch)

    async with httpx.AsyncClient(timeout=30) as client:
        await _fetch_all_tickers(_market_fetcher, tickers, client, job_name="MARKET_EOD")

    await _record_run("market_eod")

    elapsed = time.monotonic() - t_start
    _log.info(
        "market_eod_job complete: %d tickers fetched in %.1fs, %d rate-limit skips, %d net-error skips",
        n, elapsed, job_counters.rate_limit_skips, job_counters.net_error_skips,
    )


async def narrative_job() -> None:
    """
    Narrative layer job — every 30 minutes.

    Three phases:
      1. Fetch: ingests news articles from AV + Finnhub for all tickers (data-only).
      2. Cluster: runs semantic dedup (Stage 2) on unclustered articles per ticker.
      3. FinBERT: scores unscored English articles with ProsusAI/finbert (Sprint A).

    Scoring is handled by the global scoring_tick_job.
    """
    job_counters.reset()
    t_start = time.monotonic()
    _log.info("narrative_job: starting")
    tickers = await get_active_tickers()
    n = len(tickers)

    # ── Phase 1: Fetch articles ────────────────────────────────────────────
    async with httpx.AsyncClient(timeout=30) as client:
        await _fetch_all_tickers(fetch_narrative_signals, tickers, client, job_name="NARRATIVE")

    fetch_elapsed = time.monotonic() - t_start

    # ── Phase 2: Semantic clustering (Sprint 6, G-C6) ─────────────────────
    from scripts.db.queries.raw_articles import count_unclustered_articles

    t_cluster_start = time.monotonic()
    total_clusters = 0
    tickers_with_articles = 0

    try:
        total_articles = await count_unclustered_articles(since_hours=48.0)
    except Exception as exc:
        _log.warning("narrative_job: count_unclustered_articles failed: %s", exc)
        total_articles = 0

    for ticker in tickers:
        try:
            n_clusters = await cluster_articles(ticker)
            total_clusters += n_clusters
            if n_clusters > 0:
                tickers_with_articles += 1
        except Exception as exc:
            _log.warning("narrative_job: cluster_articles(%s) failed: %s", ticker, exc)

    cluster_elapsed = time.monotonic() - t_cluster_start

    # ── Telemetry ──────────────────────────────────────────────────────────
    telemetry = await _get_cluster_telemetry(since_hours=48.0)
    _log.info(
        "narrative_job dedup: total_articles_processed=%d total_clusters_formed=%d "
        "cross_source_clusters=%d same_source_clusters=%d largest_cluster_size=%d "
        "tickers_processed=%d elapsed_seconds=%.1f",
        total_articles,
        total_clusters,
        telemetry["cross_source_clusters"],
        telemetry["same_source_clusters"],
        telemetry["largest_cluster_size"],
        tickers_with_articles,
        cluster_elapsed,
    )

    # ── Phase 3: FinBERT scoring (Sprint A) ───────────────────────────────
    from scripts.db.queries.raw_articles import get_unscored_articles, update_finbert_scores
    from pipeline.nlp.finbert import score_batch as finbert_score_batch

    t_finbert_start = time.monotonic()
    n_scored = 0

    try:
        unscored = await get_unscored_articles(since_hours=48.0, language="en", limit=500)
        if unscored:
            texts = [
                (a["title"] or "") + " " + (a["summary"] or "")
                for a in unscored
            ]
            # Sync torch inference (up to 500 articles) — run off the event
            # loop so the in-process FastAPI keeps serving requests.
            scores = await asyncio.to_thread(finbert_score_batch, texts)
            rows = [
                (a["id"], s["finbert_score"], s["finbert_pos"],
                 s["finbert_neg"], s["finbert_neu"])
                for a, s in zip(unscored, scores)
            ]
            await update_finbert_scores(rows)
            n_scored = len(rows)
    except Exception as exc:
        _log.warning("narrative_job: FinBERT scoring failed: %s", exc, exc_info=True)

    finbert_elapsed = time.monotonic() - t_finbert_start
    _log.info(
        "narrative_job finbert: scored=%d elapsed_seconds=%.1f",
        n_scored, finbert_elapsed,
    )

    await _record_run("narrative")

    elapsed = time.monotonic() - t_start
    _log.info(
        "narrative_job complete: %d tickers fetched in %.1fs, %d rate-limit skips, %d net-error skips, "
        "%d clusters in %.1fs, %d finbert-scored in %.1fs",
        n, fetch_elapsed, job_counters.rate_limit_skips, job_counters.net_error_skips,
        total_clusters, cluster_elapsed, n_scored, finbert_elapsed,
    )


async def influencer_job() -> None:
    """
    Influencer layer job — every 6 hours.

    Data-only: fetches SEC insider filings and analyst signals for all
    tickers and writes to DB.  Scoring is handled by the global
    scoring_tick_job.
    """
    job_counters.reset()
    t_start = time.monotonic()
    _log.info("influencer_job: starting")
    tickers = await get_active_tickers()
    n = len(tickers)

    async with httpx.AsyncClient(timeout=30) as client:
        await _fetch_all_tickers(fetch_influencer_signals, tickers, client, job_name="INFLUENCER")

    await _record_run("influencer")

    elapsed = time.monotonic() - t_start
    _log.info(
        "influencer_job complete: %d tickers fetched in %.1fs, %d rate-limit skips, %d net-error skips",
        n, elapsed, job_counters.rate_limit_skips, job_counters.net_error_skips,
    )


async def macro_daily_job() -> None:
    """
    Macro daily job — runs once per day at 02:00 UTC. Sprint P4.3.

    Fetches the three FRED Treasury / yield-curve signals (10y, 2y, TED
    substitute) and writes them under `_MACRO_`. VIX and sector ETF data
    are now fetched on an intraday cadence by ``macro_intraday_job``.

    Data-only: scoring is handled by the global ``scoring_tick_job``.
    """
    job_counters.reset()
    t_start = time.monotonic()
    _log.info("macro_daily_job: starting")

    n_rows = 0
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            n_rows = await fetch_fred_signals(client)
        except Exception as exc:
            _log.warning("macro_daily_job: FRED fetch failed: %s", exc, exc_info=True)

    await _record_run("macro_daily")

    elapsed = time.monotonic() - t_start
    _log.info("macro_daily_job complete: %d FRED rows in %.1fs", n_rows, elapsed)


async def macro_intraday_job() -> None:
    """
    Macro intraday job — hourly weekdays 14:00–20:00 UTC. Sprint P4.3.

    Fetches VIX + the 11 sector ETF closes / 20-day returns on every
    trading-hour tick, matching the paper's hourly-during-market-hours
    cadence for these market-state signals.

    Data-only: scoring is handled by the global ``scoring_tick_job``.
    """
    job_counters.reset()
    t_start = time.monotonic()
    _log.info("macro_intraday_job: starting")

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            await fetch_macro_signals(client)
        except Exception as exc:
            _log.warning("macro_intraday_job: macro fetch failed: %s", exc, exc_info=True)

    await _record_run("macro_intraday")

    elapsed = time.monotonic() - t_start
    _log.info("macro_intraday_job complete in %.1fs", elapsed)


async def short_volume_job() -> None:
    """
    Short volume job — weekdays at 21:30 UTC (30 min after market close).

    Fetches the latest FINRA REGSHO daily short volume file, filters to the
    active ticker universe, and writes three signals per ticker:
    short_volume_otc, short_volume_total_otc, short_volume_ratio_otc.

    This is a data-ingestion-only job — the signal types are not yet
    registered in the scoring pipeline.
    """
    t_start = time.monotonic()
    _log.info("short_volume_job: starting")

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            n_tickers = await ingest_short_volume(client)
    except Exception as exc:
        _log.warning("short_volume_job: failed: %s", exc, exc_info=True)
        n_tickers = 0

    await _record_run("short_volume")

    elapsed = time.monotonic() - t_start
    _log.info("short_volume_job complete: %d tickers in %.1fs", n_tickers, elapsed)


async def scoring_tick_job() -> None:
    """
    Global scoring tick — every 15 minutes during market hours
    (weekdays 14:30-21:00 UTC), every 30 minutes outside market hours.

    Recomputes all four layers (market, narrative, influencer, macro) for
    every active ticker from current DB state.  This is the ONLY job that
    calls _score_all(); ingestion jobs are data-only.
    """
    t_start = time.monotonic()
    _log.info("scoring_tick_job: starting")
    reset_scoring_telemetry()
    tickers = await get_active_tickers()
    n = len(tickers)

    # Sprint P4.2: preload the ticker→GICS-sector map once per tick so the
    # per-ticker macro routing in _score_macro doesn't fire 502 DB queries.
    try:
        sector_map = await get_ticker_sector_map()
        _log.info(
            "scoring_tick_job: loaded sector map for %d/%d tickers",
            len(sector_map), n,
        )
    except Exception as exc:
        _log.warning(
            "scoring_tick_job: get_ticker_sector_map failed: %s — falling back to VIX-only macro",
            exc,
        )
        sector_map = {}

    fetched, total_layers = await _score_all(tickers, "SCORING_TICK", sector_map=sector_map)
    log_scoring_telemetry()
    await _record_run("scoring_tick")

    elapsed = time.monotonic() - t_start
    avg = total_layers / fetched if fetched else 0
    _log.info(
        "scoring_tick_job complete: %d/%d scored, avg %.1f/4 layers in %.1fs",
        fetched, n, avg, elapsed,
    )
    if elapsed > 300:
        _log.warning(
            "scoring_tick_job exceeded 5-minute threshold: %.1fs elapsed", elapsed
        )


OHLCV_RETENTION_DAYS = 365
SIGNAL_RETENTION_DAYS = 90
ARTICLE_RETENTION_DAYS = 30
DRIVER_COMPACT_DAYS = 30


async def retention_job() -> None:
    """Daily retention job — runs at 03:30 UTC.

    Three deletes:
      - raw_signals OHLCV rows older than 365 days
      - raw_signals non-OHLCV rows older than 90 days
      - raw_articles older than 30 days

    Plus one in-place rewrite: sentiment_history `top_drivers` older than
    30 days are re-encoded to the compact array format (description dropped
    — originals archived offline; see pipeline/scoring/driver_codec.py).

    Deliberately does NOT delete rows from `sentiment_history` or
    `price_snapshots`: both are the research paper's raw material (score
    time series and the tick-aligned closes for sentiment-vs-price
    analysis) and grow unboundedly by design. Revisit after the paper is
    published.
    """
    t_start = time.monotonic()
    now = datetime.now(timezone.utc)
    ohlcv_cutoff   = now - timedelta(days=OHLCV_RETENTION_DAYS)
    signal_cutoff  = now - timedelta(days=SIGNAL_RETENTION_DAYS)
    article_cutoff = now - timedelta(days=ARTICLE_RETENTION_DAYS)
    compact_cutoff = now - timedelta(days=DRIVER_COMPACT_DAYS)

    _log.info("retention_job: starting")

    n_ohlcv = n_signals = n_articles = n_compacted = 0
    try:
        n_ohlcv = await purge_signals_before(ohlcv_cutoff, OHLCV_SIGNAL_TYPES)
    except Exception as exc:
        _log.warning("retention_job: OHLCV purge failed: %s", exc, exc_info=True)

    try:
        n_signals = await purge_signals_before(
            signal_cutoff, OHLCV_SIGNAL_TYPES, exclude=True,
        )
    except Exception as exc:
        _log.warning("retention_job: non-OHLCV purge failed: %s", exc, exc_info=True)

    try:
        n_articles = await purge_articles_before(article_cutoff)
    except Exception as exc:
        _log.warning("retention_job: articles purge failed: %s", exc, exc_info=True)

    try:
        n_compacted = await compact_drivers_before(compact_cutoff)
    except Exception as exc:
        _log.warning("retention_job: driver compaction failed: %s", exc, exc_info=True)

    await _record_run("retention")

    elapsed = time.monotonic() - t_start
    _log.info(
        "retention_job complete: ohlcv=%d (>%dd), other_signals=%d (>%dd), "
        "articles=%d (>%dd), drivers_compacted=%d (>%dd) in %.1fs",
        n_ohlcv, OHLCV_RETENTION_DAYS,
        n_signals, SIGNAL_RETENTION_DAYS,
        n_articles, ARTICLE_RETENTION_DAYS,
        n_compacted, DRIVER_COMPACT_DAYS,
        elapsed,
    )


# ---------------------------------------------------------------------------
# Scheduler instance
# ---------------------------------------------------------------------------

scheduler = AsyncIOScheduler(timezone="UTC")

scheduler.add_job(
    market_job,
    trigger=CronTrigger(day_of_week="mon-fri", hour="14-20", minute="*/15"),
    id="market",
    name="Market data (15 min, 14:30-21:00 UTC)",
    max_instances=1,
    coalesce=True,
    misfire_grace_time=60,
)

scheduler.add_job(
    market_eod_job,
    trigger=CronTrigger(day_of_week="mon-fri", hour=21, minute=15),
    id="market_eod",
    name="Market EOD snapshot (21:15 UTC weekdays)",
    max_instances=1,
    coalesce=True,
    misfire_grace_time=300,
)

scheduler.add_job(
    narrative_job,
    trigger=IntervalTrigger(minutes=30),
    id="narrative",
    name="Narrative sentiment (30 min)",
    max_instances=1,
    coalesce=True,
    misfire_grace_time=120,
)

scheduler.add_job(
    influencer_job,
    trigger=IntervalTrigger(hours=6),
    id="influencer",
    name="Influencer activity (6 h)",
    max_instances=1,
    coalesce=True,
    misfire_grace_time=300,
)

scheduler.add_job(
    macro_daily_job,
    trigger=CronTrigger(hour=2, minute=0),
    id="macro_daily",
    name="Macro daily (FRED Treasury, 02:00 UTC)",
    max_instances=1,
    coalesce=True,
    misfire_grace_time=600,
)

scheduler.add_job(
    macro_intraday_job,
    trigger=CronTrigger(day_of_week="mon-fri", hour="14-20", minute=0),
    id="macro_intraday",
    name="Macro intraday (VIX + ETFs, hourly weekdays 14:00–20:00 UTC)",
    max_instances=1,
    coalesce=True,
    misfire_grace_time=300,
)

scheduler.add_job(
    short_volume_job,
    trigger=CronTrigger(day_of_week="mon-fri", hour=21, minute=30),
    id="short_volume",
    name="FINRA short volume (21:30 UTC weekdays)",
    max_instances=1,
    coalesce=True,
    misfire_grace_time=600,
)

scheduler.add_job(
    scoring_tick_job,
    trigger=OrTrigger([
        # Base cadence: every 30 minutes around the clock (:00 and :30).
        CronTrigger(minute="0,30"),
        # Market-hours fills (mon-fri 14:30-21:00 UTC): add :15 and :45 marks
        # so the effective cadence is 15 min while markets are open.
        CronTrigger(day_of_week="mon-fri", hour=14, minute=45),
        CronTrigger(day_of_week="mon-fri", hour="15-20", minute="15,45"),
    ]),
    id="scoring_tick",
    name="Global scoring tick (15 min market hours, 30 min off-hours)",
    max_instances=1,
    coalesce=True,
    misfire_grace_time=120,
)

scheduler.add_job(
    retention_job,
    trigger=CronTrigger(hour=3, minute=30),
    id="retention",
    name="Data retention (daily 03:30 UTC)",
    max_instances=1,
    coalesce=True,
    misfire_grace_time=600,
)
