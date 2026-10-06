"""
scripts/backfill/news_backfill.py

Backfill news articles for a past window (built for the 2026-08-10 → 10-02
narrative outage), fully processed BEFORE insert:

    fetch (AV NEWS_SENTIMENT + Finnhub company-news, date-windowed)
      → drop hashes already stored → language detect
      → FinBERT (English only) → semantic clustering (in memory)
      → bulk insert with finbert_* + event_cluster_id populated

Processing before insert matters: the live job only clusters/scores rows
<48h old, and retention blanks title/summary of rows >30 days old every
night — backfilled August articles would otherwise never be scored.

A final pass also FinBERT-scores any already-stored English articles in the
window that are still unscored (the restarted live job's 3-day catch-up
exceeds its 500-articles/run cap).

Usage
-----
    OMP_NUM_THREADS=1 TOKENIZERS_PARALLELISM=false \\
    python3 scripts/backfill/news_backfill.py --dry-run --tickers AAPL,BMY,ZTS
    python3 scripts/backfill/news_backfill.py            # full run, resumable

Rate limits: runs alongside the live narrative job on the SAME keys, so it
uses its own slower spacing (AV 2 s, Finnhub 3 s) and retries AV rate-limit
messages. Any other AV Information/Note message (invalid or unentitled key)
aborts the run.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import httpx
from dotenv import load_dotenv

# Ensure project root is importable when running as a script
_project_root = str(Path(__file__).resolve().parent.parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# Must precede the narrative import, which reads the API keys at import time.
load_dotenv(override=True)

from pipeline.sources.narrative import (  # noqa: E402
    AVLimitError,
    _detect_language,
    fetch_av_news_window,
    fetch_finnhub_news_window,
)
from scripts.db.connection import close_pool, init_pool  # noqa: E402
from scripts.db.queries import raw_articles as ra  # noqa: E402
from scripts.db.queries.universe import get_universe_as_of  # noqa: E402

_log = logging.getLogger("news_backfill")

DEFAULT_START = datetime(2026, 8, 7, tzinfo=timezone.utc)
DEFAULT_END = datetime(2026, 10, 2, 9, 0, tzinfo=timezone.utc)

AV_DELAY_S = 2.0
FINNHUB_DELAY_S = 3.0
AV_PAGE_LIMIT = 1000  # AV NEWS_SENTIMENT max; a full page means "maybe truncated"
FINNHUB_CAP = 240  # a window returning >= this many is split (verify in dry run)
FINNHUB_WINDOW = timedelta(days=7)
MIN_SPLIT = timedelta(hours=1)
CLUSTER_WINDOW_H = 4.0
FINBERT_BATCH = 32

_PROGRESS_DIR = Path(_project_root) / "exports"


def _progress_file(run_id: str) -> Path:
    """Per-run progress (a later run must not inherit an earlier run's done list)."""
    legacy = _PROGRESS_DIR / "news_backfill_progress.json"  # run news-backfill-2026-10
    return (
        legacy
        if run_id == "news-backfill-2026-10"
        else _PROGRESS_DIR / f"news_backfill_progress_{run_id}.json"
    )


class AbortBackfill(RuntimeError):
    """Unrecoverable provider error (e.g. invalid AV key)."""


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------


def _is_av_rate_limit(message: str) -> bool:
    m = message.lower()
    return "rate limit" in m or "frequency" in m or "per minute" in m or "per day" in m


async def _av_window(
    ticker: str, client: httpx.AsyncClient, start: datetime, end: datetime, stats: dict
) -> list[dict]:
    """AV articles in [start, end), bisecting any window that fills a page."""
    for attempt in range(6):
        try:
            arts = await fetch_av_news_window(
                ticker,
                client,
                time_from=start,
                time_to=end,
                limit=AV_PAGE_LIMIT,
                delay=AV_DELAY_S,
            )
            break
        except AVLimitError as exc:
            if not _is_av_rate_limit(str(exc)):
                raise AbortBackfill(f"Alpha Vantage refused the request: {exc}") from exc
            wait = 30 * (attempt + 1)
            _log.warning("AV rate limit (%s) — retrying in %ds", ticker, wait)
            await asyncio.sleep(wait)
    else:
        stats["failed_windows"] += 1
        return []
    if arts is None:
        stats["failed_windows"] += 1
        return []

    stats["av_calls"] += 1
    stats["av_max_page"] = max(stats["av_max_page"], len(arts))
    if len(arts) >= AV_PAGE_LIMIT and end - start > MIN_SPLIT:
        mid = start + (end - start) / 2
        return await _av_window(ticker, client, start, mid, stats) + await _av_window(
            ticker, client, mid, end, stats
        )
    return arts


async def _finnhub_window(
    ticker: str, client: httpx.AsyncClient, start: date, end: date, stats: dict
) -> list[dict]:
    """Finnhub articles for calendar dates [start, end], splitting capped windows."""
    arts = None
    for attempt in range(3):
        arts = await fetch_finnhub_news_window(ticker, client, start, end, delay=FINNHUB_DELAY_S)
        if arts is not None:
            break
        await asyncio.sleep(10 * (attempt + 1))
    if arts is None:
        stats["failed_windows"] += 1
        return []

    stats["fh_calls"] += 1
    stats["fh_max_page"] = max(stats["fh_max_page"], len(arts))
    if len(arts) >= FINNHUB_CAP and end > start:
        mid = start + (end - start) // 2
        return await _finnhub_window(ticker, client, start, mid, stats) + await _finnhub_window(
            ticker, client, mid + timedelta(days=1), end, stats
        )
    return arts


async def _fetch_ticker(
    ticker: str, client: httpx.AsyncClient, start: datetime, end: datetime, stats: dict
) -> list[dict]:
    """All articles for ticker in [start, end), AV first (wins URL ties)."""
    av = await _av_window(ticker, client, start, end, stats)

    fh: list[dict] = []
    w = start
    while w < end:
        w_end = min(w + FINNHUB_WINDOW, end)
        fh += await _finnhub_window(
            ticker, client, w.date(), (w_end - timedelta(microseconds=1)).date(), stats
        )
        w = w_end

    seen: set[str] = set()
    out: list[dict] = []
    for a in av + fh:
        if a["content_hash"] in seen or not (start <= a["published_at"] < end):
            continue
        seen.add(a["content_hash"])
        out.append(a)
    stats["fetched_av"] += sum(1 for a in out if a["source"] == "alpha_vantage")
    stats["fetched_fh"] += sum(1 for a in out if a["source"] == "finnhub")
    return out


# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------


def _finbert_text(a: dict) -> str:
    # Same composition as narrative_job Phase 3.
    return (a["title"] or "") + " " + (a["summary"] or "")


async def _score_and_cluster(
    ticker: str, new: list[dict], repair: list[dict], start: datetime, end: datetime
) -> list[tuple[list[int], str]]:
    """
    Fill language + FinBERT in place on ``new`` and on ``repair`` (stored but
    unscored rows, carrying their stored ``id``), using the fetched text. Then
    cluster new articles together with stored unclustered ones near the
    window (repair rows join with their fetched title — the stored one may be
    blanked). Returns (stored_ids, cluster_id) pairs to assign.
    """
    from pipeline.nlp.dedup import _get_model, cluster_members
    from pipeline.nlp.finbert import score_batch

    for a in new + repair:
        a["language"] = _detect_language(_finbert_text(a))
    english = [a for a in new + repair if a["language"] == "en"]
    if english:
        scores = await asyncio.to_thread(
            score_batch,
            [_finbert_text(a) for a in english],
            FINBERT_BATCH,
        )
        for a, s in zip(english, scores):
            a.update(s)

    pad = timedelta(hours=CLUSTER_WINDOW_H)
    stored = await ra.get_unclustered_articles_between(ticker, start - pad, end + pad)
    stored_ids = {r["id"] for r in stored}
    pool = [{"title": a["title"], "published_at": a["published_at"], "new": a} for a in new]
    pool += [
        {"title": r["title"], "published_at": r["published_at"], "id": r["id"]} for r in stored
    ]
    pool += [
        {"title": a["title"], "published_at": a["published_at"], "id": a["id"]}
        for a in repair
        if not a["clustered"] and a["id"] not in stored_ids
    ]
    if len(pool) < 2:
        return []

    embeddings = await asyncio.to_thread(
        lambda: _get_model().encode(
            [p["title"] or "" for p in pool],
            normalize_embeddings=True,
            show_progress_bar=False,
            batch_size=64,
        )
    )
    existing_assignments: list[tuple[list[int], str]] = []
    for members in cluster_members(pool, embeddings, CLUSTER_WINDOW_H):
        cluster_id = str(uuid.uuid4())
        stored_ids = []
        for i in members:
            if "new" in pool[i]:
                pool[i]["new"]["event_cluster_id"] = cluster_id
            else:
                stored_ids.append(pool[i]["id"])
        if stored_ids:
            existing_assignments.append((stored_ids, cluster_id))
    return existing_assignments


async def _backfill_ticker(
    ticker: str,
    client: httpx.AsyncClient,
    start: datetime,
    end: datetime,
    dry_run: bool,
    stats: dict,
) -> None:
    failed_before = stats["failed_windows"]
    articles = await _fetch_ticker(ticker, client, start, end, stats)
    if stats["failed_windows"] > failed_before:
        # Some window came back empty after retries — insert what we have but
        # don't mark the ticker done, so a rerun fetches it again (idempotent).
        _log.warning(
            "%s: %d fetch window(s) failed — will retry on rerun",
            ticker,
            stats["failed_windows"] - failed_before,
        )
        stats["incomplete"].add(ticker)
    have = await ra.existing_articles(ticker, [a["content_hash"] for a in articles])
    new = [a for a in articles if a["content_hash"] not in have]
    repair = [
        {
            **a,
            "id": have[a["content_hash"]]["id"],
            "clustered": have[a["content_hash"]]["clustered"],
        }
        for a in articles
        if a["content_hash"] in have and not have[a["content_hash"]]["scored"]
    ]
    stats["new"] += len(new)
    stats["already_stored"] += len(articles) - len(new)
    stats["repairable"] += len(repair)

    if dry_run:
        # Time FinBERT on a small sample instead of scoring everything.
        from pipeline.nlp.finbert import score_batch

        sample = [_finbert_text(a) for a in new[:64]]
        if sample:
            t = time.monotonic()
            await asyncio.to_thread(score_batch, sample, FINBERT_BATCH)
            stats["finbert_s_per_article"] = (time.monotonic() - t) / len(sample)
        return

    for a in new:
        a["ingest_run"] = stats["run_id"]
    existing_assignments = await _score_and_cluster(ticker, new, repair, start, end)
    await ra.insert_scored_articles(new)
    repaired = [
        (a["id"], a["finbert_score"], a["finbert_pos"], a["finbert_neg"], a["finbert_neu"])
        for a in repair
        if a.get("finbert_score") is not None
    ]
    await ra.update_finbert_scores(repaired)
    stats["repaired"] += len(repaired)
    for ids, cluster_id in existing_assignments:
        await ra.set_cluster_ids(ids, cluster_id)
    stats["inserted"] += len(new)
    stats["finbert_scored"] += sum(1 for a in new if a.get("finbert_score") is not None)
    stats["clustered"] += sum(1 for a in new if a.get("event_cluster_id"))


async def _score_stored_backlog(start: datetime, end: datetime) -> int:
    """FinBERT-score stored English articles in the window that are still unscored."""
    from pipeline.nlp.finbert import score_batch

    total = 0
    while True:
        rows = await ra.get_unscored_articles_between(start, end, "en", limit=2000)
        if not rows:
            return total
        scores = await asyncio.to_thread(
            score_batch,
            [_finbert_text(r) for r in rows],
            FINBERT_BATCH,
        )
        await ra.update_finbert_scores(
            [
                (r["id"], s["finbert_score"], s["finbert_pos"], s["finbert_neg"], s["finbert_neu"])
                for r, s in zip(rows, scores)
            ]
        )
        total += len(rows)
        _log.info("stored backlog: %d articles scored so far", total)


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def _load_progress(run_id: str) -> set[str]:
    try:
        return set(json.loads(_progress_file(run_id).read_text())["done"])
    except (FileNotFoundError, KeyError, ValueError):
        return set()


def _save_progress(done: set[str], run_id: str) -> None:
    path = _progress_file(run_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"done": sorted(done)}))


def _parse_ts(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


async def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--start", type=_parse_ts, default=DEFAULT_START, help="UTC, ISO-8601")
    p.add_argument("--end", type=_parse_ts, default=DEFAULT_END, help="UTC, ISO-8601 (exclusive)")
    p.add_argument(
        "--universe-as-of",
        type=_parse_ts,
        default=None,
        help="tickers that existed at this time (default: --start); "
        "retired symbols trading then are included, later additions excluded",
    )
    p.add_argument(
        "--tickers", help="comma-separated subset (default: universe as of --universe-as-of)"
    )
    p.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="tickers in flight (provider spacing still serializes each API)",
    )
    p.add_argument("--dry-run", action="store_true", help="fetch + report only; no DB writes")
    p.add_argument(
        "--run-id",
        default="news-backfill",
        help="raw_articles.ingest_run tag for inserted rows (migration 016)",
    )
    p.add_argument(
        "--skip-backlog",
        action="store_true",
        help="skip FinBERT-scoring of already-stored unscored articles",
    )
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s"
    )
    # httpx logs full request URLs at INFO — those carry the API keys.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    # Bounded queries: a connection killed by laptop sleep must fail, not hang.
    await init_pool(command_timeout=120)
    try:
        tickers = (
            args.tickers.upper().split(",")
            if args.tickers
            else await get_universe_as_of(args.universe_as_of or args.start)
        )
        done = set() if args.dry_run else _load_progress(args.run_id)
        todo = [t for t in tickers if t not in done]
        _log.info(
            "window %s → %s | %d tickers (%d already done)%s",
            args.start.isoformat(),
            args.end.isoformat(),
            len(todo),
            len(tickers) - len(todo),
            " [DRY RUN]" if args.dry_run else "",
        )

        stats = dict.fromkeys(
            [
                "av_calls",
                "fh_calls",
                "av_max_page",
                "fh_max_page",
                "failed_windows",
                "fetched_av",
                "fetched_fh",
                "new",
                "already_stored",
                "inserted",
                "finbert_scored",
                "clustered",
                "repairable",
                "repaired",
            ],
            0,
        )
        stats["incomplete"] = set()
        stats["run_id"] = args.run_id
        sem = asyncio.Semaphore(args.concurrency)
        t0 = time.monotonic()

        async with httpx.AsyncClient(timeout=60) as client:

            async def _one(ticker: str) -> None:
                async with sem:
                    await _backfill_ticker(
                        ticker, client, args.start, args.end, args.dry_run, stats
                    )
                    if not args.dry_run and ticker not in stats["incomplete"]:
                        done.add(ticker)
                        _save_progress(done, args.run_id)
                    _log.info(
                        "%-6s done (%d/%d, %.0fs) new=%d inserted=%d",
                        ticker,
                        len(done) if not args.dry_run else 0,
                        len(tickers),
                        time.monotonic() - t0,
                        stats["new"],
                        stats["inserted"],
                    )

            results = await asyncio.gather(*[_one(t) for t in todo], return_exceptions=True)
            for t, r in zip(todo, results):
                if isinstance(r, AbortBackfill):
                    raise r
                if isinstance(r, BaseException):
                    _log.error("%s failed: %r — rerun to retry", t, r)

        if not args.dry_run and not args.skip_backlog:
            stats["backlog_scored"] = await _score_stored_backlog(args.start, args.end)

        stats["incomplete"] = sorted(stats["incomplete"])
        stats["elapsed_s"] = round(time.monotonic() - t0, 1)
        print(json.dumps(stats, indent=2, default=str))
        return 0
    except AbortBackfill as exc:
        _log.error("ABORTED: %s", exc)
        return 2
    finally:
        await close_pool()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
