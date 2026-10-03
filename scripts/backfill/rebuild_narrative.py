"""
scripts/backfill/rebuild_narrative.py

Rebuild the sentiment_history rows that were scored WITHOUT news during the
2026-08-10 → 2026-09-17 narrative outage, now that the articles have been
backfilled (scripts/backfill/news_backfill.py).

For each existing row at time t, in timestamp order per ticker:
  - narrative layer recomputed from the backfilled articles exactly as the
    live tick would have (3-day lookback, cluster dedup, ≤ t, 6h fallback to
    the previous row's narrative value)
  - market / influencer / macro sub-indices REUSED from the row (their raw
    intraday inputs are already past retention, and they were unaffected)
  - composite, divergence, exo composite, confidence, drivers, narrative
    surprise and the EMA chain recomputed
  - row UPDATEd in place and tagged replay_run (migration 014)

Everything is computed in memory from one bulk read per ticker — no per-row
queries (the DB is ~200 ms away when run locally).

Safety: the original rows are archived to exports/ BEFORE any update and the
run refuses to write without a verified archive.

Usage
-----
    python3 scripts/backfill/rebuild_narrative.py --dry-run --tickers AAPL
    python3 scripts/backfill/rebuild_narrative.py --archive-only
    python3 scripts/backfill/rebuild_narrative.py          # resumable by ticker
"""
from __future__ import annotations

import argparse
import asyncio
import bisect
import csv
import gzip
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

_project_root = str(Path(__file__).resolve().parent.parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

load_dotenv(override=True)
# Match prod (Railway) at the time of the rows. ema.py reads the half-life at
# import time, so these must be set before the pipeline imports below. Only a
# direct run changes the process env; importing (tests) leaves it alone.
if __name__ == "__main__":
    os.environ["EMA_HALF_LIFE_HOURS"] = "2"
    os.environ["ENABLE_NARRATIVE_SURPRISE"] = "1"

from pipeline.confidence.scorer import LOW_VOLUME_THRESHOLD, compute_confidence  # noqa: E402
from pipeline.confidence.staleness import STALENESS_THRESHOLDS, check_staleness  # noqa: E402
from pipeline.features import surprise  # noqa: E402
from pipeline.features.normalize import score_narrative_signals  # noqa: E402
from pipeline.scoring.composite import compute_composite, compute_exo_composite  # noqa: E402
from pipeline.scoring.divergence import compute_divergence  # noqa: E402
from pipeline.scoring.driver_codec import compact_drivers, expand_drivers, is_compact  # noqa: E402
from pipeline.scoring.drivers import extract_drivers  # noqa: E402
from pipeline.scoring.ema import compute_ema  # noqa: E402
from pipeline.scoring.subindices import SubIndexResult, compute_sub_index  # noqa: E402
from scripts.db.connection import close_pool, get_pool, init_pool  # noqa: E402
from scripts.db.queries.universe import get_universe_as_of  # noqa: E402

_log = logging.getLogger("rebuild_narrative")

RUN_ID      = "news-backfill-2026-10"
RANGE_START = datetime(2026, 8, 10, 4, 22, tzinfo=timezone.utc)
RANGE_END   = datetime(2026, 9, 17, 13, 42, 18, tzinfo=timezone.utc)   # inclusive

NARRATIVE_LOOKBACK = timedelta(days=3)    # orchestrator._NARRATIVE_SCORE_LOOKBACK
NARRATIVE_FALLBACK = timedelta(hours=6)   # orchestrator._LAYER_LOOKBACK["narrative"]
ARTICLE_PAD = (timedelta(hours=surprise.CURRENT_WINDOW_HOURS)
               + timedelta(days=surprise.BASELINE_DAYS) + timedelta(days=1))

ARCHIVE = Path(_project_root) / "exports" / "sentiment_history_pre_replay_20261002.csv.gz"
ARCHIVE_META = ARCHIVE.with_suffix("").with_suffix(".meta.json")

_ROW_COLS = (
    "id, timestamp, composite_score, market_index, narrative_index, "
    "influencer_index, macro_index, confidence_flags, top_drivers, divergence, "
    "narrative_as_of, composite_score_smoothed, replay_run"
)


# ---------------------------------------------------------------------------
# Pure helpers (unit-tested in tests/test_backfill_replay.py)
# ---------------------------------------------------------------------------

def select_articles(articles: list[dict], published: list[datetime],
                    since: datetime, until: datetime, *,
                    include_until: bool) -> list[dict]:
    """
    In-memory equivalent of get_articles_since / get_article_scores_between:
    articles published in [since, until] (or [since, until) when
    ``include_until`` is False), one per event cluster — highest relevance
    (NULLs last), then most recent. ``articles`` must be sorted by
    published_at ascending, with ``published`` the matching key list.
    """
    lo = bisect.bisect_left(published, since)
    hi = (bisect.bisect_right if include_until else bisect.bisect_left)(published, until)
    best: dict[str, dict] = {}
    for a in articles[lo:hi]:
        key = a["event_cluster_id"] or f"id:{a['id']}"
        cur = best.get(key)
        if cur is None or _dedup_rank(a) > _dedup_rank(cur):
            best[key] = a
    return sorted(best.values(), key=lambda a: a["published_at"])


def _dedup_rank(a: dict) -> tuple:
    rel = a["relevance_score"]
    return (rel is not None, rel if rel is not None else 0.0, a["published_at"])


def merge_drivers(stored: list | None, narrative: list[dict], top_n: int = 5) -> list[dict]:
    """
    Replace the narrative entries of a stored driver list with freshly
    computed ones and re-rank. A stored driver's importance is recoverable
    as magnitude × confidence (extract_drivers: magnitude = |score−50|/50,
    confidence = min(1, weight), importance = weight·|score−50|/50).
    """
    base = expand_drivers(stored) if is_compact(stored) else list(stored or [])
    pool = [d for d in base if d.get("source_layer") != "narrative"] + narrative
    pool.sort(key=lambda d: (d.get("magnitude") or 0) * (d.get("confidence") or 0),
              reverse=True)
    return pool[:top_n]


def rebuilt_stale_sources(orig_flags: list[str], narrative_as_of: datetime | None,
                          now: datetime) -> list[str]:
    """
    Keep the row's original non-news staleness verdicts (exact — they used
    the analyst/insider split that isn't stored) and recompute only news.
    Order follows STALENESS_THRESHOLDS, as the live scorer emits it.
    """
    orig = {f.split(":", 1)[1] for f in orig_flags if f.startswith("stale:")}
    news_stale = check_staleness({"news": narrative_as_of}, now=now)["news"]
    return [s for s in STALENESS_THRESHOLDS
            if (s == "news" and news_stale) or (s != "news" and s in orig)]


# ---------------------------------------------------------------------------
# Per-ticker rebuild
# ---------------------------------------------------------------------------

def _json(v):
    return json.loads(v) if isinstance(v, str) else v


def rebuild_rows(ticker: str, rows: list[dict], seed: dict | None,
                 articles: list[dict]) -> list[tuple]:
    """Recompute every row; returns UPDATE parameter tuples (see _UPDATE_SQL)."""
    articles = sorted(articles, key=lambda a: a["published_at"])
    published = [a["published_at"] for a in articles]

    prev_smoothed = seed["composite_score_smoothed"] if seed else None
    prev_ts       = seed["timestamp"] if seed else None
    prev_narr     = ((seed["narrative_index"], seed["narrative_as_of"])
                     if seed and seed["narrative_index"] is not None else None)
    out: list[tuple] = []

    for row in rows:
        t = row["timestamp"]

        # ── narrative layer, as orchestrator._score_narrative would ───────────
        narr_si, narr_sigs, narr_as_of = None, [], None
        arts = select_articles(articles, published, t - NARRATIVE_LOOKBACK, t,
                               include_until=True)
        if arts:
            sigs = score_narrative_signals(ticker, arts, t)
            si = compute_sub_index(sigs)
            if si is not None:
                narr_si, narr_sigs = si, sigs
                narr_as_of = max(a["published_at"] for a in arts)
        if narr_si is None and prev_narr and prev_narr[1] and t - prev_narr[1] <= NARRATIVE_FALLBACK:
            narr_si, narr_as_of = SubIndexResult(float(prev_narr[0]), 1, []), prev_narr[1]

        # ── composite / divergence / exo with the stored other layers ─────────
        sub = {
            "market":     row["market_index"],
            "narrative":  narr_si,
            "influencer": row["influencer_index"],
            "macro":      row["macro_index"],
        }
        composite = compute_composite(sub)
        present = {k: (v.value if hasattr(v, "value") else float(v))
                   for k, v in sub.items() if v is not None}
        div, effective = compute_divergence(present, composite.score)
        exo = compute_exo_composite(sub)

        dt_h = max(0.0, (t - prev_ts).total_seconds() / 3600.0) if prev_ts else 0.0
        smoothed = round(compute_ema(effective, prev_smoothed, dt_h), 2)

        # ── confidence ────────────────────────────────────────────────────────
        orig_flags = _json(row["confidence_flags"]) or []
        n_narr = sum(1 for s in narr_sigs if (s.get("weight") or 0) > 0)
        n_signals = n_narr if "low_signal_volume" in orig_flags else LOW_VOLUME_THRESHOLD + n_narr
        conf = compute_confidence(
            missing_layers=composite.missing_layers,
            stale_sources=rebuilt_stale_sources(orig_flags, narr_as_of, t),
            n_signals=n_signals,
            divergence_flag=div.flag,
        )

        # ── drivers ───────────────────────────────────────────────────────────
        stored = _json(row["top_drivers"])
        drivers = merge_drivers(stored, [d.to_dict() for d in extract_drivers(narr_sigs)])
        if is_compact(stored):
            drivers = compact_drivers(drivers)

        # ── narrative surprise (prod flag on) ─────────────────────────────────
        cur_start = t - timedelta(hours=surprise.CURRENT_WINDOW_HOURS)
        n_surprise = surprise.surprise_from_rows(
            select_articles(articles, published, cur_start, t, include_until=False),
            select_articles(articles, published,
                            cur_start - timedelta(days=surprise.BASELINE_DAYS), cur_start,
                            include_until=False),
        )

        out.append((
            row["id"],
            round(effective, 2),
            round(narr_si.value, 2) if narr_si is not None else None,
            smoothed,
            round(exo.score, 2) if exo is not None else None,
            conf.score,
            json.dumps(conf.flags),
            div.flag,
            narr_as_of,
            json.dumps(drivers),
            n_surprise,
            RUN_ID,
        ))
        prev_smoothed, prev_ts = smoothed, t
        if narr_si is not None:
            prev_narr = (narr_si.value, narr_as_of)

    return out


_UPDATE_SQL = """
    UPDATE sentiment_history
       SET composite_score          = $2,
           narrative_index          = $3,
           composite_score_smoothed = $4,
           composite_score_exo      = $5,
           confidence_score         = $6,
           confidence_flags         = $7::jsonb,
           divergence               = $8,
           narrative_as_of          = $9,
           top_drivers              = $10::jsonb,
           narrative_surprise       = $11,
           replay_run               = $12
     WHERE id = $1
"""


async def _load_ticker(conn, ticker: str):
    rows = await conn.fetch(
        f"SELECT {_ROW_COLS} FROM sentiment_history "
        "WHERE ticker = $1 AND timestamp >= $2 AND timestamp <= $3 ORDER BY timestamp",
        ticker, RANGE_START, RANGE_END,
    )
    seed = await conn.fetchrow(
        "SELECT timestamp, composite_score_smoothed, narrative_index, narrative_as_of "
        "FROM sentiment_history WHERE ticker = $1 AND timestamp < $2 "
        "ORDER BY timestamp DESC LIMIT 1",
        ticker, RANGE_START,
    )
    articles = await conn.fetch(
        "SELECT id, published_at, finbert_score, relevance_score, source, "
        "       finbert_pos, finbert_neg, finbert_neu, event_cluster_id "
        "FROM raw_articles WHERE ticker = $1 AND published_at >= $2 "
        "AND published_at <= $3 AND finbert_score IS NOT NULL",
        ticker, RANGE_START - ARTICLE_PAD, RANGE_END,
    )
    return [dict(r) for r in rows], (dict(seed) if seed else None), [dict(a) for a in articles]


# ---------------------------------------------------------------------------
# Archive
# ---------------------------------------------------------------------------

async def archive_originals() -> int:
    """COPY the in-range rows to a gzip CSV, verify the row count, write a sidecar."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        expected = await conn.fetchval(
            "SELECT COUNT(*) FROM sentiment_history WHERE timestamp >= $1 AND timestamp <= $2",
            RANGE_START, RANGE_END,
        )
        tmp = ARCHIVE.with_suffix(".tmp")
        ARCHIVE.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(tmp, "wb") as fh:
            await conn.copy_from_query(
                "SELECT * FROM sentiment_history WHERE timestamp >= $1 AND timestamp <= $2 ORDER BY id",
                RANGE_START, RANGE_END, output=fh, format="csv", header=True,
            )
    with gzip.open(tmp, "rt", newline="") as fh:
        written = sum(1 for _ in csv.reader(fh)) - 1
    if written != expected:
        raise RuntimeError(f"archive row count {written} != expected {expected}; not saved")
    tmp.rename(ARCHIVE)
    ARCHIVE_META.write_text(json.dumps({
        "rows": expected, "range_start": RANGE_START.isoformat(),
        "range_end": RANGE_END.isoformat(), "created_at": datetime.now(timezone.utc).isoformat(),
    }))
    return expected


def _archive_ok() -> bool:
    try:
        meta = json.loads(ARCHIVE_META.read_text())
    except (FileNotFoundError, ValueError):
        return False
    return (ARCHIVE.exists() and meta.get("rows", 0) > 0
            and meta["range_start"] == RANGE_START.isoformat()
            and meta["range_end"] == RANGE_END.isoformat())


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

async def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--tickers", help="comma-separated subset (default: active universe)")
    p.add_argument("--dry-run", action="store_true", help="compute + report; no writes, no archive")
    p.add_argument("--archive-only", action="store_true", help="write the archive and stop")
    p.add_argument("--concurrency", type=int, default=6)
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s")
    # httpx logs full request URLs at INFO — those carry the API keys.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    await init_pool()
    try:
        if args.archive_only:
            n = await archive_originals()
            _log.info("archived %d rows → %s", n, ARCHIVE)
            return 0
        if not args.dry_run and not _archive_ok():
            _log.error("no verified archive at %s — run --archive-only first", ARCHIVE)
            return 2

        tickers = (args.tickers.upper().split(",") if args.tickers
                   else await get_universe_as_of(RANGE_START))
        pool = await get_pool()
        sem = asyncio.Semaphore(args.concurrency)
        totals = {"tickers": 0, "rows": 0, "narr_before": 0, "narr_after": 0,
                  "conf_before": 0.0, "conf_after": 0.0, "skipped_done": 0}
        t0 = time.monotonic()

        async def _one(ticker: str) -> None:
            async with sem:
                async with pool.acquire() as conn:
                    rows, seed, articles = await _load_ticker(conn, ticker)
                if not rows:
                    return
                if not args.dry_run and all(r.get("replay_run") == RUN_ID for r in rows):
                    totals["skipped_done"] += 1
                    return
                updates = await asyncio.to_thread(rebuild_rows, ticker, rows, seed, articles)

                totals["tickers"] += 1
                totals["rows"] += len(rows)
                totals["narr_before"] += sum(r["narrative_index"] is not None for r in rows)
                totals["narr_after"] += sum(u[2] is not None for u in updates)
                orig_conf = await _orig_confidence(pool, ticker) if args.dry_run else None
                if orig_conf is not None:
                    totals["conf_before"] += orig_conf * len(rows)
                totals["conf_after"] += sum(u[5] for u in updates)

                if args.dry_run:
                    for r, u in list(zip(rows, updates))[:: max(1, len(rows) // 4)]:
                        _log.info("%s %s  raw %.1f→%.1f  narr %s→%s  conf→%d  flags=%s",
                                  ticker, r["timestamp"].strftime("%m-%d %H:%M"),
                                  r["composite_score"], u[1], r["narrative_index"], u[2],
                                  u[5], u[6])
                    return
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        await conn.executemany(_UPDATE_SQL, updates)
                _log.info("%-6s %5d rows rebuilt (%d/%d tickers, %.0fs)", ticker, len(rows),
                          totals["tickers"], len(tickers), time.monotonic() - t0)

        results = await asyncio.gather(*[_one(t) for t in tickers], return_exceptions=True)
        for t, r in zip(tickers, results):
            if isinstance(r, BaseException):
                _log.error("%s failed: %r — rerun to retry", t, r)

        n = max(1, totals["rows"])
        print(json.dumps({
            **{k: totals[k] for k in ("tickers", "rows", "skipped_done")},
            "narrative_present_pct_before": round(100 * totals["narr_before"] / n, 1),
            "narrative_present_pct_after": round(100 * totals["narr_after"] / n, 1),
            "mean_confidence_before": round(totals["conf_before"] / n, 1) if args.dry_run else None,
            "mean_confidence_after": round(totals["conf_after"] / n, 1),
            "elapsed_s": round(time.monotonic() - t0, 1),
        }, indent=2))
        return 0
    finally:
        await close_pool()


async def _orig_confidence(pool, ticker: str) -> float | None:
    async with pool.acquire() as conn:
        avg = await conn.fetchval(
            "SELECT AVG(confidence_score) FROM sentiment_history "
            "WHERE ticker = $1 AND timestamp >= $2 AND timestamp <= $3",
            ticker, RANGE_START, RANGE_END,
        )
    return float(avg) if avg is not None else None


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
