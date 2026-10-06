"""
tools/backfill_finbert.py — One-off FinBERT backfill + Finnhub relevance fix.

Run this ONCE after deploying Sprint A and applying migration 007.

What it does:
    1. Sets relevance_score=1.0 for all Finnhub articles where it's NULL
       (paper Stage 2: ticker-keyed endpoint → w_rel=1.0 by construction).
    2. Detects language for articles where language IS NULL.
    3. Scores English articles where finbert_score IS NULL with ProsusAI/finbert.

Usage:
    python3 tools/backfill_finbert.py [--batch-size 64] [--dry-run]

This script is idempotent — safe to re-run if interrupted.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time

from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s"
)
_log = logging.getLogger(__name__)


async def backfill(batch_size: int = 64, dry_run: bool = False) -> None:
    import asyncpg
    from langdetect import LangDetectException, detect

    dsn = os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)

    # ── Step 1: Finnhub relevance_score backfill ──────────────────────────
    count = await conn.fetchval(
        "SELECT COUNT(*) FROM raw_articles WHERE source = 'finnhub' AND relevance_score IS NULL"
    )
    _log.info("Step 1: %d Finnhub articles with NULL relevance_score", count)
    if count > 0 and not dry_run:
        result = await conn.execute(
            "UPDATE raw_articles SET relevance_score = 1.0 WHERE source = 'finnhub' AND relevance_score IS NULL"
        )
        _log.info("Step 1 done: %s", result)

    # ── Step 2: Language detection backfill ────────────────────────────────
    null_lang_count = await conn.fetchval(
        "SELECT COUNT(*) FROM raw_articles WHERE language IS NULL"
    )
    _log.info("Step 2: %d articles with NULL language", null_lang_count)

    if null_lang_count > 0 and not dry_run:
        offset = 0
        total_detected = 0
        while True:
            rows = await conn.fetch(
                """
                SELECT id, title, summary FROM raw_articles
                WHERE language IS NULL
                ORDER BY id
                LIMIT $1 OFFSET $2
                """,
                batch_size * 10,  # larger batches for langdetect (fast)
                offset,
            )
            if not rows:
                break
            updates = []
            for r in rows:
                text = (r["title"] or "") + " " + (r["summary"] or "")
                if len(text.strip()) < 20:
                    lang = None
                else:
                    try:
                        lang = detect(text)
                    except LangDetectException:
                        lang = None
                if lang:
                    updates.append((r["id"], lang))
            if updates:
                await conn.executemany(
                    "UPDATE raw_articles SET language = $2 WHERE id = $1",
                    updates,
                )
                total_detected += len(updates)
            offset += len(rows)
            _log.info("Step 2 progress: %d/%d language-detected", total_detected, null_lang_count)
        _log.info("Step 2 done: %d articles language-detected", total_detected)

    # ── Step 3: FinBERT scoring backfill ──────────────────────────────────
    from pipeline.nlp.finbert import score_batch

    unscored_count = await conn.fetchval(
        "SELECT COUNT(*) FROM raw_articles WHERE finbert_score IS NULL AND language = 'en'"
    )
    _log.info("Step 3: %d English articles with NULL finbert_score", unscored_count)

    if unscored_count > 0 and not dry_run:
        offset = 0
        total_scored = 0
        t_start = time.monotonic()
        while True:
            rows = await conn.fetch(
                """
                SELECT id, title, summary FROM raw_articles
                WHERE finbert_score IS NULL AND language = 'en'
                ORDER BY id
                LIMIT $1
                """,
                batch_size,
            )
            if not rows:
                break
            texts = [(r["title"] or "") + " " + (r["summary"] or "") for r in rows]
            scores = score_batch(texts)
            updates = [
                (r["id"], s["finbert_score"], s["finbert_pos"], s["finbert_neg"], s["finbert_neu"])
                for r, s in zip(rows, scores)
            ]
            await conn.executemany(
                """
                UPDATE raw_articles
                SET finbert_score = $2, finbert_pos = $3, finbert_neg = $4, finbert_neu = $5
                WHERE id = $1
                """,
                updates,
            )
            total_scored += len(updates)
            elapsed = time.monotonic() - t_start
            rate = total_scored / elapsed if elapsed > 0 else 0
            _log.info(
                "Step 3 progress: %d/%d scored (%.1f articles/sec)",
                total_scored,
                unscored_count,
                rate,
            )
        _log.info(
            "Step 3 done: %d articles FinBERT-scored in %.1fs",
            total_scored,
            time.monotonic() - t_start,
        )

    await conn.close()
    _log.info("Backfill complete.")


def main():
    parser = argparse.ArgumentParser(description="Backfill FinBERT scores")
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--dry-run", action="store_true", help="Print counts only, don't modify DB")
    args = parser.parse_args()
    asyncio.run(backfill(batch_size=args.batch_size, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
