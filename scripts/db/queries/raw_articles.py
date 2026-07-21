"""
db/queries/raw_articles.py

All raw_articles table operations. No raw SQL anywhere else in the codebase.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from scripts.db.connection import get_pool


async def hash_exists(ticker: str, content_hash: str) -> bool:
    """Return True if an article with this (ticker, content_hash) already exists."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        n = await conn.fetchval(
            """
            SELECT COUNT(*) FROM raw_articles
            WHERE ticker       = $1
              AND content_hash = $2
            """,
            ticker,
            content_hash,
        )
    return (n or 0) > 0


async def insert_article(
    ticker: str,
    title: str,
    summary: str | None,
    source: str,
    source_url: str | None,
    published_at: datetime,
    provider_sentiment: float | None,
    relevance_score: float | None,
    content_hash: str,
    language: str | None = None,
) -> None:
    """
    Insert one article row. Silently ignores duplicates via ON CONFLICT DO NOTHING
    (unique index on (ticker, content_hash)).
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO raw_articles
                (ticker, title, summary, source, source_url, published_at,
                 provider_sentiment, relevance_score, content_hash, language)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (ticker, content_hash) DO NOTHING
            """,
            ticker,
            title,
            summary,
            source,
            source_url,
            published_at,
            provider_sentiment,
            relevance_score,
            content_hash,
            language,
        )


async def get_unclustered_articles(
    ticker: str,
    since_hours: float = 48.0,
) -> list[dict]:
    """
    Return articles for `ticker` published in the last `since_hours` hours
    that have no event_cluster_id assigned yet.

    Returns list of dicts with keys: id, title, published_at, relevance_score, source.
    """
    since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, title, published_at, relevance_score, source
            FROM raw_articles
            WHERE ticker           = $1
              AND published_at    >= $2
              AND event_cluster_id IS NULL
            ORDER BY published_at ASC
            """,
            ticker,
            since,
        )
    return [dict(r) for r in rows]


async def get_articles_since(
    ticker: str,
    since: datetime,
) -> list[dict]:
    """
    Return FinBERT-scored articles for scoring, deduplicated by event cluster.

    Dedup logic (Sprint 6, G-C6):
      - Clustered articles (event_cluster_id IS NOT NULL): only the
        highest-relevance article per cluster is returned.
      - Unclustered articles (event_cluster_id IS NULL): each treated as
        its own unique row via COALESCE(event_cluster_id, id::text).
      - finbert_score IS NOT NULL is in the WHERE clause (before
        DISTINCT ON) so unscored articles are excluded from cluster
        selection entirely.

    Only rows where finbert_score IS NOT NULL are returned (Sprint A).

    Returns
    -------
    list of dicts with keys: published_at, finbert_score, relevance_score,
    source, finbert_pos, finbert_neg, finbert_neu.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT published_at, finbert_score, relevance_score, source,
                   finbert_pos, finbert_neg, finbert_neu
            FROM (
                SELECT DISTINCT ON (COALESCE(event_cluster_id, id::text))
                       published_at, finbert_score, relevance_score, source,
                       finbert_pos, finbert_neg, finbert_neu
                FROM raw_articles
                WHERE ticker            = $1
                  AND published_at     >= $2
                  AND finbert_score IS NOT NULL
                ORDER BY COALESCE(event_cluster_id, id::text),
                         relevance_score DESC NULLS LAST,
                         published_at DESC
            ) deduped
            ORDER BY published_at DESC
            """,
            ticker,
            since,
        )
    return [dict(r) for r in rows]


async def count_unclustered_articles(since_hours: float = 48.0) -> int:
    """Count total unclustered articles across all tickers in the last N hours."""
    since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
    pool = await get_pool()
    async with pool.acquire() as conn:
        n = await conn.fetchval(
            """
            SELECT COUNT(*) FROM raw_articles
            WHERE published_at >= $1
              AND event_cluster_id IS NULL
            """,
            since,
        )
    return n or 0


async def get_cluster_source_breakdown(since_hours: float = 48.0) -> list[dict]:
    """
    Return per-cluster source breakdown for recently clustered articles.

    Used by narrative_job telemetry to compute cross_source vs same_source
    cluster counts.

    Returns list of dicts with keys: event_cluster_id, sources (array),
    article_count.
    """
    since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT event_cluster_id,
                   array_agg(DISTINCT source) AS sources,
                   COUNT(*) AS article_count
            FROM raw_articles
            WHERE event_cluster_id IS NOT NULL
              AND published_at >= $1
            GROUP BY event_cluster_id
            HAVING COUNT(*) >= 2
            """,
            since,
        )
    return [dict(r) for r in rows]


async def set_cluster_ids(article_ids: list[int], cluster_id: str) -> None:
    """Assign `cluster_id` to each article in `article_ids`."""
    if not article_ids:
        return
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE raw_articles
            SET event_cluster_id = $1
            WHERE id = ANY($2::int[])
            """,
            cluster_id,
            article_ids,
        )


async def get_unscored_articles(
    since_hours: float = 48.0,
    language: str = "en",
    limit: int | None = None,
) -> list[dict]:
    """
    Return articles that have not yet been scored by FinBERT.

    Filters to the specified language (default: English) and returns
    articles where finbert_score IS NULL.  When `limit` is supplied, caps
    the result set to that many rows (oldest first, so a backlog is
    drained in publication order across successive job cycles).

    Returns list of dicts with keys: id, title, summary, source.
    """
    since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
    pool = await get_pool()
    async with pool.acquire() as conn:
        if limit is None:
            rows = await conn.fetch(
                """
                SELECT id, title, summary, source
                FROM raw_articles
                WHERE published_at    >= $1
                  AND finbert_score IS NULL
                  AND language         = $2
                ORDER BY published_at ASC
                """,
                since,
                language,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT id, title, summary, source
                FROM raw_articles
                WHERE published_at    >= $1
                  AND finbert_score IS NULL
                  AND language         = $2
                ORDER BY published_at ASC
                LIMIT $3
                """,
                since,
                language,
                limit,
            )
    return [dict(r) for r in rows]


async def update_finbert_scores(
    rows: list[tuple[int, float, float, float, float]],
) -> None:
    """
    Batch-update FinBERT scores for articles by ID.

    Parameters
    ----------
    rows : list of (id, finbert_score, finbert_pos, finbert_neg, finbert_neu)
    """
    if not rows:
        return
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.executemany(
            """
            UPDATE raw_articles
            SET finbert_score = $2,
                finbert_pos   = $3,
                finbert_neg   = $4,
                finbert_neu   = $5
            WHERE id = $1
            """,
            rows,
        )


async def purge_articles_before(cutoff: datetime) -> int:
    """Delete raw_articles rows with published_at < cutoff. Returns rows deleted."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        tag = await conn.execute(
            "DELETE FROM raw_articles WHERE published_at < $1",
            cutoff,
        )
    return int(tag.split()[-1]) if tag else 0


async def strip_article_text_before(cutoff: datetime) -> int:
    """Blank title/summary/source_url on rows with published_at < cutoff.

    Research-retention compaction (2026-07-22): old articles keep the fields
    the research program needs — published_at, finbert_* tone scores,
    relevance_score, source, ticker, event_cluster_id, content_hash (Stage-1
    dedup), language — while the free-text (~60% of the row) is dropped.
    `title` is NOT NULL so it is set to '' rather than NULL. Only rows not
    already stripped are touched, so the daily pass is cheap. Safe because
    dedup clustering and FinBERT only ever read articles ≤48h old.

    Returns rows updated.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        tag = await conn.execute(
            """
            UPDATE raw_articles
            SET title = '', summary = NULL, source_url = NULL
            WHERE published_at < $1
              AND (title <> '' OR summary IS NOT NULL OR source_url IS NOT NULL)
            """,
            cutoff,
        )
    return int(tag.split()[-1]) if tag else 0


async def get_article_scores_between(
    ticker: str,
    since: datetime,
    until: datetime,
) -> list[dict]:
    """
    FinBERT-scored articles in [since, until), deduplicated by event cluster
    (same rules as get_articles_since). Windowed variant used by the
    narrative-surprise feature (nowcasting plan, Phase 5b) to build a
    trailing coverage baseline that excludes the current window.

    Returns oldest-first rows: published_at, finbert_score, relevance_score.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT published_at, finbert_score, relevance_score
            FROM (
                SELECT DISTINCT ON (COALESCE(event_cluster_id, id::text))
                       published_at, finbert_score, relevance_score
                FROM raw_articles
                WHERE ticker            = $1
                  AND published_at     >= $2
                  AND published_at     <  $3
                  AND finbert_score IS NOT NULL
                ORDER BY COALESCE(event_cluster_id, id::text),
                         relevance_score DESC NULLS LAST,
                         published_at DESC
            ) deduped
            ORDER BY published_at ASC
            """,
            ticker,
            since,
            until,
        )
    return [dict(r) for r in rows]
