#!/usr/bin/env python3
"""
tools/db_health.py

Database query functions for the Pipeline Health and Data Quality screens.

All SQL lives here — db_viewer.py calls these functions and renders the results.
Every function is async and accepts an asyncpg Connection.
"""

from __future__ import annotations

import asyncpg

# ---------------------------------------------------------------------------
# PIPELINE HEALTH queries
# ---------------------------------------------------------------------------


async def query_scoring_activity_24h(conn: asyncpg.Connection) -> dict:
    """Scoring activity in the last 24 hours: row count, ticker count, time range, avg confidence."""
    row = await conn.fetchrow(
        """
        SELECT COUNT(*) AS n_rows,
               COUNT(DISTINCT ticker) AS n_tickers,
               MIN(timestamp) AS earliest,
               MAX(timestamp) AS latest,
               AVG(confidence_score)::numeric(5,1) AS avg_conf
        FROM sentiment_history
        WHERE timestamp > NOW() - INTERVAL '24 hours'
        """
    )
    return dict(row) if row else {}


async def query_confidence_flag_breakdown_24h(conn: asyncpg.Connection) -> list[dict]:
    """Unnest confidence_flags JSONB array and count occurrences per flag (last 24h)."""
    rows = await conn.fetch(
        """
        SELECT flag, COUNT(*) AS cnt
        FROM (
            SELECT jsonb_array_elements_text(confidence_flags) AS flag
            FROM sentiment_history
            WHERE timestamp > NOW() - INTERVAL '24 hours'
              AND confidence_flags IS NOT NULL
        ) sub
        GROUP BY flag
        ORDER BY cnt DESC
        """
    )
    return [dict(r) for r in rows]


async def query_divergence_distribution_24h(conn: asyncpg.Connection) -> list[dict]:
    """Count rows by divergence value in the last 24 hours."""
    rows = await conn.fetch(
        """
        SELECT COALESCE(divergence, 'none') AS divergence, COUNT(*) AS cnt
        FROM sentiment_history
        WHERE timestamp > NOW() - INTERVAL '24 hours'
        GROUP BY divergence
        ORDER BY cnt DESC
        """
    )
    return [dict(r) for r in rows]


async def query_missing_layer_breakdown_24h(conn: asyncpg.Connection) -> dict:
    """Count rows where each sub-index is NULL in the last 24h."""
    row = await conn.fetchrow(
        """
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE market_index IS NULL) AS market_null,
            COUNT(*) FILTER (WHERE narrative_index IS NULL) AS narrative_null,
            COUNT(*) FILTER (WHERE influencer_index IS NULL) AS influencer_null,
            COUNT(*) FILTER (WHERE macro_index IS NULL) AS macro_null
        FROM sentiment_history
        WHERE timestamp > NOW() - INTERVAL '24 hours'
        """
    )
    return dict(row) if row else {}


# ---------------------------------------------------------------------------
# DATA QUALITY queries
# ---------------------------------------------------------------------------


async def query_ticker_coverage(conn: asyncpg.Connection) -> dict:
    """Universe size, scored in last 24h, scored in last 7d, coverage percentage."""
    row = await conn.fetchrow(
        """
        SELECT
            (SELECT COUNT(*) FROM ticker_universe WHERE tier = 'tier1_supported') AS universe_size,
            (SELECT COUNT(DISTINCT ticker) FROM sentiment_history
             WHERE timestamp > NOW() - INTERVAL '24 hours') AS scored_24h,
            (SELECT COUNT(DISTINCT ticker) FROM sentiment_history
             WHERE timestamp > NOW() - INTERVAL '7 days') AS scored_7d
        """
    )
    return dict(row) if row else {}


async def query_stale_tickers(conn: asyncpg.Connection) -> list[dict]:
    """
    Tickers in tier1_supported with no recent score (>24h or never scored).
    Joins company_name. Cap at 50 rows, sorted oldest-gap first.
    """
    rows = await conn.fetch(
        """
        SELECT tu.ticker,
               tu.company_name,
               MAX(sh.timestamp) AS last_scored
        FROM ticker_universe tu
        LEFT JOIN sentiment_history sh ON sh.ticker = tu.ticker
        WHERE tu.tier = 'tier1_supported'
        GROUP BY tu.ticker, tu.company_name
        HAVING MAX(sh.timestamp) IS NULL
            OR MAX(sh.timestamp) < NOW() - INTERVAL '24 hours'
        ORDER BY MAX(sh.timestamp) ASC NULLS FIRST
        LIMIT 50
        """
    )
    return [dict(r) for r in rows]


async def query_signal_freshness(conn: asyncpg.Connection) -> list[dict]:
    """Signal freshness per source: latest timestamp and count in last 24h."""
    rows = await conn.fetch(
        """
        SELECT source, MAX(timestamp) AS latest, COUNT(*) AS n_signals_24h
        FROM raw_signals
        WHERE timestamp > NOW() - INTERVAL '24 hours'
        GROUP BY source
        ORDER BY latest DESC
        """
    )
    return [dict(r) for r in rows]


async def query_null_rate_audit_24h(conn: asyncpg.Connection) -> list[dict]:
    """
    Null-rate audit for key columns in sentiment_history (last 24h).
    Returns one row per column with null_count, total, null_pct.
    """
    row = await conn.fetchrow(
        """
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE market_index IS NULL) AS market_index_null,
            COUNT(*) FILTER (WHERE narrative_index IS NULL) AS narrative_index_null,
            COUNT(*) FILTER (WHERE influencer_index IS NULL) AS influencer_index_null,
            COUNT(*) FILTER (WHERE macro_index IS NULL) AS macro_index_null,
            COUNT(*) FILTER (WHERE top_drivers IS NULL) AS top_drivers_null,
            COUNT(*) FILTER (WHERE confidence_flags IS NULL) AS confidence_flags_null
        FROM sentiment_history
        WHERE timestamp > NOW() - INTERVAL '24 hours'
        """
    )
    if not row:
        return []
    total = row["total"]
    columns = [
        "market_index",
        "narrative_index",
        "influencer_index",
        "macro_index",
        "top_drivers",
        "confidence_flags",
    ]
    results = []
    for col in columns:
        null_count = row[f"{col}_null"]
        results.append(
            {
                "column": col,
                "null_count": null_count,
                "total": total,
                "null_pct": (null_count / total * 100) if total > 0 else 0,
            }
        )
    return results


async def query_article_volume_24h(conn: asyncpg.Connection) -> list[dict]:
    """Article volume per source in the last 24 hours."""
    rows = await conn.fetch(
        """
        SELECT source, COUNT(*) AS n, COUNT(DISTINCT ticker) AS n_tickers
        FROM raw_articles
        WHERE published_at > NOW() - INTERVAL '24 hours'
        GROUP BY source
        ORDER BY n DESC
        """
    )
    return [dict(r) for r in rows]
