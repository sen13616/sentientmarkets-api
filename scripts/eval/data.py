"""Read-only DB loaders for the eval harness.

All loaders return pandas DataFrames and use the shared asyncpg pool. They are
deliberately separate from scripts/db/queries/* — eval queries are bulk,
analytical, and must never leak into the production query modules.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from scripts.db.connection import get_pool

_SENTIMENT_COLS = (
    "ticker, timestamp, composite_score, composite_score_smoothed, "
    "composite_score_exo, "
    "market_index, narrative_index, influencer_index, macro_index, "
    "confidence_score, research_features"
)


def _to_frame(rows) -> pd.DataFrame:
    return pd.DataFrame([dict(r) for r in rows])


async def load_sentiment_panel(
    start: datetime, end: datetime, granularity: str = "daily"
) -> pd.DataFrame:
    """Sentiment rows in [start, end).

    granularity="daily" collapses to the last tick per (ticker, ET calendar
    day) in SQL — the input the daily analysis wants and ~50x less transfer.
    granularity="raw" returns every scoring tick (intraday analysis).
    """
    if granularity == "daily":
        query = f"""
            SELECT DISTINCT ON (ticker, ((timestamp AT TIME ZONE 'America/New_York')::date))
                   {_SENTIMENT_COLS}
              FROM sentiment_history
             WHERE timestamp >= $1 AND timestamp < $2
             ORDER BY ticker,
                      ((timestamp AT TIME ZONE 'America/New_York')::date),
                      timestamp DESC
        """
    elif granularity == "raw":
        query = f"""
            SELECT {_SENTIMENT_COLS}
              FROM sentiment_history
             WHERE timestamp >= $1 AND timestamp < $2
             ORDER BY ticker, timestamp
        """
    else:
        raise ValueError(f"unknown granularity: {granularity!r}")
    pool = await get_pool()
    rows = await pool.fetch(query, start, end)
    return _to_frame(rows)


async def load_close_panel(start: datetime, end: datetime) -> pd.DataFrame:
    """Daily adjusted closes as a wide (trade_date x ticker) frame.

    Close rows carry the daily BAR date as their timestamp (yfinance bar index
    at midnight, labeled UTC — see scheduler._yf_batch_download), so the trade
    date is the UTC date, NOT an ET conversion (which would shift midnight
    rows to the previous day). Rows are written from auto_adjust=True
    downloads, so these are adjusted closes; created_at DESC breaks ties so
    the definitive EOD write wins over intraday partial bars.
    """
    query = """
        SELECT DISTINCT ON (ticker, ((timestamp AT TIME ZONE 'UTC')::date))
               ticker,
               (timestamp AT TIME ZONE 'UTC')::date AS trade_date,
               value AS close
          FROM raw_signals
         WHERE signal_type IN ('yf_close', 'ohlcv_close')
           AND timestamp >= $1 AND timestamp < $2
         ORDER BY ticker,
                  ((timestamp AT TIME ZONE 'UTC')::date),
                  timestamp DESC, created_at DESC
    """
    pool = await get_pool()
    rows = await pool.fetch(query, start, end)
    df = _to_frame(rows)
    if df.empty:
        return df
    wide = df.pivot(index="trade_date", columns="ticker", values="close").sort_index()
    wide.index = pd.to_datetime(wide.index)
    return wide


async def load_price_snapshots(start: datetime, end: datetime) -> pd.DataFrame:
    """Intraday tick closes from price_snapshots (long: ticker, timestamp, close).

    Only written during US market hours, so bars outside hours simply don't
    exist. No PK/indexes on this table — this is a sequential scan by design.
    """
    query = """
        SELECT ticker, timestamp, close
          FROM price_snapshots
         WHERE timestamp >= $1 AND timestamp < $2
    """
    pool = await get_pool()
    rows = await pool.fetch(query, start, end)
    return _to_frame(rows)


async def load_article_latency(start: datetime, end: datetime) -> pd.DataFrame:
    """Per-article ingestion latency: created_at - published_at, by source.

    Quantifies the "predictive budget" — how stale news already is when we
    ingest it (Phase 5a of the nowcasting plan).
    """
    query = """
        SELECT source, published_at, created_at
          FROM raw_articles
         WHERE created_at >= $1 AND created_at < $2
    """
    pool = await get_pool()
    rows = await pool.fetch(query, start, end)
    df = _to_frame(rows)
    if not df.empty:
        df["latency_minutes"] = (
            df["created_at"] - df["published_at"]
        ).dt.total_seconds() / 60.0
    return df
