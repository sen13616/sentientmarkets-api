"""
pipeline/sources/narrative.py  — Layer 05

Fetches news articles for a single ticker and writes them to raw_articles.
Deduplication is performed via SHA-256 hash of the article URL (content_hash).

Sources
-------
Primary:  Alpha Vantage NEWS_SENTIMENT
          Returns title, summary, source, URL, provider_sentiment, relevance_score.

Secondary: Finnhub /company-news
          Returns title, summary, URL, published_at.
          No provider sentiment — stored with provider_sentiment=None.
          relevance_score=1.0 per paper Stage 2 (ticker-keyed endpoint).

Language detection (Sprint A): langdetect is run on title+summary at
ingestion time, storing the detected language in raw_articles.language.
FinBERT scoring (Phase 3 of narrative_job) filters on language='en'.
"""
from __future__ import annotations

import hashlib
import logging
import os
from datetime import date, datetime, timedelta, timezone

import httpx
from dotenv import load_dotenv

from langdetect import detect, LangDetectException

from scripts.db.queries.raw_articles import hash_exists, insert_article
from pipeline.rate_limits import (
    AV_SEM, AV_DELAY,
    FINNHUB_SEM, FINNHUB_DELAY,
    guarded_get,
)

_log = logging.getLogger(__name__)

load_dotenv(override=False)

_AV_KEY      = os.environ.get("ALPHA_VANTAGE_KEY", "")
_FINNHUB_KEY = os.environ.get("FINNHUB_KEY", "")

_AV_BASE      = "https://www.alphavantage.co/query"
_FINNHUB_BASE = "https://finnhub.io/api/v1"

_ARTICLE_LOOKBACK_DAYS = 3   # fetch news from the last N days


def _hash_url(url: str) -> str:
    """SHA-256 of the article URL, hex-encoded (64 chars)."""
    return hashlib.sha256(url.encode()).hexdigest()


def _detect_language(text: str) -> str | None:
    """Detect language of text using langdetect. Returns ISO 639-1 code or None."""
    if not text or len(text.strip()) < 20:
        return None
    try:
        return detect(text)
    except LangDetectException:
        return None


# ---------------------------------------------------------------------------
# Alpha Vantage NEWS_SENTIMENT
# ---------------------------------------------------------------------------

def _parse_av_time(time_str: str) -> datetime | None:
    """Parse AV timestamp format: '20240115T163000' → datetime UTC."""
    try:
        return datetime.strptime(time_str, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


class AVLimitError(RuntimeError):
    """AV answered with a ``Note``/``Information`` body instead of a feed
    (rate limit, or an invalid/unentitled key). Carries the provider message."""


async def fetch_av_news_window(
    ticker: str,
    client: httpx.AsyncClient,
    *,
    time_from: datetime | None = None,
    time_to: datetime | None = None,
    limit: int = 50,
    delay: float = AV_DELAY,
) -> list[dict] | None:
    """
    AV NEWS_SENTIMENT for ticker, optionally bounded to [time_from, time_to].
    Returns normalized article dicts; None when the request itself failed
    (network / exhausted 429 retries).

    Raises AVLimitError when AV returns a Note/Information body — the live
    wrapper logs and swallows it; the backfill retries or aborts on it.
    """
    params = {
        "function": "NEWS_SENTIMENT",
        "tickers":  ticker,
        "limit":    limit,
        "apikey":   _AV_KEY,
    }
    if time_from is not None:
        params["time_from"] = time_from.strftime("%Y%m%dT%H%M")
    if time_to is not None:
        params["time_to"] = time_to.strftime("%Y%m%dT%H%M")

    resp = await guarded_get(
        client, _AV_BASE,
        params=params,
        sem=AV_SEM, delay=delay, label=f"AV NEWS_SENTIMENT {ticker}",
    )
    if resp is None:
        return None

    try:
        body = resp.json()
    except Exception as exc:
        _log.warning("AV NEWS_SENTIMENT JSON parse error for %s: %s", ticker, exc)
        return []

    if "Note" in body or "Information" in body:
        raise AVLimitError(str(body.get("Note") or body.get("Information")))

    articles = []
    for item in body.get("feed", []):
        url = item.get("url", "")
        if not url:
            continue

        published_at = _parse_av_time(item.get("time_published", ""))
        if published_at is None:
            continue

        # Extract per-ticker relevance and sentiment from ticker_sentiment list
        provider_sentiment: float | None = None
        relevance_score:    float | None = None
        for ts in item.get("ticker_sentiment", []):
            if ts.get("ticker", "").upper() == ticker.upper():
                try:
                    provider_sentiment = float(ts["ticker_sentiment_score"])
                    relevance_score    = float(ts["relevance_score"])
                except (KeyError, ValueError) as exc:
                    _log.debug("AV ticker_sentiment field malformed for %s: %s", ticker, exc)
                break

        articles.append({
            "ticker":             ticker,
            "title":              item.get("title", "")[:500],
            "summary":            (item.get("summary") or "")[:2000] or None,
            "source":             "alpha_vantage",
            "source_url":         url,
            "published_at":       published_at,
            "provider_sentiment": provider_sentiment,
            "relevance_score":    relevance_score,
            "content_hash":       _hash_url(url),
        })

    return articles


async def _fetch_av_news(ticker: str, client: httpx.AsyncClient) -> list[dict]:
    """Live fetch: latest 50 AV articles for ticker. Never raises."""
    try:
        return await fetch_av_news_window(ticker, client) or []
    except AVLimitError as exc:
        # Previously a silent [] — an invalid key or exhausted quota looked
        # exactly like "no news" (2026-08 outage). Make it visible.
        _log.warning("AV NEWS_SENTIMENT %s: no feed (%s)", ticker, exc)
        return []


# ---------------------------------------------------------------------------
# Finnhub company-news (fallback)
# ---------------------------------------------------------------------------

async def _fetch_finnhub_news(ticker: str, client: httpx.AsyncClient) -> list[dict]:
    """
    Finnhub /company-news for last N days.
    Returns list of normalized article dicts (provider_sentiment=None).
    """
    today      = datetime.now(timezone.utc).date()
    from_date  = (datetime.now(timezone.utc) - timedelta(days=_ARTICLE_LOOKBACK_DAYS)).date()
    return await fetch_finnhub_news_window(ticker, client, from_date, today) or []


async def fetch_finnhub_news_window(
    ticker: str,
    client: httpx.AsyncClient,
    from_date: date,
    to_date: date,
    *,
    delay: float = FINNHUB_DELAY,
) -> list[dict] | None:
    """
    Finnhub /company-news for ticker over [from_date, to_date] (inclusive
    calendar dates). Returns normalized article dicts; None when the request
    failed (no response or non-200) so callers can tell failure from "no news".
    """
    resp = await guarded_get(
        client, f"{_FINNHUB_BASE}/company-news",
        params={
            "symbol": ticker,
            "from":   str(from_date),
            "to":     str(to_date),
            "token":  _FINNHUB_KEY,
        },
        sem=FINNHUB_SEM, delay=delay, label=f"Finnhub company-news {ticker}",
    )
    if resp is None or resp.status_code != 200:
        return None

    try:
        items = resp.json()
    except Exception as exc:
        _log.warning("Finnhub company-news JSON parse error for %s: %s", ticker, exc)
        return []

    if not isinstance(items, list):
        return []

    articles = []
    for item in items:
        url = item.get("url", "")
        if not url:
            continue

        unix_ts = item.get("datetime")
        if unix_ts is None:
            continue

        try:
            published_at = datetime.fromtimestamp(int(unix_ts), tz=timezone.utc)
        except (ValueError, OSError):
            continue

        articles.append({
            "ticker":             ticker,
            "title":              (item.get("headline") or "")[:500],
            "summary":            (item.get("summary") or "")[:2000] or None,
            "source":             "finnhub",
            "source_url":         url,
            "published_at":       published_at,
            "provider_sentiment": None,
            "relevance_score":    1.0,  # Paper Stage 2: ticker-keyed endpoint → w_rel=1.0
            "content_hash":       _hash_url(url),
        })

    return articles


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def _run_narrative(ticker: str, client: httpx.AsyncClient) -> None:
    """Core implementation — requires a live client."""
    # Collect articles from both sources; AV first (higher credibility weight)
    articles = await _fetch_av_news(ticker, client)

    finnhub_articles = await _fetch_finnhub_news(ticker, client)
    articles.extend(finnhub_articles)

    # Deduplicate within this batch first (same URL from both sources)
    seen_hashes: set[str] = set()
    for article in articles:
        h = article["content_hash"]
        if h in seen_hashes:
            continue
        seen_hashes.add(h)

        # Skip if already in DB
        try:
            if await hash_exists(ticker, h):
                continue
        except Exception as exc:
            _log.warning("hash_exists error for %s: %s", ticker, exc)
            continue

        # Detect language at ingestion for FinBERT English-only filter
        text_for_lang = (article["title"] or "") + " " + (article["summary"] or "")
        language = _detect_language(text_for_lang)

        try:
            await insert_article(
                ticker             = article["ticker"],
                title              = article["title"],
                summary            = article["summary"],
                source             = article["source"],
                source_url         = article["source_url"],
                published_at       = article["published_at"],
                provider_sentiment = article["provider_sentiment"],
                relevance_score    = article["relevance_score"],
                content_hash       = article["content_hash"],
                language           = language,
            )
        except Exception as exc:
            _log.warning("insert_article error for %s: %s", ticker, exc)


async def fetch_narrative_signals(
    ticker: str,
    client: httpx.AsyncClient | None = None,
) -> None:
    """
    Fetch news for `ticker` from AV (primary) and Finnhub (additional),
    deduplicate by content_hash, and write new articles to raw_articles.

    If `client` is None a new AsyncClient is created internally, making this
    function callable standalone (e.g. in tests or one-off scripts).
    Pass a shared client from the orchestrator for efficiency.
    """
    if client is None:
        async with httpx.AsyncClient(timeout=30) as _client:
            return await _run_narrative(ticker, _client)
    return await _run_narrative(ticker, client)
