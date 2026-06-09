#!/usr/bin/env python3
"""
tools/db_viewer.py

Terminal UI database viewer for the SentimentAPI pipeline (v2).

Run with:
    python3 tools/db_viewer.py

Navigation:
    1      OVERVIEW — table counts, scheduler status
    2      EXPLORE  — sub-menu (a: scores, b: signals, c: articles, d: top/bottom)
    3      TICKER DEEP-DIVE — historical charts for a single ticker
    4      PIPELINE HEALTH — scheduler, confidence flags, divergence
    5      DATA QUALITY — coverage gaps, signal freshness, null-rate audit
    6      LIVE SCORE — Redis cached score lookup

    A      toggle auto-refresh (30 s countdown in status bar)
    E      open export sub-menu
    R      refresh current screen
    Q      quit
"""
from __future__ import annotations

import asyncio
import json
import os
import select
import sys
import traceback
from datetime import datetime, timedelta, timezone

# Allow running from the project root or from tools/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import asyncpg
import redis.asyncio as aioredis
from dotenv import load_dotenv
from rich import box
from rich.columns import Columns
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table
from rich.text import Text

# db_exports loads dotenv itself; importing it here also initialises EXPORTS_DIR
from db_exports import EXPORTS_DIR, show_export_menu  # noqa: E402
from db_charts import ascii_line_chart, export_chart_png  # noqa: E402
from db_health import (  # noqa: E402
    query_scoring_activity_24h,
    query_confidence_flag_breakdown_24h,
    query_divergence_distribution_24h,
    query_missing_layer_breakdown_24h,
    query_ticker_coverage,
    query_stale_tickers,
    query_signal_freshness,
    query_null_rate_audit_24h,
    query_article_volume_24h,
)

_ENV_FILE = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(_ENV_FILE, override=True)  # belt-and-suspenders for the viewer

console = Console()

# ---------------------------------------------------------------------------
# Auto-refresh + export state
# ---------------------------------------------------------------------------

_auto_refresh     = False
AUTO_REFRESH_SECS = 30

# Last fetched data per screen key ("1"–"6"), stored as plain dicts for CSV.
_last_data: dict[str, list[dict]] = {}
_last_ticker = ""   # most recent ticker entered by the user

# Tracks the display name of the last-viewed EXPLORE sub-screen (for export).
_explore_screen_name: str = "EXPLORE"

# ---------------------------------------------------------------------------
# DB / Redis helpers
# ---------------------------------------------------------------------------

def _dsn() -> str:
    url = os.environ["DATABASE_URL"]
    return (
        url.replace("postgresql+asyncpg://", "postgresql://")
           .replace("postgres+asyncpg://", "postgres://")
    )


async def _get_conn() -> asyncpg.Connection:
    return await asyncpg.connect(dsn=_dsn())


def _redis_client() -> aioredis.Redis:
    url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    return aioredis.from_url(url, decode_responses=True)


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _score_style(score: float | None) -> str:
    if score is None:
        return "white"
    if score > 60:
        return "bold green"
    if score < 40:
        return "bold red"
    return "white"


def _label(score: float | None) -> str:
    if score is None:
        return "\u2014"
    if score > 60:
        return "Bullish"
    if score < 40:
        return "Bearish"
    return "Neutral"


def _fmt(v: float | None, decimals: int = 1) -> str:
    return f"{v:.{decimals}f}" if v is not None else "\u2014"


def _ago(ts) -> str:
    if ts is None:
        return "\u2014"
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts)
    now = datetime.now(tz=timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    delta = int((now - ts).total_seconds())
    if delta < 60:
        return f"{delta}s ago"
    if delta < 3600:
        return f"{delta // 60}m ago"
    if delta < 86400:
        return f"{delta // 3600}h ago"
    return f"{delta // 86400}d ago"


# ---------------------------------------------------------------------------
# Screen 1 — OVERVIEW
# ---------------------------------------------------------------------------

async def screen_overview() -> None:
    """Dashboard overview: table counts, tickers scored, scheduler timestamps."""
    global _last_data
    conn  = await _get_conn()
    try:
        tables = [
            "raw_signals", "raw_articles", "sentiment_history",
            "price_snapshots", "backtest_results",
        ]
        counts: dict[str, int] = {}
        for t in tables:
            counts[t] = await conn.fetchval(f"SELECT COUNT(*) FROM {t}")

        scored    = await conn.fetchval("SELECT COUNT(DISTINCT ticker) FROM sentiment_history")
        latest_ts = await conn.fetchval("SELECT MAX(timestamp) FROM sentiment_history")
    finally:
        await conn.close()

    # Redis scheduler timestamps — graceful degradation if unreachable
    scheduler_keys = {
        "market":       "pipeline:last_run:market",
        "market_eod":   "pipeline:last_run:market_eod",
        "narrative":    "pipeline:last_run:narrative",
        "influencer":   "pipeline:last_run:influencer",
        "macro_daily":    "pipeline:last_run:macro_daily",
        "macro_intraday": "pipeline:last_run:macro_intraday",
        "short_volume": "pipeline:last_run:short_volume",
    }
    scheduler_ts: dict[str, str | None] = {}
    redis_error: str | None = None
    try:
        redis = _redis_client()
        try:
            for layer, key in scheduler_keys.items():
                scheduler_ts[layer] = await redis.get(key)
        finally:
            await redis.close()
    except Exception as exc:  # noqa: BLE001
        redis_error = str(exc)
        for layer in scheduler_keys:
            scheduler_ts[layer] = None

    # Store for export
    export_rows: list[dict] = [{"metric": t, "value": counts[t]} for t in tables]
    export_rows.append({"metric": "tickers_scored",  "value": scored})
    export_rows.append({"metric": "latest_score_ts", "value": str(latest_ts)})
    for layer, raw in scheduler_ts.items():
        export_rows.append({"metric": f"scheduler_{layer}", "value": raw or "never"})
    _last_data["1"] = export_rows

    # Render
    console.clear()
    console.rule("[bold cyan]OVERVIEW[/bold cyan]")

    t = Table(title="Table Row Counts", box=box.SIMPLE_HEAD, show_edge=False)
    t.add_column("Table", style="cyan")
    t.add_column("Rows",  justify="right", style="yellow")
    for name in tables:
        t.add_row(name, f"{counts[name]:,}")
    console.print(t)

    console.print(
        f"  [bold]Tickers with scored data:[/bold] [yellow]{scored}[/yellow]   "
        f"[bold]Most recent score:[/bold] [yellow]{_ago(latest_ts)}[/yellow] "
        f"({latest_ts.strftime('%Y-%m-%d %H:%M UTC') if latest_ts else '\u2014'})"
    )
    console.print()

    if redis_error:
        console.print(f"  [yellow]Redis unavailable ({redis_error}) \u2014 scheduler timestamps skipped.[/yellow]")
    else:
        st = Table(title="Scheduler Last Run (Redis)", box=box.SIMPLE_HEAD, show_edge=False)
        st.add_column("Layer",    style="cyan", width=15)
        st.add_column("Last Run", style="white")
        st.add_column("Age",      style="yellow")
        for layer, raw in scheduler_ts.items():
            if raw:
                ts_obj  = datetime.fromisoformat(raw)
                display = ts_obj.strftime("%Y-%m-%d %H:%M UTC")
                age     = _ago(ts_obj)
            else:
                display = "never"
                age     = "\u2014"
            st.add_row(layer, display, age)
        console.print(st)


# ---------------------------------------------------------------------------
# Screen 2 — EXPLORE (sub-menu dispatcher)
# ---------------------------------------------------------------------------

async def _explore_sentiment_scores() -> None:
    """Show the 20 most recently scored tickers."""
    global _last_data, _explore_screen_name
    _explore_screen_name = "SENTIMENT SCORES"

    conn = await _get_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT DISTINCT ON (ticker)
                ticker, composite_score, market_index, narrative_index,
                influencer_index, macro_index, confidence_score, divergence, timestamp
            FROM sentiment_history
            ORDER BY ticker, timestamp DESC
            LIMIT 20
            """,
        )
        rows = sorted(rows, key=lambda r: r["timestamp"], reverse=True)[:20]
    finally:
        await conn.close()

    _last_data["2"] = [
        {
            "ticker":           r["ticker"],
            "composite_score":  r["composite_score"],
            "label":            _label(r["composite_score"]),
            "market_index":     r["market_index"],
            "narrative_index":  r["narrative_index"],
            "influencer_index": r["influencer_index"],
            "macro_index":      r["macro_index"],
            "confidence_score": r["confidence_score"],
            "divergence":       r["divergence"],
            "timestamp":        r["timestamp"].isoformat(),
        }
        for r in rows
    ]

    console.clear()
    console.rule("[bold cyan]SENTIMENT SCORES \u2014 20 most recently scored tickers[/bold cyan]")

    t = Table(box=box.SIMPLE_HEAD, show_edge=False)
    t.add_column("Ticker",     style="bold white", width=7)
    t.add_column("Score",      justify="right",    width=7)
    t.add_column("Label",      width=9)
    t.add_column("Market",     justify="right",    width=8)
    t.add_column("Narrative",  justify="right",    width=10)
    t.add_column("Influencer", justify="right",    width=11)
    t.add_column("Macro",      justify="right",    width=7)
    t.add_column("Conf",       justify="right",    width=6)
    t.add_column("Divergence", width=18)
    t.add_column("Scored",     width=12)

    for r in rows:
        score = r["composite_score"]
        style = _score_style(score)
        t.add_row(
            r["ticker"],
            Text(f"{score:.1f}", style=style),
            Text(_label(score), style=style),
            _fmt(r["market_index"]),
            _fmt(r["narrative_index"]),
            _fmt(r["influencer_index"]),
            _fmt(r["macro_index"]),
            str(r["confidence_score"]),
            r["divergence"] or "\u2014",
            _ago(r["timestamp"]),
        )

    console.print(t)


async def _explore_signal_data() -> None:
    """Show the last 20 raw signals for a user-specified ticker."""
    global _last_data, _last_ticker, _explore_screen_name
    _explore_screen_name = "SIGNAL DATA"

    console.clear()
    console.rule("[bold cyan]SIGNAL DATA[/bold cyan]")
    ticker = Prompt.ask("  Ticker symbol").strip().upper()
    if not ticker:
        return
    _last_ticker = ticker

    conn = await _get_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT signal_type, value, source, upload_type, timestamp
            FROM raw_signals
            WHERE ticker = $1
            ORDER BY timestamp DESC
            LIMIT 20
            """,
            ticker,
        )
    finally:
        await conn.close()

    _last_data["2"] = [
        {
            "ticker":      ticker,
            "signal_type": r["signal_type"],
            "value":       r["value"],
            "source":      r["source"],
            "upload_type": r["upload_type"],
            "timestamp":   r["timestamp"].isoformat(),
        }
        for r in rows
    ]

    console.rule(f"[bold cyan]Last 20 signals \u2014 {ticker}[/bold cyan]")

    if not rows:
        console.print(f"  [yellow]No signals found for {ticker}[/yellow]")
        return

    t = Table(box=box.SIMPLE_HEAD, show_edge=False)
    t.add_column("Signal Type", style="cyan",    width=30)
    t.add_column("Value",       justify="right", width=14)
    t.add_column("Source",      width=16)
    t.add_column("Upload Type", width=16)
    t.add_column("Timestamp",   width=22)

    for r in rows:
        t.add_row(
            r["signal_type"],
            _fmt(r["value"], 4),
            r["source"],
            r["upload_type"],
            r["timestamp"].strftime("%Y-%m-%d %H:%M UTC"),
        )
    console.print(t)


async def _explore_articles() -> None:
    """Show the 20 most recent articles."""
    global _last_data, _explore_screen_name
    _explore_screen_name = "ARTICLES"

    conn = await _get_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT ticker, title, source, provider_sentiment, published_at
            FROM raw_articles
            ORDER BY published_at DESC
            LIMIT 20
            """
        )
    finally:
        await conn.close()

    _last_data["2"] = [
        {
            "ticker":             r["ticker"],
            "title":              r["title"],
            "source":             r["source"],
            "provider_sentiment": r["provider_sentiment"],
            "published_at":       r["published_at"].isoformat(),
        }
        for r in rows
    ]

    console.clear()
    console.rule("[bold cyan]ARTICLES \u2014 20 most recent[/bold cyan]")

    t = Table(box=box.SIMPLE_HEAD, show_edge=False)
    t.add_column("Ticker",    style="bold white", width=7)
    t.add_column("Title",     width=62)
    t.add_column("Source",    width=16)
    t.add_column("Sentiment", justify="right",    width=10)
    t.add_column("Published", width=22)

    for r in rows:
        title = (r["title"] or "")[:60] + ("\u2026" if len(r["title"] or "") > 60 else "")
        sent  = r["provider_sentiment"]
        if sent is not None:
            sent_style = "green" if sent > 0.1 else ("red" if sent < -0.1 else "white")
            sent_text  = Text(f"{sent:+.2f}", style=sent_style)
        else:
            sent_text = Text("\u2014")
        t.add_row(
            r["ticker"],
            title,
            r["source"],
            sent_text,
            r["published_at"].strftime("%Y-%m-%d %H:%M UTC"),
        )
    console.print(t)


async def _explore_top_scores() -> None:
    """Show the top 10 bullish and bearish tickers in the last 24h."""
    global _last_data, _explore_screen_name
    _explore_screen_name = "TOP SCORES TODAY"

    conn = await _get_conn()
    try:
        all_rows = await conn.fetch(
            """
            SELECT DISTINCT ON (ticker) ticker, composite_score, confidence_score, timestamp
            FROM sentiment_history
            WHERE timestamp > NOW() - INTERVAL '24 hours'
            ORDER BY ticker, timestamp DESC
            """,
        )
    finally:
        await conn.close()

    bullish = sorted(all_rows, key=lambda r: r["composite_score"], reverse=True)[:10]
    bearish = sorted(all_rows, key=lambda r: r["composite_score"])[:10]

    _last_data["2"] = (
        [{"category": "bullish", "ticker": r["ticker"],
          "composite_score": r["composite_score"], "confidence_score": r["confidence_score"],
          "timestamp": r["timestamp"].isoformat()} for r in bullish]
        +
        [{"category": "bearish", "ticker": r["ticker"],
          "composite_score": r["composite_score"], "confidence_score": r["confidence_score"],
          "timestamp": r["timestamp"].isoformat()} for r in bearish]
    )

    console.clear()
    console.rule("[bold cyan]TOP SCORES TODAY \u2014 last 24 hours[/bold cyan]")

    def _make_table(rows, title: str, score_style: str) -> Table:
        t = Table(title=title, box=box.SIMPLE_HEAD, show_edge=False, min_width=38)
        t.add_column("Ticker", style="bold white", width=7)
        t.add_column("Score",  justify="right",    width=7)
        t.add_column("Conf",   justify="right",    width=6)
        t.add_column("Scored", width=12)
        for r in rows:
            t.add_row(
                r["ticker"],
                Text(f"{r['composite_score']:.1f}", style=score_style),
                str(r["confidence_score"]),
                _ago(r["timestamp"]),
            )
        return t

    if not all_rows:
        console.print("  [yellow]No sentiment data in the last 24 hours.[/yellow]")
        return

    left  = _make_table(bullish, "Most Bullish", "bold green")
    right = _make_table(bearish, "Most Bearish", "bold red")
    console.print(Columns([left, right], equal=True, expand=False))


# EXPLORE sub-screen registry
_EXPLORE_SCREENS: dict[str, tuple[str, ...]] = {
    "a": ("Sentiment scores (latest 20)",    _explore_sentiment_scores),  # type: ignore[dict-item]
    "b": ("Signal data (per-ticker)",        _explore_signal_data),       # type: ignore[dict-item]
    "c": ("Articles (latest 20)",            _explore_articles),          # type: ignore[dict-item]
    "d": ("Top scores today (bull/bear)",    _explore_top_scores),        # type: ignore[dict-item]
}


async def screen_explore() -> None:
    """Display the EXPLORE sub-menu and dispatch to the chosen sub-screen."""
    console.clear()
    console.rule("[bold cyan]EXPLORE[/bold cyan]")
    console.print(Panel(
        "[bold]Choose a view:[/bold]\n\n"
        "  [cyan]a[/cyan]  Sentiment scores (latest 20)\n"
        "  [cyan]b[/cyan]  Signal data (per-ticker)\n"
        "  [cyan]c[/cyan]  Articles (latest 20)\n"
        "  [cyan]d[/cyan]  Top scores today (bullish/bearish)\n\n"
        "  [dim]Any other key \u2192 back to nav[/dim]",
        title="[bold yellow]EXPLORE[/bold yellow]",
        expand=False,
    ))

    key = _read_key().lower()
    if key in _EXPLORE_SCREENS:
        _, fn = _EXPLORE_SCREENS[key]
        await fn()


# ---------------------------------------------------------------------------
# Screen 3 — TICKER DEEP-DIVE
# ---------------------------------------------------------------------------

async def screen_ticker_deep_dive() -> None:
    """Full deep-dive for a single ticker: history, charts, divergence, export."""
    global _last_data, _last_ticker
    console.clear()
    console.rule("[bold cyan]TICKER DEEP-DIVE[/bold cyan]")

    # --- 1. Prompt for ticker ---
    ticker = Prompt.ask("  Ticker symbol").strip().upper()
    if not ticker:
        return
    _last_ticker = ticker

    # --- 2. Prompt for lookback period ---
    console.print(
        "  Lookback:  [cyan]1[/cyan]=24h  [cyan]2[/cyan]=7d  "
        "[cyan]3[/cyan]=30d  [cyan]4[/cyan]=90d  [dim](Enter=7d)[/dim]"
    )
    period_key = _read_key().lower()
    interval_map = {
        "1": ("24 hours", timedelta(hours=24)),
        "2": ("7 days",   timedelta(days=7)),
        "3": ("30 days",  timedelta(days=30)),
        "4": ("90 days",  timedelta(days=90)),
    }
    interval_label, interval_td = interval_map.get(period_key, ("7 days", timedelta(days=7)))
    cutoff = datetime.now(tz=timezone.utc) - interval_td

    # --- 3. Query sentiment_history ---
    conn = await _get_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT timestamp, composite_score, market_index, narrative_index,
                   influencer_index, macro_index, confidence_score, divergence
            FROM sentiment_history
            WHERE ticker = $1 AND timestamp > $2
            ORDER BY timestamp ASC
            """,
            ticker,
            cutoff,
        )
    finally:
        await conn.close()

    # Cache for CSV export
    _last_data["3"] = [
        {
            "ticker":           ticker,
            "timestamp":        r["timestamp"].isoformat(),
            "composite_score":  r["composite_score"],
            "market_index":     r["market_index"],
            "narrative_index":  r["narrative_index"],
            "influencer_index": r["influencer_index"],
            "macro_index":      r["macro_index"],
            "confidence_score": r["confidence_score"],
            "divergence":       r["divergence"],
        }
        for r in rows
    ]

    console.clear()
    console.rule(f"[bold cyan]TICKER DEEP-DIVE \u2014 {ticker} ({interval_label})[/bold cyan]")

    if not rows:
        console.print(f"  [yellow]No data for {ticker} in the last {interval_label}.[/yellow]")
        return

    # --- 4a. Header panel ---
    latest = rows[-1]
    score = latest["composite_score"]
    style = _score_style(score)
    conf = latest["confidence_score"]
    console.print(Panel(
        f"[{style}]{ticker} \u2014 {_label(score)} ({score:.1f})[/{style}]   "
        f"confidence: [yellow]{conf}[/yellow]   "
        f"last scored: [dim]{_ago(latest['timestamp'])}[/dim]",
        expand=False,
    ))

    # --- 4b. Latest sub-indices table ---
    idx_table = Table(
        title="Latest Sub-Indices", box=box.SIMPLE_HEAD, show_edge=False
    )
    idx_table.add_column("Layer", style="cyan", width=14)
    idx_table.add_column("Value", justify="right", width=8)
    idx_table.add_column("Label", width=10)
    for layer_name, field in [
        ("Market",     "market_index"),
        ("Narrative",  "narrative_index"),
        ("Influencer", "influencer_index"),
        ("Macro",      "macro_index"),
    ]:
        v = latest[field]
        idx_table.add_row(
            layer_name,
            Text(_fmt(v), style=_score_style(v)),
            Text(_label(v), style=_score_style(v)),
        )
    console.print(idx_table)

    # --- Build series for charts ---
    composite_series = {
        "composite": [(r["timestamp"], r["composite_score"]) for r in rows],
    }
    sub_index_series = {
        "market":     [(r["timestamp"], r["market_index"])     for r in rows],
        "narrative":  [(r["timestamp"], r["narrative_index"])  for r in rows],
        "influencer": [(r["timestamp"], r["influencer_index"]) for r in rows],
        "macro":      [(r["timestamp"], r["macro_index"])      for r in rows],
    }
    confidence_series = {
        "confidence": [(r["timestamp"], r["confidence_score"]) for r in rows],
    }

    # --- 4c. ASCII chart: Composite score ---
    ascii_line_chart(
        f"{ticker} — Composite Score",
        composite_series,
        y_range=(0, 100),
    )
    console.print()

    # --- 4d. ASCII chart: Sub-indices ---
    ascii_line_chart(
        f"{ticker} — Sub-Indices",
        sub_index_series,
        y_range=(0, 100),
    )
    console.print()

    # --- 4e. ASCII chart: Confidence ---
    ascii_line_chart(
        f"{ticker} — Confidence",
        confidence_series,
        width=90,
        height=10,
        y_range=(0, 100),
    )
    console.print()

    # --- 4f. Divergence distribution ---
    div_counts: dict[str, int] = {}
    for r in rows:
        d = r["divergence"] or "none"
        div_counts[d] = div_counts.get(d, 0) + 1

    div_table = Table(
        title="Divergence Distribution", box=box.SIMPLE_HEAD, show_edge=False
    )
    div_table.add_column("Divergence", style="cyan", width=22)
    div_table.add_column("Count", justify="right", width=8)
    div_table.add_column("Pct", justify="right", width=8)
    total = len(rows)
    for dv, cnt in sorted(div_counts.items(), key=lambda x: -x[1]):
        pct = cnt / total * 100
        div_table.add_row(dv, str(cnt), f"{pct:.0f}%")
    console.print(div_table)
    console.print()

    # --- 5. Footer / export options ---
    console.print(
        "  [cyan][P][/cyan] Export PNG charts   "
        "[cyan][E][/cyan] Export CSV   "
        "[dim]any other key \u2192 back to nav[/dim]"
    )
    action = _read_key().lower()

    if action == "p":
        # PNG export
        ts_slug = datetime.now().strftime("%Y%m%d_%H%M%S")
        folder = os.path.join(EXPORTS_DIR, f"deepdive_{ticker}_{ts_slug}")
        os.makedirs(folder, exist_ok=True)

        p1 = export_chart_png(
            f"{ticker} \u2014 Composite Score",
            composite_series,
            os.path.join(folder, "composite.png"),
            y_range=(0, 100),
        )
        p2 = export_chart_png(
            f"{ticker} \u2014 Sub-Indices",
            sub_index_series,
            os.path.join(folder, "sub_indices.png"),
            y_range=(0, 100),
        )
        p3 = export_chart_png(
            f"{ticker} \u2014 Confidence",
            confidence_series,
            os.path.join(folder, "confidence.png"),
            y_range=(0, 100),
        )

        console.print(f"\n  [bold green]PNGs exported \u2192[/bold green] [cyan]{folder}/[/cyan]")
        if p1:
            console.print(f"    composite.png")
        if p2:
            console.print(f"    sub_indices.png")
        if p3:
            console.print(f"    confidence.png")

    elif action == "e":
        # CSV export via the standard path
        await _handle_export("3")


# ---------------------------------------------------------------------------
# Screen 4 — PIPELINE HEALTH
# ---------------------------------------------------------------------------

async def screen_pipeline_health() -> None:
    """Pipeline health dashboard: scheduler, scoring activity, flags, divergence."""
    global _last_data
    console.clear()
    console.rule("[bold cyan]PIPELINE HEALTH[/bold cyan]")

    # --- 1. Scheduler last runs (Redis) ---
    scheduler_keys = {
        "market":       "pipeline:last_run:market",
        "market_eod":   "pipeline:last_run:market_eod",
        "narrative":    "pipeline:last_run:narrative",
        "influencer":   "pipeline:last_run:influencer",
        "macro_daily":    "pipeline:last_run:macro_daily",
        "macro_intraday": "pipeline:last_run:macro_intraday",
        "short_volume": "pipeline:last_run:short_volume",
    }

    # Staleness thresholds (in seconds). None = no staleness check.
    # market: 90 min during US market hours (weekdays 14:30–21:00 UTC)
    # market_eod: 25h on weekdays, ignore weekends
    # narrative: 6h always
    # influencer: 3 days always
    # macro_daily (FRED): 25h always
    # macro_intraday (VIX/ETF): hourly during market hours only — no staleness
    #   check (would false-alarm over weekends when the job doesn't run)
    # short_volume: TODO — confirm threshold with Aayudh
    _STALENESS_SECS = {
        "market":         90 * 60,
        "market_eod":     25 * 3600,
        "narrative":      6 * 3600,
        "influencer":     3 * 86400,
        "macro_daily":    25 * 3600,
        "macro_intraday": None,
        "short_volume":   None,
    }

    scheduler_ts: dict[str, str | None] = {}
    redis_error: str | None = None
    try:
        redis = _redis_client()
        try:
            for layer, key in scheduler_keys.items():
                scheduler_ts[layer] = await redis.get(key)
        finally:
            await redis.close()
    except Exception as exc:  # noqa: BLE001
        redis_error = str(exc)
        for layer in scheduler_keys:
            scheduler_ts[layer] = None

    now = datetime.now(tz=timezone.utc)
    weekday = now.weekday()  # 0=Mon, 6=Sun
    is_weekday = weekday < 5
    # US market hours: 14:30–21:00 UTC
    market_hour = now.hour
    market_minute = now.minute
    in_market_hours = is_weekday and (
        (market_hour == 14 and market_minute >= 30) or
        (15 <= market_hour < 21)
    )

    if redis_error:
        console.print(f"  [yellow]Redis unavailable ({redis_error}) \u2014 scheduler timestamps skipped.[/yellow]")
    else:
        st = Table(title="Scheduler Last Runs", box=box.SIMPLE_HEAD, show_edge=False)
        st.add_column("Layer", style="cyan", width=15)
        st.add_column("Last Run", style="white", width=22)
        st.add_column("Age", style="yellow", width=12)
        st.add_column("Status", width=12)

        for layer, raw in scheduler_ts.items():
            if raw:
                ts_obj = datetime.fromisoformat(raw)
                if ts_obj.tzinfo is None:
                    ts_obj = ts_obj.replace(tzinfo=timezone.utc)
                display = ts_obj.strftime("%Y-%m-%d %H:%M UTC")
                age = _ago(ts_obj)
                age_secs = int((now - ts_obj).total_seconds())

                # Apply staleness rules
                threshold = _STALENESS_SECS[layer]
                if threshold is None:
                    status = "[dim]\u2014[/dim]"  # no threshold defined
                elif layer == "market" and not in_market_hours:
                    status = "[dim]off-hours[/dim]"
                elif layer == "market_eod" and not is_weekday:
                    status = "[dim]weekend[/dim]"
                elif age_secs > threshold:
                    status = "[bold red]STALE[/bold red]"
                else:
                    status = "[green]OK[/green]"
            else:
                display = "never"
                age = "\u2014"
                status = "[bold red]NEVER RUN[/bold red]"

            st.add_row(layer, display, age, status)
        console.print(st)
    console.print()

    # --- 2–5. Database queries ---
    conn = await _get_conn()
    try:
        activity = await query_scoring_activity_24h(conn)
        flags = await query_confidence_flag_breakdown_24h(conn)
        divergence = await query_divergence_distribution_24h(conn)
        missing = await query_missing_layer_breakdown_24h(conn)
    finally:
        await conn.close()

    # --- 2. Scoring activity ---
    console.print("[bold]Scoring Activity (last 24h)[/bold]")
    if activity and activity.get("n_rows", 0) > 0:
        console.print(
            f"  Rows scored: [yellow]{activity['n_rows']:,}[/yellow]   "
            f"Tickers: [yellow]{activity['n_tickers']}[/yellow]   "
            f"Avg confidence: [yellow]{activity['avg_conf']}[/yellow]"
        )
        earliest = activity.get("earliest")
        latest = activity.get("latest")
        console.print(
            f"  Earliest: [dim]{earliest.strftime('%Y-%m-%d %H:%M UTC') if earliest else '\u2014'}[/dim]   "
            f"Latest: [dim]{latest.strftime('%Y-%m-%d %H:%M UTC') if latest else '\u2014'}[/dim]"
        )
    else:
        console.print("  [yellow]No scoring activity in the last 24 hours.[/yellow]")
    console.print()

    # --- 3. Confidence flag breakdown ---
    console.print("[bold]Confidence Flag Breakdown (last 24h)[/bold]")
    if flags:
        ft = Table(box=box.SIMPLE_HEAD, show_edge=False)
        ft.add_column("Flag", style="cyan", width=35)
        ft.add_column("Count", justify="right", width=8)
        for f in flags:
            ft.add_row(f["flag"], str(f["cnt"]))
        console.print(ft)
    else:
        console.print("  [yellow]No confidence flags recorded.[/yellow]")
    console.print()

    # --- 4. Divergence distribution ---
    console.print("[bold]Divergence Distribution (last 24h)[/bold]")
    if divergence:
        dt = Table(box=box.SIMPLE_HEAD, show_edge=False)
        dt.add_column("Divergence", style="cyan", width=25)
        dt.add_column("Count", justify="right", width=8)
        for d in divergence:
            dt.add_row(d["divergence"], str(d["cnt"]))
        console.print(dt)
    else:
        console.print("  [yellow]No divergence data.[/yellow]")
    console.print()

    # --- 5. Missing-layer breakdown ---
    console.print("[bold]Missing Layers (last 24h)[/bold]")
    if missing and missing.get("total", 0) > 0:
        total = missing["total"]
        mt = Table(box=box.SIMPLE_HEAD, show_edge=False)
        mt.add_column("Layer", style="cyan", width=18)
        mt.add_column("Null", justify="right", width=8)
        mt.add_column("Pct", justify="right", width=8)
        for layer in ["market", "narrative", "influencer", "macro"]:
            null_count = missing[f"{layer}_null"]
            pct = null_count / total * 100 if total > 0 else 0
            style = "bold red" if pct > 20 else ""
            mt.add_row(
                layer,
                Text(str(null_count), style=style),
                Text(f"{pct:.0f}%", style=style),
            )
        console.print(mt)
    else:
        console.print("  [yellow]No data.[/yellow]")

    # Cache for export
    export_rows: list[dict] = []
    for layer, raw in scheduler_ts.items():
        export_rows.append({"section": "scheduler", "key": layer, "value": raw or "never"})
    if activity:
        for k, v in activity.items():
            export_rows.append({"section": "scoring_activity", "key": k, "value": str(v)})
    for f in flags:
        export_rows.append({"section": "confidence_flags", "key": f["flag"], "value": str(f["cnt"])})
    for d in divergence:
        export_rows.append({"section": "divergence", "key": d["divergence"], "value": str(d["cnt"])})
    if missing:
        for k, v in missing.items():
            export_rows.append({"section": "missing_layers", "key": k, "value": str(v)})
    _last_data["4"] = export_rows


# ---------------------------------------------------------------------------
# Screen 5 — DATA QUALITY
# ---------------------------------------------------------------------------

async def screen_data_quality() -> None:
    """Data quality dashboard: coverage, gaps, freshness, null rates, articles."""
    global _last_data
    console.clear()
    console.rule("[bold cyan]DATA QUALITY[/bold cyan]")

    conn = await _get_conn()
    try:
        coverage = await query_ticker_coverage(conn)
        stale_tickers = await query_stale_tickers(conn)
        signal_fresh = await query_signal_freshness(conn)
        null_audit = await query_null_rate_audit_24h(conn)
        article_vol = await query_article_volume_24h(conn)
    finally:
        await conn.close()

    # --- 1. Ticker coverage ---
    console.print("[bold]Ticker Coverage[/bold]")
    if coverage:
        universe = coverage["universe_size"]
        scored_24h = coverage["scored_24h"]
        scored_7d = coverage["scored_7d"]
        pct = (scored_24h / universe * 100) if universe > 0 else 0
        pct_style = "green" if pct > 80 else ("yellow" if pct > 50 else "bold red")
        console.print(
            f"  Universe: [yellow]{universe}[/yellow]   "
            f"Scored 24h: [yellow]{scored_24h}[/yellow]   "
            f"Scored 7d: [yellow]{scored_7d}[/yellow]   "
            f"Coverage (24h): [{pct_style}]{pct:.0f}%[/{pct_style}]"
        )
    else:
        console.print("  [yellow]No coverage data.[/yellow]")
    console.print()

    # --- 2. Stale / missing tickers ---
    console.print("[bold]Tickers With No Recent Score (>24h or never)[/bold]")
    if stale_tickers:
        gt = Table(box=box.SIMPLE_HEAD, show_edge=False)
        gt.add_column("Ticker", style="bold white", width=8)
        gt.add_column("Company", width=30)
        gt.add_column("Last Scored", width=22)
        gt.add_column("Gap", style="yellow", width=10)
        for t in stale_tickers:
            last = t["last_scored"]
            if last:
                last_display = last.strftime("%Y-%m-%d %H:%M UTC")
                gap = _ago(last)
            else:
                last_display = "never"
                gap = "\u2014"
            gt.add_row(
                t["ticker"],
                t.get("company_name") or "\u2014",
                last_display,
                gap,
            )
        console.print(gt)
        if len(stale_tickers) == 50:
            console.print("  [dim](capped at 50 rows)[/dim]")
    else:
        console.print("  [green]All tier1 tickers scored within the last 24h.[/green]")
    console.print()

    # --- 3. Signal freshness per source ---
    console.print("[bold]Signal Freshness Per Source (last 24h)[/bold]")
    if signal_fresh:
        sf = Table(box=box.SIMPLE_HEAD, show_edge=False)
        sf.add_column("Source", style="cyan", width=18)
        sf.add_column("Latest", width=22)
        sf.add_column("Age", style="yellow", width=10)
        sf.add_column("Signals (24h)", justify="right", width=14)
        for s in signal_fresh:
            latest = s["latest"]
            sf.add_row(
                s["source"],
                latest.strftime("%Y-%m-%d %H:%M UTC") if latest else "\u2014",
                _ago(latest) if latest else "\u2014",
                f"{s['n_signals_24h']:,}",
            )
        console.print(sf)
    else:
        console.print("  [yellow]No signals in the last 24 hours.[/yellow]")
    console.print()

    # --- 4. Null-rate audit ---
    console.print("[bold]Null-Rate Audit (last 24h sentiment_history)[/bold]")
    if null_audit:
        nt = Table(box=box.SIMPLE_HEAD, show_edge=False)
        nt.add_column("Column", style="cyan", width=20)
        nt.add_column("Null", justify="right", width=8)
        nt.add_column("Total", justify="right", width=8)
        nt.add_column("Null %", justify="right", width=8)
        for n in null_audit:
            pct = n["null_pct"]
            style = "bold red" if pct > 20 else ("yellow" if pct > 5 else "")
            nt.add_row(
                n["column"],
                Text(str(n["null_count"]), style=style),
                str(n["total"]),
                Text(f"{pct:.1f}%", style=style),
            )
        console.print(nt)
    else:
        console.print("  [yellow]No data.[/yellow]")
    console.print()

    # --- 5. Article volume per source ---
    console.print("[bold]Article Volume Per Source (last 24h)[/bold]")
    if article_vol:
        at = Table(box=box.SIMPLE_HEAD, show_edge=False)
        at.add_column("Source", style="cyan", width=18)
        at.add_column("Articles", justify="right", width=10)
        at.add_column("Tickers", justify="right", width=10)
        for a in article_vol:
            at.add_row(a["source"], f"{a['n']:,}", str(a["n_tickers"]))
        console.print(at)
    else:
        console.print("  [yellow]No articles in the last 24 hours.[/yellow]")

    # Cache for export
    export_rows: list[dict] = []
    if coverage:
        for k, v in coverage.items():
            export_rows.append({"section": "coverage", "key": k, "value": str(v)})
    for t in stale_tickers:
        export_rows.append({
            "section": "stale_tickers",
            "key": t["ticker"],
            "value": t["last_scored"].isoformat() if t["last_scored"] else "never",
            "company_name": t.get("company_name") or "",
        })
    for s in signal_fresh:
        export_rows.append({
            "section": "signal_freshness",
            "key": s["source"],
            "value": s["latest"].isoformat() if s["latest"] else "never",
            "n_signals_24h": str(s["n_signals_24h"]),
        })
    for n in null_audit:
        export_rows.append({
            "section": "null_audit",
            "key": n["column"],
            "value": f"{n['null_pct']:.1f}%",
            "null_count": str(n["null_count"]),
            "total": str(n["total"]),
        })
    for a in article_vol:
        export_rows.append({
            "section": "article_volume",
            "key": a["source"],
            "value": str(a["n"]),
            "n_tickers": str(a["n_tickers"]),
        })
    _last_data["5"] = export_rows


# ---------------------------------------------------------------------------
# Screen 6 — LIVE SCORE (Redis)
# ---------------------------------------------------------------------------

async def screen_live_score() -> None:
    """Look up the cached scored state for a ticker from Redis."""
    global _last_data, _last_ticker
    console.clear()
    console.rule("[bold cyan]LIVE SCORE LOOKUP (Redis)[/bold cyan]")
    ticker = Prompt.ask("  Ticker symbol").strip().upper()
    if not ticker:
        return
    _last_ticker = ticker

    try:
        redis = _redis_client()
        try:
            raw = await redis.get(f"sentiment:{ticker}")
        finally:
            await redis.close()
    except Exception as exc:  # noqa: BLE001
        console.print(f"  [yellow]Redis unavailable ({exc}) \u2014 cannot fetch live score.[/yellow]")
        _last_data["6"] = []
        return

    console.rule(f"[bold cyan]Redis key: sentiment:{ticker}[/bold cyan]")

    if raw is None:
        console.print(f"  [yellow]No Redis key found for sentiment:{ticker}[/yellow]")
        _last_data["6"] = []
        return

    data = json.loads(raw)
    sub  = data.get("sub_indices") or {}

    _last_data["6"] = [{
        "ticker":           ticker,
        "composite_score":  data.get("composite_score"),
        "label":            _label(data.get("composite_score")),
        "confidence":       (data.get("confidence") or {}).get("score"),
        "divergence":       data.get("divergence"),
        "timestamp":        data.get("timestamp"),
        "market_index":     (sub.get("market")     or {}).get("value"),
        "narrative_index":  (sub.get("narrative")  or {}).get("value"),
        "influencer_index": (sub.get("influencer") or {}).get("value"),
        "macro_index":      (sub.get("macro")      or {}).get("value"),
        "explanation":      data.get("explanation"),
    }]

    score = data.get("composite_score")
    style = _score_style(score)
    conf  = (data.get("confidence") or {}).get("score", "\u2014")
    ts    = data.get("timestamp", "\u2014")

    console.print(Panel(
        f"[{style}]{_label(score)} \u2014 score {score:.1f}[/{style}]   "
        f"confidence: [yellow]{conf}[/yellow]   "
        f"scored: [dim]{_ago(ts)}[/dim] ({ts})",
        title=f"[bold]{ticker}[/bold]",
        expand=False,
    ))

    if sub:
        st = Table(title="Sub-indices", box=box.SIMPLE_HEAD, show_edge=False)
        st.add_column("Layer", style="cyan", width=14)
        st.add_column("Value", justify="right", width=8)
        for layer, info in sub.items():
            v = (info or {}).get("value")
            st.add_row(layer, _fmt(v))
        console.print(st)

    drivers = data.get("top_drivers") or []
    if drivers:
        dt = Table(title="Top Drivers", box=box.SIMPLE_HEAD, show_edge=False)
        dt.add_column("Signal",    style="cyan", width=30)
        dt.add_column("Direction", width=10)
        dt.add_column("Magnitude", justify="right", width=10)
        dt.add_column("Layer",     width=14)
        for d in drivers:
            direction = d.get("direction", "\u2014")
            dir_style = "green" if direction == "bullish" else ("red" if direction == "bearish" else "white")
            dt.add_row(
                d.get("signal", "\u2014"),
                Text(direction, style=dir_style),
                _fmt(d.get("magnitude"), 2),
                d.get("source_layer", "\u2014"),
            )
        console.print(dt)

    explanation = data.get("explanation")
    if explanation:
        console.print(Panel(explanation, title="Explanation", expand=False))

    console.print()
    console.print("[dim]Full JSON:[/dim]")
    console.print_json(json.dumps(data))


# ---------------------------------------------------------------------------
# Key input — blocking and timeout variants
# ---------------------------------------------------------------------------

def _read_key() -> str:
    """Blocking: read one character from stdin without requiring Enter."""
    import termios
    import tty
    fd  = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        ch = sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)
    return ch


async def _wait_for_key(timeout: float) -> str | None:
    """
    Wait up to `timeout` seconds for a keypress.
    Returns the character, or None if the timeout elapsed.
    Runs a single blocking thread so there is no stdin-thread leak.
    """
    loop = asyncio.get_event_loop()

    def _blocking() -> str | None:
        import termios
        import tty
        fd  = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        try:
            tty.setraw(fd)
            readable, _, _ = select.select([sys.stdin], [], [], timeout)
            return sys.stdin.read(1) if readable else None
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old)

    return await loop.run_in_executor(None, _blocking)


# ---------------------------------------------------------------------------
# Export — delegates to db_exports.show_export_menu
# ---------------------------------------------------------------------------

async def _handle_export(key: str) -> None:
    """Open the export sub-menu for the current screen key."""
    # Build an effective screens dict so the export filename reflects the
    # actual sub-screen name when inside EXPLORE.
    effective_screens = dict(SCREENS)
    if key == "2" and _explore_screen_name != "EXPLORE":
        effective_screens["2"] = (_explore_screen_name, screen_explore)
    await show_export_menu(key, _last_data, effective_screens, console, _read_key)


# ---------------------------------------------------------------------------
# Navigation bar + screen runner
# ---------------------------------------------------------------------------

SCREENS: dict[str, tuple[str, ...]] = {
    "1": ("OVERVIEW",         screen_overview),          # type: ignore[dict-item]
    "2": ("EXPLORE",          screen_explore),           # type: ignore[dict-item]
    "3": ("TICKER DEEP-DIVE", screen_ticker_deep_dive),  # type: ignore[dict-item]
    "4": ("PIPELINE HEALTH",  screen_pipeline_health),   # type: ignore[dict-item]
    "5": ("DATA QUALITY",     screen_data_quality),      # type: ignore[dict-item]
    "6": ("LIVE SCORE",       screen_live_score),        # type: ignore[dict-item]
}


def _print_nav(current: str) -> None:
    """Render the navigation bar. Highlights the active top-level screen."""
    parts = []
    for key, (name, _) in SCREENS.items():
        if key == current:
            parts.append(f"[bold reverse cyan] {key}:{name} [/bold reverse cyan]")
        else:
            parts.append(f"[dim] {key}:{name} [/dim]")
    auto_badge = (
        "[bold yellow] A:auto ON [/bold yellow]" if _auto_refresh
        else "[dim] A:auto [/dim]"
    )
    parts.append(auto_badge)
    parts.append("[dim] E:export  R:refresh  Q:quit [/dim]")
    console.print("  " + "  ".join(parts))
    console.print()


async def _run_screen(key: str) -> None:
    """Execute the screen function for the given key and redraw the nav."""
    _, fn = SCREENS[key]
    try:
        await fn()
    except Exception as exc:  # noqa: BLE001
        console.print(f"\n[bold red]Error:[/bold red] {exc}")
        # One-line location hint for debugging
        tb = traceback.extract_tb(exc.__traceback__)
        if tb:
            last = tb[-1]
            console.print(f"  [dim]{last.filename}:{last.lineno} in {last.name}[/dim]")
    _print_nav(key)


# ---------------------------------------------------------------------------
# Status-line helper (used by auto-refresh countdown)
# ---------------------------------------------------------------------------

def _write_status(text: str) -> None:
    """Overwrite the current terminal line in-place (no rich markup)."""
    sys.stdout.write(f"\r\033[K{text}")
    sys.stdout.flush()


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

async def main() -> None:
    global _auto_refresh

    if "DATABASE_URL" not in os.environ:
        console.print("[bold red]DATABASE_URL not set \u2014 check your .env file.[/bold red]")
        sys.exit(1)

    current = "1"
    await _run_screen(current)

    while True:

        if _auto_refresh:
            # ----------------------------------------------------------------
            # Auto-refresh countdown — 1-second ticks, overwrites status line
            # ----------------------------------------------------------------
            countdown = AUTO_REFRESH_SECS
            while _auto_refresh:
                _write_status(
                    f"  \u23f1  Auto-refresh in {countdown:2d}s  |  "
                    "A:stop  E:export  R:now  1-6:switch  Q:quit"
                )
                key = await _wait_for_key(1.0)

                if key is None:
                    # Tick
                    countdown -= 1
                    if countdown <= 0:
                        sys.stdout.write("\n")
                        await _run_screen(current)
                        countdown = AUTO_REFRESH_SECS
                    continue

                # A key was pressed inside the countdown
                sys.stdout.write("\n")
                key = key.lower()

                if key == "q":
                    console.print("[bold cyan]Bye.[/bold cyan]")
                    return
                elif key == "a":
                    _auto_refresh = False
                    console.print("  [yellow]Auto-refresh stopped.[/yellow]")
                elif key == "r":
                    await _run_screen(current)
                    countdown = AUTO_REFRESH_SECS
                elif key == "e":
                    await _handle_export(current)
                elif key in SCREENS:
                    current = key
                    await _run_screen(current)
                    countdown = AUTO_REFRESH_SECS
                # unknown keys: loop continues

        else:
            # ----------------------------------------------------------------
            # Normal mode — blocking single keypress
            # ----------------------------------------------------------------
            console.print("[dim]Press a key\u2026[/dim]", end="")
            key = _read_key().lower()
            console.print()

            if key == "q":
                console.print("[bold cyan]Bye.[/bold cyan]")
                break
            elif key == "r":
                await _run_screen(current)
            elif key == "a":
                _auto_refresh = True
                console.print(
                    f"  [bold yellow]Auto-refresh ON[/bold yellow] \u2014 "
                    f"refreshing every {AUTO_REFRESH_SECS}s"
                )
            elif key == "e":
                await _handle_export(current)
            elif key in SCREENS:
                current = key
                await _run_screen(current)
            # unknown keys: ignored


if __name__ == "__main__":
    asyncio.run(main())
