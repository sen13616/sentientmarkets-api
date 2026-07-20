#!/usr/bin/env python3
"""
tools/db_exports.py

Shared export library for the SentimentAPI database viewer and standalone
export script.  All export functions are importable by both db_viewer.py
and export.py.

Public API
----------
    EXPORTS_DIR                 — canonical export output directory
    export_screen(name, rows)   — option 1: current viewer screen
    export_full_database(cb)    — option 2: all tables to a folder
    export_sentiment_history()  — option 3: sentiment_history + computed cols
    export_raw_signals()        — option 4: raw signals (30 d) + pivot summary
    export_articles()           — option 5: raw_articles, all rows
    export_top_bottom_scores()  — option 6: top/bottom 50 + full ranked list
    export_sentiment_snapshot() — option 7: current scores for entire universe
    show_export_menu(...)       — interactive sub-menu (for db_viewer.py)
"""
from __future__ import annotations

import csv
import json
import os
import re
import sys
from datetime import datetime, timezone

# Allow running from the project root or from tools/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import asyncpg
from dotenv import load_dotenv

from pipeline.scoring.driver_codec import expand_drivers, is_compact

_ENV_FILE = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
load_dotenv(_ENV_FILE, override=True)

EXPORTS_DIR = os.path.join(os.path.dirname(__file__), "exports")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _dsn() -> str:
    url = os.environ["DATABASE_URL"]
    return (
        url.replace("postgresql+asyncpg://", "postgresql://")
           .replace("postgres+asyncpg://", "postgres://")
    )


async def _conn() -> asyncpg.Connection:
    return await asyncpg.connect(dsn=_dsn())


def _masked_db_url() -> str:
    """Return DATABASE_URL with the password replaced by ***."""
    url = os.environ.get("DATABASE_URL", "not set")
    return re.sub(r":[^:@/]+@", ":***@", url)


def _v(x) -> str:
    """Serialise any value to a CSV-safe string.  None → empty string."""
    if x is None:
        return ""
    if isinstance(x, (dict, list)):
        return json.dumps(x)
    if isinstance(x, datetime):
        return x.isoformat()
    return str(x)


def _write_csv(filepath: str, rows: list[dict], fieldnames: list[str] | None = None) -> int:
    """Write *rows* to *filepath* as CSV.  Returns the number of data rows."""
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    headers = fieldnames or (list(rows[0].keys()) if rows else [])
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in rows:
            writer.writerow([_v(row.get(h) if isinstance(row, dict) else row[h])
                              for h in headers])
    return len(rows)


def _normalize_drivers(raw):
    """
    Expand compact-encoded top_drivers (rows past DRIVER_COMPACT_DAYS) back
    into the verbose dict shape so exports have a uniform schema; compacted
    rows get description=None. Verbose/empty/null values pass through.
    """
    if not raw:
        return raw
    try:
        drivers = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError):
        return raw
    if is_compact(drivers):
        return json.dumps(expand_drivers(drivers))
    return raw


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _score_to_label(score: float | None) -> str:
    if score is None:
        return ""
    s = int(score)
    if s <= 20: return "Strongly Bearish"
    if s <= 40: return "Bearish"
    if s <= 60: return "Neutral"
    if s <= 80: return "Bullish"
    return "Strongly Bullish"


# ---------------------------------------------------------------------------
# Option 1 — current viewer screen
# ---------------------------------------------------------------------------

async def export_screen(screen_name: str, rows: list[dict]) -> tuple[str, int]:
    """Export the viewer's cached rows for one screen. Returns (filepath, n)."""
    slug     = screen_name.lower().replace(" ", "_")
    filepath = os.path.join(EXPORTS_DIR, f"{slug}_{_ts()}.csv")
    n        = _write_csv(filepath, rows)
    return filepath, n


# ---------------------------------------------------------------------------
# Option 2 — full database export
# ---------------------------------------------------------------------------

_FULL_TABLES: dict[str, list[str]] = {
    "sentiment_history": [
        "id", "ticker", "composite_score", "market_index", "narrative_index",
        "influencer_index", "macro_index", "confidence_score",
        "confidence_flags", "top_drivers", "divergence",
        "market_as_of", "narrative_as_of", "influencer_as_of", "macro_as_of",
        "timestamp", "created_at",
    ],
    "raw_signals": [
        "id", "ticker", "signal_type", "value", "source",
        "upload_type", "timestamp", "created_at",
    ],
    "raw_articles": [
        "id", "ticker", "title", "summary", "source", "source_url",
        "published_at", "provider_sentiment", "relevance_score",
        "content_hash", "event_cluster_id", "finbert_score", "created_at",
    ],
    "price_snapshots": [
        "id", "ticker", "close", "volume", "timestamp", "created_at",
    ],
    "backtest_results": [
        "id", "ticker", "score_timestamp", "composite_score",
        "forward_return_1d", "forward_return_5d", "forward_return_20d", "created_at",
    ],
    "ticker_universe": [
        "id", "ticker", "tier", "added_at", "last_requested_at",
    ],
}
# api_keys exported without key_hash for security
_API_KEYS_COLS = ["id", "tier", "owner_email", "is_active", "created_at", "last_used_at"]


async def export_full_database(
    progress_cb=None,
) -> tuple[str, int]:
    """
    Export every table to CSV files inside a timestamped sub-folder.

    progress_cb(table_name: str, row_count: int) is called after each table.
    Returns (folder_path, total_rows).
    """
    folder = os.path.join(EXPORTS_DIR, f"full_export_{_ts()}")
    os.makedirs(folder, exist_ok=True)

    db = await _conn()
    total_rows   = 0
    table_counts: dict[str, int] = {}

    try:
        for table, cols in _FULL_TABLES.items():
            col_sql = ", ".join(cols)
            rows = await db.fetch(f"SELECT {col_sql} FROM {table}")
            fp   = os.path.join(folder, f"{table}.csv")
            dicts = [dict(r) for r in rows]
            if table == "sentiment_history":
                for d in dicts:
                    d["top_drivers"] = _normalize_drivers(d.get("top_drivers"))
            n    = _write_csv(fp, dicts, fieldnames=cols)
            table_counts[table] = n
            total_rows += n
            if progress_cb:
                progress_cb(table, n)

        # api_keys — omit key_hash
        col_sql = ", ".join(_API_KEYS_COLS)
        rows = await db.fetch(f"SELECT {col_sql} FROM api_keys")
        fp   = os.path.join(folder, "api_keys.csv")
        n    = _write_csv(fp, [dict(r) for r in rows], fieldnames=_API_KEYS_COLS)
        table_counts["api_keys"] = n
        total_rows += n
        if progress_cb:
            progress_cb("api_keys", n)

    finally:
        await db.close()

    # ---- summary.txt ----
    csv_size = sum(
        os.path.getsize(os.path.join(folder, f))
        for f in os.listdir(folder)
        if f.endswith(".csv")
    )
    lines = [
        "SentimentAPI Full Database Export",
        "==================================",
        f"Exported : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"Database : {_masked_db_url()}",
        "",
        "Table Counts",
        "------------",
        *[f"  {t:<28} {c:>10,} rows" for t, c in table_counts.items()],
        "",
        f"  {'TOTAL':<28} {total_rows:>10,} rows",
        "",
        f"Total CSV size: {csv_size / 1_048_576:.2f} MB",
    ]
    with open(os.path.join(folder, "summary.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    return folder, total_rows


# ---------------------------------------------------------------------------
# Option 3 — sentiment history with computed columns
# ---------------------------------------------------------------------------

async def export_sentiment_history() -> tuple[str, int]:
    """Export all sentiment_history rows with derived analysis columns."""
    db = await _conn()
    try:
        rows = await db.fetch(
            """
            SELECT id, ticker, composite_score, market_index, narrative_index,
                   influencer_index, macro_index, confidence_score,
                   confidence_flags, top_drivers, divergence,
                   market_as_of, narrative_as_of, influencer_as_of, macro_as_of,
                   timestamp, created_at
            FROM sentiment_history
            ORDER BY timestamp DESC
            """
        )
    finally:
        await db.close()

    now    = datetime.now(timezone.utc)
    output = []
    for r in rows:
        ts = r["timestamp"]
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        score  = r["composite_score"]
        layers = [r["market_index"], r["narrative_index"],
                  r["influencer_index"], r["macro_index"]]
        output.append({
            "id":               r["id"],
            "ticker":           r["ticker"],
            "composite_score":  score,
            "label":            _score_to_label(score),
            "score_band":       _score_to_label(score),
            "market_index":     r["market_index"],
            "narrative_index":  r["narrative_index"],
            "influencer_index": r["influencer_index"],
            "macro_index":      r["macro_index"],
            "confidence_score": r["confidence_score"],
            "confidence_flags": r["confidence_flags"],
            "top_drivers":      _normalize_drivers(r["top_drivers"]),
            "divergence":       r["divergence"],
            "market_as_of":     r["market_as_of"],
            "narrative_as_of":  r["narrative_as_of"],
            "influencer_as_of": r["influencer_as_of"],
            "macro_as_of":      r["macro_as_of"],
            "timestamp":        r["timestamp"],
            "created_at":       r["created_at"],
            "days_ago":         int((now - ts).total_seconds() / 86400),
            "has_market":       r["market_index"]     is not None,
            "has_narrative":    r["narrative_index"]  is not None,
            "has_influencer":   r["influencer_index"] is not None,
            "has_macro":        r["macro_index"]      is not None,
            "layers_present":   sum(1 for x in layers if x is not None),
        })

    filepath = os.path.join(EXPORTS_DIR, f"sentiment_history_{_ts()}.csv")
    n = _write_csv(filepath, output)
    return filepath, n


# ---------------------------------------------------------------------------
# Option 4 — raw signals (last 30 days) + pivot summary
# ---------------------------------------------------------------------------

async def export_raw_signals() -> tuple[tuple[str, int], tuple[str, int]]:
    """
    Export two files:
      raw_signals_{ts}.csv         — all signals from the last 30 days
      raw_signals_summary_{ts}.csv — pivot: one row per (ticker, signal_type)

    Returns ((raw_path, n_raw), (summary_path, n_summary)).
    """
    db = await _conn()
    try:
        raw_rows = await db.fetch(
            """
            SELECT id, ticker, signal_type, value, source, upload_type,
                   timestamp, created_at
            FROM raw_signals
            WHERE timestamp > NOW() - INTERVAL '30 days'
            ORDER BY timestamp DESC
            """
        )
        pivot_rows = await db.fetch(
            """
            WITH latest AS (
                SELECT DISTINCT ON (ticker, signal_type)
                    ticker, signal_type,
                    value AS latest_value,
                    timestamp AS latest_timestamp,
                    source
                FROM raw_signals
                WHERE timestamp > NOW() - INTERVAL '30 days'
                ORDER BY ticker, signal_type, timestamp DESC
            ),
            agg AS (
                SELECT
                    ticker, signal_type,
                    COUNT(*)      AS count,
                    MIN(value)    AS min_value,
                    MAX(value)    AS max_value,
                    AVG(value)    AS avg_value
                FROM raw_signals
                WHERE timestamp > NOW() - INTERVAL '30 days'
                GROUP BY ticker, signal_type
            )
            SELECT
                agg.ticker, agg.signal_type,
                agg.count, agg.min_value, agg.max_value, agg.avg_value,
                latest.latest_value, latest.latest_timestamp,
                latest.source
            FROM agg
            JOIN latest
              ON agg.ticker = latest.ticker
             AND agg.signal_type = latest.signal_type
            ORDER BY agg.ticker, agg.signal_type
            """
        )
    finally:
        await db.close()

    ts = _ts()
    raw_fields = ["id", "ticker", "signal_type", "value", "source",
                  "upload_type", "timestamp", "created_at"]
    sum_fields = ["ticker", "signal_type", "count", "min_value", "max_value",
                  "avg_value", "latest_value", "latest_timestamp", "source"]

    raw_path = os.path.join(EXPORTS_DIR, f"raw_signals_{ts}.csv")
    sum_path = os.path.join(EXPORTS_DIR, f"raw_signals_summary_{ts}.csv")

    n_raw = _write_csv(raw_path, [dict(r) for r in raw_rows], fieldnames=raw_fields)
    n_sum = _write_csv(sum_path, [dict(r) for r in pivot_rows], fieldnames=sum_fields)

    return (raw_path, n_raw), (sum_path, n_sum)


# ---------------------------------------------------------------------------
# Option 5 — articles
# ---------------------------------------------------------------------------

async def export_articles() -> tuple[str, int]:
    """Export all raw_articles rows."""
    db = await _conn()
    try:
        rows = await db.fetch(
            """
            SELECT id, ticker, title, summary, source, source_url,
                   published_at, provider_sentiment, relevance_score,
                   content_hash, event_cluster_id, finbert_score, created_at
            FROM raw_articles
            ORDER BY published_at DESC
            """
        )
    finally:
        await db.close()

    filepath = os.path.join(EXPORTS_DIR, f"articles_{_ts()}.csv")
    n = _write_csv(filepath, [dict(r) for r in rows])
    return filepath, n


# ---------------------------------------------------------------------------
# Option 6 — top / bottom 50 scores today
# ---------------------------------------------------------------------------

_SCORE_COLS = [
    "id", "ticker", "composite_score", "market_index", "narrative_index",
    "influencer_index", "macro_index", "confidence_score", "divergence", "timestamp",
]


async def export_top_bottom_scores() -> tuple[tuple, tuple, tuple]:
    """
    Export three files:
      top_scores_{ts}.csv     — top 50 composite scores in last 24 h
      bottom_scores_{ts}.csv  — bottom 50
      scores_ranked_{ts}.csv  — all tickers ranked (1 = most bullish)

    Returns ((top_path, n), (bot_path, n), (ranked_path, n)).
    """
    db = await _conn()
    try:
        col_sql = ", ".join(_SCORE_COLS)
        rows = await db.fetch(
            f"""
            SELECT DISTINCT ON (ticker) {col_sql}
            FROM sentiment_history
            WHERE timestamp > NOW() - INTERVAL '24 hours'
            ORDER BY ticker, timestamp DESC
            """
        )
    finally:
        await db.close()

    all_dicts = [dict(r) for r in rows]
    ranked_desc = sorted(all_dicts, key=lambda r: r["composite_score"], reverse=True)

    top50  = ranked_desc[:50]
    bot50  = list(reversed(ranked_desc[-50:]))  # most bearish first
    ranked = [{"rank": i + 1, **r} for i, r in enumerate(ranked_desc)]

    ts = _ts()
    top_path    = os.path.join(EXPORTS_DIR, f"top_scores_{ts}.csv")
    bot_path    = os.path.join(EXPORTS_DIR, f"bottom_scores_{ts}.csv")
    ranked_path = os.path.join(EXPORTS_DIR, f"scores_ranked_{ts}.csv")

    n_top    = _write_csv(top_path,    top50,  fieldnames=_SCORE_COLS)
    n_bot    = _write_csv(bot_path,    bot50,  fieldnames=_SCORE_COLS)
    n_ranked = _write_csv(ranked_path, ranked, fieldnames=["rank"] + _SCORE_COLS)

    return (top_path, n_top), (bot_path, n_bot), (ranked_path, n_ranked)


# ---------------------------------------------------------------------------
# Option 7 — sentiment snapshot (one row per ticker, latest score)
# ---------------------------------------------------------------------------

async def export_sentiment_snapshot() -> tuple[str, int]:
    """
    Export a point-in-time snapshot: one row per ticker with the most recent
    sentiment score, sub-indices, confidence, and company name.

    Returns (filepath, n_rows).
    """
    db = await _conn()
    try:
        rows = await db.fetch(
            """
            SELECT DISTINCT ON (sh.ticker)
                sh.ticker,
                tu.company_name,
                sh.composite_score,
                sh.market_index,
                sh.narrative_index,
                sh.influencer_index,
                sh.macro_index,
                sh.confidence_score,
                sh.confidence_flags,
                sh.top_drivers,
                sh.divergence,
                sh.market_as_of,
                sh.narrative_as_of,
                sh.influencer_as_of,
                sh.macro_as_of,
                sh.timestamp
            FROM sentiment_history sh
            LEFT JOIN ticker_universe tu ON tu.ticker = sh.ticker
            ORDER BY sh.ticker, sh.timestamp DESC
            """
        )
    finally:
        await db.close()

    now    = datetime.now(timezone.utc)
    output = []
    for r in rows:
        score  = r["composite_score"]
        layers = [r["market_index"], r["narrative_index"],
                  r["influencer_index"], r["macro_index"]]
        ts = r["timestamp"]
        if ts is not None and ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        output.append({
            "ticker":           r["ticker"],
            "company_name":     r["company_name"],
            "composite_score":  score,
            "label":            _score_to_label(score),
            "market_index":     r["market_index"],
            "narrative_index":  r["narrative_index"],
            "influencer_index": r["influencer_index"],
            "macro_index":      r["macro_index"],
            "layers_present":   sum(1 for x in layers if x is not None),
            "confidence_score": r["confidence_score"],
            "confidence_flags": r["confidence_flags"],
            "divergence":       r["divergence"],
            "top_drivers":      r["top_drivers"],
            "market_as_of":     r["market_as_of"],
            "narrative_as_of":  r["narrative_as_of"],
            "influencer_as_of": r["influencer_as_of"],
            "macro_as_of":      r["macro_as_of"],
            "timestamp":        r["timestamp"],
            "minutes_ago":      int((now - ts).total_seconds() / 60) if ts else None,
        })

    # Sort by composite score descending (most bullish first)
    output.sort(key=lambda r: r["composite_score"] or 0, reverse=True)

    filepath = os.path.join(EXPORTS_DIR, f"sentiment_snapshot_{_ts()}.csv")
    n = _write_csv(filepath, output)
    return filepath, n


# ---------------------------------------------------------------------------
# Interactive export sub-menu (called from db_viewer.py)
# ---------------------------------------------------------------------------

async def show_export_menu(
    current_key: str,
    last_data: dict,
    screens: dict,
    console,
    read_key_fn,
) -> None:
    """
    Print the export sub-menu and run the selected option.

    Parameters
    ----------
    current_key  : active screen key ("1"–"6")
    last_data    : viewer's _last_data dict (screen key → list of row dicts)
    screens      : viewer's SCREENS dict (screen key → (name, fn))
    console      : rich Console instance
    read_key_fn  : sync callable that returns one character (the viewer's _read_key)
    """
    from rich.panel import Panel

    console.print()
    console.print(Panel(
        "[bold]EXPORT OPTIONS[/bold]\n\n"
        "  [cyan]1[/cyan]  Current screen only\n"
        "  [cyan]2[/cyan]  Full database export (all tables → timestamped folder)\n"
        "  [cyan]3[/cyan]  Sentiment history (all rows + computed columns)\n"
        "  [cyan]4[/cyan]  Raw signals — last 30 days + pivot summary\n"
        "  [cyan]5[/cyan]  Articles — all rows\n"
        "  [cyan]6[/cyan]  Top / bottom 50 scores today\n"
        "  [cyan]7[/cyan]  Sentiment snapshot (current scores, entire universe)\n"
        "  [cyan]8[/cyan]  Cancel",
        title="[bold yellow]Export[/bold yellow]",
        expand=False,
    ))

    key = read_key_fn()
    console.print()

    if key == "1":
        rows = last_data.get(current_key)
        if not rows:
            console.print("  [yellow]No data cached — refresh the screen first.[/yellow]")
            return
        screen_name = screens[current_key][0]
        path, n = await export_screen(screen_name, rows)
        console.print(f"  [bold green]Exported {n} rows →[/bold green] [cyan]{path}[/cyan]")

    elif key == "2":
        console.print("  [bold yellow]Full database export — fetching all tables…[/bold yellow]\n")

        def _cb(table: str, n: int) -> None:
            console.print(f"    [dim]{table:<28}[/dim] [yellow]{n:>10,}[/yellow] rows")

        folder, total = await export_full_database(_cb)
        console.print(
            f"\n  [bold green]Complete — {total:,} total rows →[/bold green] "
            f"[cyan]{folder}[/cyan]"
        )

    elif key == "3":
        console.print("  [dim]Exporting sentiment history…[/dim]")
        path, n = await export_sentiment_history()
        console.print(f"  [bold green]Exported {n:,} rows →[/bold green] [cyan]{path}[/cyan]")

    elif key == "4":
        console.print("  [dim]Exporting raw signals (last 30 days)…[/dim]")
        (raw_path, n_raw), (sum_path, n_sum) = await export_raw_signals()
        console.print(f"  [bold green]Raw data: {n_raw:,} rows →[/bold green] [cyan]{raw_path}[/cyan]")
        console.print(f"  [bold green]Summary:  {n_sum:,} rows →[/bold green] [cyan]{sum_path}[/cyan]")

    elif key == "5":
        console.print("  [dim]Exporting articles…[/dim]")
        path, n = await export_articles()
        console.print(f"  [bold green]Exported {n:,} rows →[/bold green] [cyan]{path}[/cyan]")

    elif key == "6":
        console.print("  [dim]Exporting top / bottom scores…[/dim]")
        (tp, nt), (bp, nb), (rp, nr) = await export_top_bottom_scores()
        console.print(f"  [bold green]Top 50:  {nt} rows →[/bold green] [cyan]{tp}[/cyan]")
        console.print(f"  [bold green]Bot 50:  {nb} rows →[/bold green] [cyan]{bp}[/cyan]")
        console.print(f"  [bold green]Ranked:  {nr} rows →[/bold green] [cyan]{rp}[/cyan]")

    elif key == "7":
        console.print("  [dim]Exporting sentiment snapshot…[/dim]")
        path, n = await export_sentiment_snapshot()
        console.print(f"  [bold green]Exported {n:,} tickers →[/bold green] [cyan]{path}[/cyan]")

    # key == "8" or anything else → silent cancel
