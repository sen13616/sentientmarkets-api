# SentimentAPI Database Viewer (v2)

Terminal UI for inspecting the SentimentAPI pipeline state, data quality, and scored results.

## Quick start

```bash
# From the project root (requires .venv with asyncpg, redis, rich, dotenv):
python3 scripts/tools/db_viewer.py

# Install dev-only chart dependencies (plotext + matplotlib for PNG export):
pip install -r scripts/tools/requirements-dev.txt
```

## Navigation

| Key | Screen | Description |
|-----|--------|-------------|
| `1` | OVERVIEW | Table row counts, scheduler last-run timestamps |
| `2` | EXPLORE | Sub-menu with 4 data views (see below) |
| `3` | TICKER DEEP-DIVE | Historical charts + sub-index breakdown for one ticker |
| `4` | PIPELINE HEALTH | Scheduler staleness, scoring activity, confidence flags |
| `5` | DATA QUALITY | Ticker coverage gaps, signal freshness, null-rate audit |
| `6` | LIVE SCORE | Redis cached score lookup (full JSON) |

### Global key bindings

| Key | Action |
|-----|--------|
| `A` | Toggle auto-refresh (30s countdown) |
| `E` | Open export sub-menu (CSV) |
| `R` | Refresh current screen |
| `Q` | Quit |

### EXPLORE sub-menu (Screen 2)

| Key | View |
|-----|------|
| `a` | Sentiment scores (20 most recently scored tickers) |
| `b` | Signal data (per-ticker, last 20 signals) |
| `c` | Articles (20 most recent) |
| `d` | Top scores today (top 10 bullish + bearish) |

Any other key returns to the top-level nav.

### Ticker Deep-Dive (Screen 3)

After entering a ticker, choose a lookback period:

| Key | Period |
|-----|--------|
| `1` | 24 hours |
| `2` | 7 days (default on Enter) |
| `3` | 30 days |
| `4` | 90 days |

After the screen renders:

| Key | Action |
|-----|--------|
| `P` | Export PNG charts to `tools/exports/deepdive_{TICKER}_{timestamp}/` |
| `E` | Export CSV |
| any | Back to nav |

## PNG export

Chart PNGs are saved to timestamped folders under `tools/exports/`:

```
tools/exports/deepdive_AAPL_20260506_143022/
  composite.png
  sub_indices.png
  confidence.png
```

Requires `matplotlib` (install via `scripts/tools/requirements-dev.txt`).

## CSV export

Press `E` from any screen to open the export sub-menu. Options:

1. Current screen only (uses cached data from last render)
2. Full database export (all tables to a timestamped folder)
3. Sentiment history (all rows + computed columns)
4. Raw signals (last 30 days + pivot summary)
5. Articles (all rows)
6. Top/bottom 50 scores today
7. Sentiment snapshot (current scores, entire universe)

All CSVs are written to `tools/exports/`.

## Files

| File | Purpose |
|------|---------|
| `db_viewer.py` | Main TUI entry point |
| `db_exports.py` | Shared CSV export library |
| `db_charts.py` | Chart rendering (ASCII via plotext, PNG via matplotlib) |
| `db_health.py` | SQL queries for Pipeline Health + Data Quality screens |
| `requirements-dev.txt` | Dev-only deps (plotext, matplotlib) |
| `exports/` | Output directory for CSVs and PNGs |
