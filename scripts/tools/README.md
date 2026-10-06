# Operations tools

Run from the repository root with the project virtualenv active. Tools that touch the
database read `DATABASE_URL` (and `REDIS_URL` where noted) from `.env`.

| Tool | Purpose |
|---|---|
| `update_universe.py` | Keep `ticker_universe` current: retire symbols that stopped trading, flag S&P 500 membership, add missing members, re-sync GICS sectors. `--dry-run` prints the diff. Inputs are the cited snapshots in `data/`. |
| `generate_keys.py` | Mint, list and revoke API keys (`create --tier pro --label website`, `list`, `revoke --id N`). The plaintext key is printed once. |
| `backfill_fred.py` | Backfill FRED Treasury series (`BACKFILL_DAYS`, default 90). |
| `seed_company_names.py`, `seed_sectors.py` | Legacy seeders for the original 502-ticker universe (static maps in `company_names.py`, `sector_map.py`). New setups use `update_universe.py`. |
| `tier_preview.py` | Print what the free and pro API responses look like for a ticker. |
| `export.py` | Export tables to CSV for offline analysis. |
| `db_viewer.py` (+ `db_charts.py`, `db_exports.py`, `db_health.py`) | Terminal UI for pipeline state and data quality (below). |

### `oneoff/` — completed one-time migrations

Kept for provenance; each has already been run against production and should not be needed again.

| Script | What it did |
|---|---|
| `dedupe_raw_signals.py` | 2026-07-20: archived and removed 3.45M historical duplicate `raw_signals` rows |
| `compact_drivers_backfill.py` | 2026-07-20: re-encoded `top_drivers` older than 30 days to the compact array format |
| `tiered_retention_backfill.py` | 2026-07-20: first pass of the tiered `raw_signals` retention policy |
| `backfill_finbert.py` | Sprint A: FinBERT-scored the article backlog and fixed Finnhub relevance values |
| `generate_sector_map.py` | Sprint P4.1: generated `sector_map.py` for the original universe |

---

## Database viewer (`db_viewer.py`)

Terminal UI for inspecting the SentimentAPI pipeline state, data quality, and scored results.

### Quick start

```bash
# From the project root (requires .venv with asyncpg, redis, rich, dotenv):
python3 scripts/tools/db_viewer.py

# Install dev-only chart dependencies (plotext + matplotlib for PNG export):
pip install -r requirements-dev.txt
```

### Navigation

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

### PNG export

Chart PNGs are saved to timestamped folders under `tools/exports/`:

```
tools/exports/deepdive_AAPL_20260506_143022/
  composite.png
  sub_indices.png
  confidence.png
```

Requires `matplotlib` (install via `requirements-dev.txt` at the repo root).

### CSV export

Press `E` from any screen to open the export sub-menu. Options:

1. Current screen only (uses cached data from last render)
2. Full database export (all tables to a timestamped folder)
3. Sentiment history (all rows + computed columns)
4. Raw signals (last 30 days + pivot summary)
5. Articles (all rows)
6. Top/bottom 50 scores today
7. Sentiment snapshot (current scores, entire universe)

All CSVs are written to `tools/exports/`.

### Files

| File | Purpose |
|------|---------|
| `db_viewer.py` | Main TUI entry point |
| `db_exports.py` | Shared CSV export library |
| `db_charts.py` | Chart rendering (ASCII via plotext, PNG via matplotlib) |
| `db_health.py` | SQL queries for Pipeline Health + Data Quality screens |
| `exports/` | Output directory for CSVs and PNGs |
