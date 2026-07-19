# SentientMarkets Sentiment API

A four-channel sentiment scoring engine for US equities. A continuously running pipeline ingests market, narrative, influencer, and macroeconomic signals; normalizes them per the methodology below; and serves a single 0–100 composite score per ticker via a FastAPI read-only endpoint. The universe is the S&P 500 (502 tickers as of 2026-04-24).

This repository accompanies the research paper **"How to Quantify Stock Sentiment"** (target: SSRN). The paper describes the methodology — channel decomposition, per-event weight formula, FinBERT-based narrative scoring, z-score normalization, sub-index aggregation, and composite construction. This codebase is the reference implementation. The intent is that an academic reviewer can read the paper, open this repo, and verify that every formula, weight, and threshold the paper claims is actually present in the code.

**Deployed phase:** 4 (as of 2026-05-16). See [Phases & audits](#phases--audits) below for what changed in each phase.

---

## How to Use

SentientMarkets API is currently invite-only. To request access, reach out directly.

Once provisioned, you'll receive a Bearer token. Include it in the `Authorization` header on every request.

### Authentication

```bash
curl -H "Authorization: Bearer sk-sm-prod-xxxxxxxxxxxx" \
  https://sentimentapi-p.up.railway.app/v1/sentiment/AAPL
```

### Get sentiment for a ticker

```bash
GET /v1/sentiment/{ticker}
```

**Response (Free tier):**
```json
{
  "ticker": "AAPL",
  "score": 72,
  "label": "Bullish",
  "confidence": 81,
  "timestamp": "2026-05-16T14:30:00Z",
  "cache_age_seconds": 480,
  "market_hours": { "is_open": true, "next_open": "...", "last_close": "..." }
}
```

**Pro tier** (requires `?detail=full`) adds `score_raw`, `ema_obs_count`, `sub_indices`, `missing_layers`, `divergence`, `top_drivers`, `explanation`, `freshness`, and `confidence_flags`.

### Get sentiment history (Pro)

```bash
GET /v1/sentiment/{ticker}/history?days=30&interval=daily
```

**Response:**
```json
{
  "ticker": "AAPL",
  "history": [
    { "date": "2026-04-16", "score": 68.5 },
    { "date": "2026-04-17", "score": 70.1 }
  ]
}
```

### List all covered tickers

```bash
GET /v1/tickers
```

Returns all 502 S&P 500 tickers with company names and sectors.

### Full API reference

See [`docs/api/README.md`](docs/api/README.md) for complete endpoint documentation.

---

## Methodology

### Four channels, weighted composite

```
Composite = 0.35 · Market
          + 0.30 · Narrative
          + 0.25 · Influencer
          + 0.10 · Macro
```

If any layer is missing, its weight is redistributed proportionally across the remaining layers. Weights are defined at [`pipeline/scoring/composite.py:23-28`](pipeline/scoring/composite.py).

### Per-signal weight formula

Every individual signal — a news article, an insider trade, an RSI reading, a VIX print — enters its layer's sub-index with a weight:

```
w_i  =  w_src  ·  w_rel  ·  w_conf  ·  w_author  ·  e^(−λ · Δt_i)
```

- **`w_src`** — source-credibility weight. Source-keyed by default (`_SOURCE_WEIGHTS` in [`normalize.py`](pipeline/features/normalize.py)). For the influencer channel, the weight is **signal-channel-keyed** instead (insider / analyst-consensus / analyst-target / earnings-revisions), per paper §Event-Level Weighting — `_INFLUENCER_SIGNAL_WEIGHT` in `normalize.py`. The source-keyed `w_src` for the influencer layer falls through only when the signal type is not one of the four paper channels.
- **`w_rel`** — relevance weight, narrative channel only. Articles below `relevance_score < 0.60` are excluded entirely (Sprint D, paper Stage 2).
- **`w_conf`** — model confidence, narrative channel only. Computed from FinBERT class probabilities as `1 − (−Σ P_i ln P_i) / ln 3`; high entropy → low confidence. Set to `1.00` for all non-textual signals (paper: "Model confidence does not apply to non-textual signals"). Defined at [`normalize.py:_compute_w_conf`](pipeline/features/normalize.py).
- **`w_author`** — author credibility. `1.00` uniformly today; scaffold for the deferred role-based hierarchy (CEO → CFO → Director …) that the paper places in Future Additions.
- **`e^(−λ · Δt_i)`** — exponential time decay. Half-life depends on the (layer, source, signal-type) — see table below.

### Source-credibility weights (`_SOURCE_WEIGHTS`)

| Source | Weight | Used by |
|---|---:|---|
| `polygon` | 0.90 | Market OHLCV fallback |
| `yfinance` | 0.90 | Market OHLCV primary, influencer target-price + earnings revisions |
| `finra_regsho` | 0.90 | FINRA short-volume daily file |
| `computed` | 0.85 | RSI (Wilder's), derived returns |
| `alpha_vantage` | 0.75 | Narrative `NEWS_SENTIMENT` (text only, not provider sentiment scores anymore) |
| `finnhub` | 0.65 | Narrative news fallback, influencer insider + analyst-consensus |

Note: `sec_edgar` is no longer a wired source. The Form 4 primary path was removed in P3.4 (Finnhub is now the sole insider provider). Sprint C (EDGAR 8-K narrative source) was drafted but never merged; the source label was retracted in Phase 5 and 8-K filings moved to Future Additions.

### Half-lives

| Layer | Default half-life | Source-specific overrides | Signal-type overrides |
|---|---|---|---|
| Market | 1 h | — | — |
| Narrative | 12 h | — | — |
| Influencer | 72 h (analyst default) | — | `insider_net_shares` → 168 h |
| Macro | 336 h (14 d) | — | — |

The empty `_HALF_LIFE_OVERRIDE` dict at [`normalize.py`](pipeline/features/normalize.py) is retained as a stable extension point. Influencer signal-channel half-lives are in `_INFLUENCER_SIGNAL_HALF_LIFE_H`.

### Influencer signal-channel weights

Per paper §Event-Level Weighting, the influencer channel keys weights by signal type rather than provider:

| Signal channel | `w_src` |
|---|---:|
| `insider_net_shares` | 1.00 |
| `analyst_buy_pct` | 0.85 |
| `analyst_target_price` | 0.85 |
| `earnings_estimate_revision` | 0.80 |

### Normalization

Each numeric signal is scored to a direction-corrected `[0, 100]` value (50 = neutral, > 50 = bullish, < 50 = bearish). The primary path is **rolling z-score** with a per-signal-type window:

```
z = (x − μ) / max(σ, σ_floor)        clamped to [−3, +3]
score = 50 + 50 · z / 3               (50 + 50·(−z)/3 for sign-inverted signals)
```

The window is **500 observations** for intraday market signals (`rsi_14`, `return_*`, `volume_ratio`, `order_flow_imbalance`, `bid_ask_spread_bps`) and **90 observations** for daily-cadence signals (short-volume, insider net-shares, analyst, VIX, sector ETF, FRED). When history is below the `fill_threshold` (default 50% of window), a parametric fallback (`_score_rsi`, `_score_vix`, `_score_treasury_*`, etc.) is used instead — see [`normalize.py`](pipeline/features/normalize.py).

Sign-inverted signals (negated z before scaling): RSI (contrarian interpretation in the normalizer — see note in market sub-index below), VIX, bid-ask spread, short-volume ratio, all Treasury yields, TED-substitute spread.

### Sub-index aggregation

- **Generic layers (narrative, influencer):**
  ```
  raw   = Σ(w_i · score_i) / Σ(w_i)            volume-weighted average
  value = 50 + min(1, n / 5) · (raw − 50)       shrinkage toward 50 when n < 5
  ```
- **Market layer:** a structured **6-component** aggregator [`compute_market_sub_index`](pipeline/scoring/subindices.py). Each component is mapped to `[−1, +1]` then weighted:

  | Component | Weight | Signal type(s) |
  |---|---:|---|
  | Returns | 0.30 | mean of `return_1d`, `return_5d`, `return_20d` |
  | Momentum | 0.15 | `rsi_14` as momentum (RSI 70 → +1 bullish) |
  | Order flow | 0.20 | `order_flow_imbalance` (CLV) |
  | Liquidity | 0.10 | `bid_ask_spread_bps` (wider → bearish) |
  | Short volume | 0.15 | `short_volume_ratio_otc` z-score (already direction-corrected) |
  | Volume | 0.10 | `volume_ratio` |

  Missing components have their weight redistributed across present components. 5-of-6 missing → layer treated as missing entirely.

  RSI sign convention: the market sub-index treats RSI as a momentum signal (RSI 70 → bullish); the contrarian interpretation (RSI 70 → bearish) lives in the parametric normalizer and surfaces in driver descriptions. Both are intentional.

- **Macro layer:** a paper-direct weighted average [`compute_macro_sub_index`](pipeline/scoring/subindices.py) with **no shrinkage** (paper: "macro signals are market-wide state variables available at every scoring tick"). Per-signal weights (`_MACRO_SIGNAL_WEIGHTS`):

  | Macro signal | Weight |
  |---|---:|
  | `sector_etf_return_20d` | 1.50 |
  | `vix` | 1.00 |
  | `treasury_yield_10y` | 1.00 |
  | `ted_spread` (substitute = `T10Y2Y` 10y−2y slope) | 1.00 |
  | `treasury_yield_2y` | 0.75 |

  Missing signals are absorbed by re-normalizing against the sum of present weights.

  The sector ETF component is **per-ticker**: each ticker is routed to its GICS sector ETF (XLK / XLV / XLE / …) via `ticker_universe.sector` (P4.2). Treasury yields and the TED-substitute come from FRED (P4.3); the original 3-month-LIBOR-based TED spread was discontinued in 2022, and `T10Y2Y` is the agreed substitute pending paper text reconciliation.

### Composite EMA smoothing

After the composite is computed, a variable-timestep EMA with a 4-hour half-life is applied per ticker:

```
α = 1 − 0.5^(Δt / 4h)
composite_smoothed = α · composite_raw + (1 − α) · composite_smoothed_prev
```

Cold-start seeds with the raw composite. The API's `score` field returns the smoothed value; `score_raw` (Pro tier only) returns the unsmoothed composite. Defined at [`pipeline/scoring/ema.py`](pipeline/scoring/ema.py).

### Label mapping

| Score | Label |
|---|---|
| 0–20 | Strongly Bearish |
| 21–40 | Bearish |
| 41–60 | Neutral |
| 61–80 | Bullish |
| 81–100 | Strongly Bullish |

---

## Architecture

Two systems run side-by-side, separated by Redis as the read/write boundary.

```
                        ┌──────────────────────────────────────┐
   External APIs ────▶  │              SYSTEM A                │
   (AV, Finnhub,        │      Background pipeline             │
    yfinance,           │  ┌────────────────┐                  │
    Polygon, FRED,      │  │ Ingestion jobs │ (write raw_*)    │
    FINRA REGSHO)       │  │  (data only)   │                  │
                        │  └──────┬─────────┘                  │
                        │         ▼                            │
                        │  ┌────────────────┐                  │
                        │  │ scoring_tick   │ every 30 min     │
                        │  │ (Layers 03–10) │                  │
                        │  └──────┬─────────┘                  │
                        │         ▼                            │
                        │  ┌────────────┐   ┌────────────┐     │
                        │  │   Redis    │   │ PostgreSQL │     │
                        │  │ (current)  │   │ (history)  │     │
                        │  └─────┬──────┘   └────────────┘     │
                        └────────┼─────────────────────────────┘
                                 │
                        ┌────────┼─────────────────────────────┐
                        │   ▼    │           SYSTEM B          │
                        │  FastAPI read-only endpoints         │
                        │   /v1/sentiment/{ticker}             │
                        │   /v1/sentiment/{ticker}/history     │
                        │   /v1/tickers   /v1/status   /health │
                        └──────────────────────────────────────┘
```

### Pipeline jobs (`pipeline/scheduler.py`)

| Job | Schedule | Writes |
|---|---|---|
| `scoring_tick_job` | every 30 min | **the only scoring job** — recomputes all 4 layers for all 502 tickers from current DB state |
| `market_job` | weekdays 14:00–20:45 UTC, every 15 min | OHLCV (yfinance batch), per-ticker RSI / order flow / bid-ask |
| `market_eod_job` | weekdays 21:15 UTC | definitive close prices |
| `narrative_job` | every 30 min (24/7) | AV NEWS_SENTIMENT + Finnhub news → `raw_articles`; semantic dedup; FinBERT scoring |
| `influencer_job` | every 6 h | Finnhub insider + analyst consensus; yfinance target price + earnings-estimate revisions |
| `macro_job` | daily 02:00 UTC | VIX, sector ETF closes |
| `fred_job` | hourly | FRED `DGS10`, `DGS2`, `T10Y2Y` |
| `short_volume_job` | weekdays 21:30 UTC | FINRA REGSHO daily short-volume file |
| `retention_job` | daily 03:30 UTC | deletes old rows — see retention policy below |

Ingestion and scoring are decoupled: ingestion jobs are data-only; only `scoring_tick_job` runs the scoring pipeline. Concurrency is bounded by a `Semaphore(10)` to stay within the asyncpg pool limit.

**Retention policy** (`retention_job`, daily 03:30 UTC):

| Data | Kept for | Reason |
|---|---|---|
| `raw_signals` OHLCV (`yf_*`, `ohlcv_*`) | 365 days | longest scoring lookback is 50 trading days; 7× safety margin |
| `raw_signals` everything else | 90 days | RSI / order-flow / macro / short-volume normalizers use ≤20-day windows |
| `raw_articles` | 30 days | narrative scoring half-life is 4 h; clustering window is 48 h |

`sentiment_history`, `price_snapshots`, `ticker_universe`, and `api_keys` are not touched. Tune the windows via `OHLCV_RETENTION_DAYS` / `SIGNAL_RETENTION_DAYS` / `ARTICLE_RETENTION_DAYS` in `pipeline/scheduler.py`.

### Scoring pipeline (per tick, per ticker)

1. **Fetch** — pull recent signals from `raw_signals` and `raw_articles`.
2. **Filter staleness** — per-signal staleness rules (`pipeline/confidence/staleness.py`).
3. **Normalize** — z-score (or parametric fallback) → direction-corrected `[0, 100]` score; apply `w = w_src · w_rel · w_conf · w_author · e^(−λΔt)`.
4. **Aggregate** — per-layer sub-index (market 6-component, macro paper-table, narrative + influencer generic).
5. **Composite** — weighted average of 4 sub-indices, missing-layer redistribution.
6. **EMA smoothing** — 4h half-life over the composite.
7. **Confidence** — base 100 minus penalties for stale / missing / divergent signals.
8. **Drivers + explanation** — top-N ranked drivers; template-based explanation.
9. **Persist** — write `sentiment:{ticker}` to Redis (current state), append row to `sentiment_history` (immutable record).

### Database

PostgreSQL via `asyncpg`, Redis via `redis.asyncio`. The schema lives in:

- `migrations.sql` — base migrations 001–004 (`raw_signals`, `raw_articles`, `sentiment_history`, `price_snapshots`, `backtest_results`, `api_keys`, `ticker_universe`)
- `migrations/005-008` — additive: `ticker_universe.company_name`, `sentiment_history.composite_score_smoothed` + `ema_obs_count`, `raw_articles.finbert_*` + `language`, `ticker_universe.sector`

A column-by-column data dictionary will be added at `docs/DATA_DICTIONARY.md` in Phase-5 Group B.

---

## Running locally

### Unit tests — no infrastructure required

The unit-test suite has zero dependencies on a live database, Redis, or external API keys. Integration tests are marked `@pytest.mark.integration` and are auto-deselected by `conftest.py` unless explicitly requested.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest                          # ~31 unit-test files; should be green
```

This is the entry point reviewers can run on a clean clone to verify that the scoring math, normalization, EMA smoothing, sub-index aggregation, dedup, and FinBERT-batch chunking all behave as the paper specifies.

### Full pipeline — production infrastructure required

End-to-end runs require a PostgreSQL database with the schema applied, a Redis instance, and live API keys for Alpha Vantage, Finnhub, FRED, and (in production) the FinBERT model weights cached locally. There is no SQLite or in-memory replacement. The production deployment is on Railway; reviewers without Railway access cannot run the live pipeline.

```bash
# Apply schema (psql)
psql $DATABASE_URL < migrations.sql
psql $DATABASE_URL < migrations/005_add_company_name.sql
psql $DATABASE_URL < migrations/006_add_smoothed_score.sql
psql $DATABASE_URL < migrations/007_finbert_columns.sql
psql $DATABASE_URL < migrations/008_add_ticker_sector.sql

# Seed reference data
python3 tools/seed_company_names.py
python3 tools/seed_sectors.py

# Run dev server
python3 -m uvicorn main:app --reload
```

### Required environment variables

A `.env.example` listing every variable is on the Phase-5 Group B to-do list. The full set in use today:

| Variable | Required? | Used by |
|---|---|---|
| `DATABASE_URL` | yes | asyncpg pool (`db/connection.py`); strip the `+asyncpg` dialect — handled by the helper |
| `REDIS_URL` | yes | Redis client (`db/redis.py`) |
| `ALPHA_VANTAGE_KEY` | yes (live) | Narrative `NEWS_SENTIMENT` |
| `FINNHUB_KEY` | yes (live) | Narrative fallback news, insider, analyst |
| `FRED_API_KEY` | yes (live) | Treasury yields and TED-substitute |
| `LOG_LEVEL` | optional | Default `INFO` |
| `LOG_FORMAT` | optional | `json` (Railway) or `text` (local) |

API authentication uses bearer tokens (`Authorization: Bearer sk-sm-…`); keys are stored as SHA-256 hashes in the `api_keys` table. Tier is either `free` (10 req/min, basic fields) or `pro` (600 req/min, full breakdown via `?detail=full`: sub-indices + drivers + explanation + `score_raw`). Keys are minted via `python3 scripts/tools/generate_keys.py` — the plaintext key is printed once at creation and never recoverable from the DB.

---

## API contract

Auth is a bearer token in the `Authorization` header on every `/v1/*` endpoint. `/health` is the only unauthenticated endpoint.

```
GET /v1/sentiment/{ticker}             — latest cached score
GET /v1/sentiment/{ticker}/history     — history (Pro only)
GET /v1/tickers                        — supported universe (502 tier-1)
GET /v1/status                         — last-run timestamps per job
GET /health                            — liveness + tier echo
```

A free-tier response carries `ticker`, `score`, `label`, `confidence`, `timestamp`, `cache_age_seconds`, and `market_hours`. A pro-tier response (returned only when `?detail=full` is set on a Pro key — otherwise the summary body is served) also includes `score_raw`, `ema_obs_count`, `sub_indices`, `missing_layers`, `divergence`, `top_drivers`, `explanation`, `freshness`, and `confidence_flags`. The full Pydantic schemas live in [`api/response/schemas.py`](api/response/schemas.py).

---

## Repository layout

```
sentimentapi/
├── api/                       System B — request-serving FastAPI
│   ├── auth.py                Bearer → SHA-256 → api_keys row → tier
│   ├── rate_limit.py          Per-key sliding-window via Redis INCR
│   ├── routes/                sentiment, history, tickers, status, health
│   └── response/              assembler, labels, schemas
│
├── pipeline/                  System A — background scoring engine
│   ├── scheduler.py           APScheduler job registration
│   ├── orchestrator.py        Per-ticker single-tick scoring
│   ├── rate_limits.py         Shared semaphores + guarded_get backoff
│   ├── sources/               market, narrative, influencer, macro,
│   │                          short_volume, fred
│   ├── nlp/                   dedup (cosine clustering), finbert (scoring)
│   ├── features/              normalize.py — z-score + weight formula
│   ├── scoring/               subindices, composite, ema, drivers, divergence
│   ├── confidence/            staleness, scorer
│   ├── explanation/           templates (free), gpt (pro — planned)
│   ├── persistence/           redis_writer, pg_writer
│   └── scripts/               backfill_short_volume
│
├── db/                        Database access layer
│   ├── connection.py          asyncpg pool with lazy init
│   ├── redis.py               redis.asyncio client
│   └── queries/               One module per table
│
├── backfill/                  One-time historical loaders
│   ├── ohlcv_backfill.py      AV TIME_SERIES_DAILY_ADJUSTED → raw_signals
│   ├── etf_backfill.py        Sector ETF closes → raw_signals
│   ├── indicators_backfill.py Wilder's RSI(14) computed from close history
│   └── universe_seed.py       Seed 502 S&P 500 tickers
│
├── tools/                     Operational scripts
│   ├── db_viewer.py           TUI for live DB inspection
│   ├── db_health.py           Pipeline health checks
│   ├── generate_keys.py       Mint new API keys
│   ├── seed_company_names.py  / seed_sectors.py
│   └── backfill_finbert.py    / backfill_fred.py
│
├── tests/                     33 test files (31 unit + 2 integration)
├── migrations.sql             Base schema (migrations 001–004)
├── migrations/                Additive migrations 005–008
├── main.py                    FastAPI app + lifespan (init pool → init redis → scheduler.start)
├── railway.toml               nixpacks deployment config
├── conftest.py                Auto-deselects @pytest.mark.integration
├── pytest.ini                 asyncio_mode=auto, pythonpath=.
├── requirements.txt
├── CHANGELOG.md               Phase-by-phase change log
└── docs/                      Audits, sprint plans, diagnostics
    ├── audit_A_market_postphase1.md          (paper compliance — market)
    ├── audit_B_narrative_postphase1.md       (paper compliance — narrative)
    ├── audit_C_composite_postphase1.md       (paper compliance — composite)
    ├── audit_D_influencer_macro_postphase1.md(paper compliance — influencer + macro;
    │                                          most current — updated through P4.4)
    ├── diagnostic_narrative_2026_05_11.md    (pre-Phase-2 state snapshot)
    ├── sprintA_diagnostic.md                 (FinBERT integration plan)
    ├── phase2_plan.md                        (Phase 2 sprint plan)
    ├── sprint_plan.md                        (24-sprint umbrella, Phases 1–4)
    └── spikes/apewisdom_2026_05_11.md
```

---

## Deployment

Production runs on [Railway](https://railway.app/) with nixpacks (`railway.toml`). Postgres and Redis are Railway-managed; the app starts as `python3 -m uvicorn main:app --host 0.0.0.0 --port $PORT`. Health-check path is `/health`.

Post-deploy steps for a fresh Railway environment:

```bash
for f in scripts/migrations/0*.sql; do psql $DATABASE_URL < "$f"; done

DATABASE_URL=$RAILWAY_URL python3 scripts/tools/seed_company_names.py
DATABASE_URL=$RAILWAY_URL python3 scripts/tools/seed_sectors.py
DATABASE_URL=$RAILWAY_URL python3 scripts/tools/generate_keys.py  # prints plaintext keys once
```

---

## License

See [`LICENSE`](LICENSE).
