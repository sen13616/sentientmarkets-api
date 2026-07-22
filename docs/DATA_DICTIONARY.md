# Data Dictionary

**Updated:** 2026-07-22 (post research-program build-out; EMA half-life change E004)
**Scope:** All PostgreSQL tables touched by the methodology described in the research paper "How to Quantify Stock Sentiment".

This document maps every column in the production schema to the methodology it implements. Use it as a Rosetta stone when reading the paper alongside the code.

The schema is defined across the numbered migrations in [`scripts/migrations/`](../scripts/migrations/):

- `000_initial_schema.sql` — base tables
- `005_add_company_name.sql`, `006_add_smoothed_score.sql`, `007_finbert_columns.sql`, `008_add_ticker_sector.sql`
- `009_api_key_labels.sql` — per-consumer key labels
- `010_add_score_exo.sql` — `sentiment_history.composite_score_exo`
- `011_add_narrative_surprise.sql` — `sentiment_history.narrative_surprise` (research-only)
- `012_research_features.sql` — `sentiment_history.research_features` JSONB (research-only feature store)

Apply order: `000` first, then `005`–`012` in numeric order. All migrations after 000 are additive (`ADD COLUMN IF NOT EXISTS`) and idempotent in practice.

> **Schema changes of 2026-07-20 (storage reclaim), reflected below:** `raw_signals` and `price_snapshots` have **no primary key** (the `id` columns remain but `raw_signals_pkey` / `price_snapshots_pkey` were dropped, along with `idx_price_snapshots_ticker_ts`); `price_snapshots` is written only during US market hours.

---

## `raw_signals`

**Purpose.** The numeric signal store. Every quantitative observation the pipeline ingests — an OHLCV bar, an RSI reading, a VIX print, an insider net-shares total, a 20-day sector ETF return — is appended here as a single row. Layer 07 (`pipeline/features/normalize.py`) reads this table to compute z-scores, parametric fallbacks, and the per-signal weight `w_i`.

**No `UNIQUE` constraint, no primary key** (since 2026-07-20 — `raw_signals_pkey` dropped for storage; the `id` column remains but nothing queries by it). Per-tick deduplication is done at write time via a `NOT EXISTS` guard on the natural key `(ticker, signal_type, timestamp, value, source)` in `insert_signals()`. The two indexes (`idx_raw_signals_lookup`, `idx_raw_signals_ticker_ts`) make these checks cheap.

**Retention tiers** (`retention_job`, daily 03:30 UTC): OHLCV > 365 d; derived intraday signals > 45 d; quote telemetry > 14 d; everything else > 90 d — **except `RESEARCH_RETAIN_SIGNAL_TYPES`, which are never purged** (`short_volume_*`, `insider_net_shares`, `analyst_buy_pct`, `analyst_target_price`, `analyst_eps_estimate_mean`, and the four options snapshot types — unbackfillable research raw material; see `scripts/db/queries/raw_signals.py`).

| Column | Type | Nullable | Meaning |
|---|---|---|---|
| `id` | `SERIAL PRIMARY KEY` | NO | Surrogate key. No methodological role. |
| `ticker` | `VARCHAR(10)` | NO | US-listed equity symbol. The reserved value `_MACRO_` is used for market-wide signals (VIX, FRED Treasuries, the TED-substitute) that are not per-ticker. Sector ETF closes are stored under the **ETF symbol** itself (`XLK`, `XLV`, …), then resolved per-ticker via the GICS routing introduced in P4.2. |
| `signal_type` | `VARCHAR(50)` | NO | The canonical name of the signal — see the [Signal type catalog](#signal-type-catalog) below. Used as the join key for everything downstream: parametric scorer lookup (`_SIMPLE_SCORERS`), z-score window config (`_ZSCORE_CONFIG`), signal-channel weight override (`_INFLUENCER_SIGNAL_WEIGHT`), signal-channel half-life override (`_INFLUENCER_SIGNAL_HALF_LIFE_H`), and sub-index component routing. |
| `value` | `FLOAT` | NO | The raw observation. Units depend on `signal_type` (price, percent, count of shares, basis points, …). Stored uninterpreted; direction correction and z-score normalization happen in Layer 07. |
| `source` | `VARCHAR(50)` | NO | Data provider label. Drives the per-source credibility weight `w_src` in `_SOURCE_WEIGHTS` (`normalize.py`). Current valid values: `alpha_vantage`, `finnhub`, `yfinance`, `polygon`, `computed`, `finra_regsho`, `fred`, `yfinance_options` (research-only, feeds no score). The historical label `sec_edgar` was retired in P3.4 (Form 4 path removed) and Phase 5 (Sprint C 8-K retraction). |
| `upload_type` | `VARCHAR(20)` | NO | Either `'manual_backfill'` (rows loaded by `backfill/*.py`) or `'live'` (rows written by `pipeline/sources/*.py`). Used to distinguish bootstrap history from live observations during diagnostics. Has no effect on scoring. |
| `timestamp` | `TIMESTAMPTZ` | NO | The **observation time** — when the underlying market event happened, NOT when the row was written. Drives the time decay `e^(−λΔt)` in the weight formula. |
| `created_at` | `TIMESTAMPTZ` | NO (default NOW) | When the row was inserted. Used only for diagnostic audits (ingestion lag, drift detection). |

### Signal type catalog

The complete enumeration of `signal_type` values currently written by the pipeline:

**Market layer (per-ticker, live from market hours)**

| `signal_type` | `source` | Layer 03 role |
|---|---|---|
| `yf_open`, `yf_high`, `yf_low`, `yf_close`, `yf_volume` | `yfinance` | Live OHLCV (primary). |
| `ohlcv_open`, `ohlcv_high`, `ohlcv_low`, `ohlcv_close`, `ohlcv_volume` | `alpha_vantage`, `polygon` | Historical backfill naming (also used for sector-ETF closes). Polygon is **backfill-only** — its `/prev` endpoint returns the prior day's bar, so it is deliberately not a live fallback. |
| `rsi_14` | `computed` | Wilder's 14-period RSI, derived locally from close history. |
| `return_1d`, `return_5d`, `return_20d` | `computed` | Log returns over the named window. |
| `volume_ratio` | `computed` | Current volume / 20-day average volume. |
| `order_flow_imbalance` | `computed` | Close-location value (CLV) in `[−1, +1]`. |
| `buy_pressure`, `sell_pressure` | `computed` | Decomposed CLV-derived buy/sell fractions in `[0, 1]`. Used for drivers, not for the market sub-index. |
| `bid_ask_spread_bps` | `yfinance` | Bid-ask spread in basis points, market hours only. (Raw `bid`/`ask`/`bid_ask_spread` stopped being written 2026-07-20 — they were write-only telemetry.) |

**Short-volume layer (per-ticker, FINRA daily file)**

| `signal_type` | `source` | Role |
|---|---|---|
| `short_volume_otc` | `finra_regsho` | Raw OTC short volume (shares). |
| `short_volume_total_otc` | `finra_regsho` | Raw OTC total volume (shares). |
| `short_volume_ratio_otc` | `finra_regsho` | `short_volume_otc / short_volume_total_otc`. The 5th component of the market sub-index. |

**Influencer layer (per-ticker, 6-hourly cadence)**

| `signal_type` | `source` | Layer 04 role |
|---|---|---|
| `insider_net_shares` | `finnhub` | Net shares bought (positive) or sold (negative) by insiders in the lookback window. `w_src = 1.00`, half-life 168 h — both signal-channel-keyed per paper §Event-Level Weighting. |
| `analyst_buy_pct` | `finnhub` | Fraction of analysts rating buy / outperform (0–1). `w_src = 0.85`, half-life 72 h. |
| `analyst_target_price` | `yfinance`, `finnhub` | Mean analyst 12-month price target. Normalized by computing `(target − current_price) / current_price` upside and z-scoring per-ticker. `w_src = 0.85`. |
| `analyst_eps_estimate_mean` | `yfinance` | Mean analyst current-year EPS estimate. Stored as the raw input; the *delta* (period-over-period relative change) is what feeds the sub-index. |
| `earnings_estimate_revision` | `computed` | Period-over-period relative delta of `analyst_eps_estimate_mean`. `w_src = 0.80`. |

**Options snapshots (per-ticker, daily 21:20 UTC — research-only, feeds no score)**

Commissioned 2026-07-22 (`pipeline/sources/options.py`, `options_job`). One snapshot per ticker per trading day from the yfinance option chain at the expiry nearest 30 calendar days. Unbackfillable — all four types are in `RESEARCH_RETAIN_SIGNAL_TYPES` and never purged. Evaluation is a future registered experiment (`scripts/eval/EXPERIMENTS.md` § Queued).

| `signal_type` | `source` | Role |
|---|---|---|
| `pcr_volume` | `yfinance_options` | Put/call volume ratio at the chosen expiry (both sides ≥ 50 contracts required). |
| `pcr_oi` | `yfinance_options` | Put/call open-interest ratio (both sides ≥ 100 contracts required). |
| `atm_iv_30d` | `yfinance_options` | At-the-money implied vol — mean of the call/put IV at the strike nearest spot; IVs sane-bounded to (0.05, 5.0) with flat-placeholder-surface rejection. |
| `iv_skew_25d` | `yfinance_options` | 25-delta skew **approximated by moneyness** (5% OTM put IV − 5% OTM call IV; yfinance provides no greeks). Positive = downside protection bid. |

**Macro layer**

| `signal_type` | `source` | Ticker key | Layer 06 role |
|---|---|---|---|
| `vix` | `alpha_vantage`, `finnhub`, `yfinance`, `computed` | `_MACRO_` | CBOE Volatility Index. Sign-inverted (high VIX = bearish). |
| `sector_etf_return_20d` | `computed` | per ETF symbol (`XLK`, `XLV`, …) | 20-day return on the sector ETF. Resolved **per-ticker** via the `ticker_universe.sector` → ETF map in `pipeline/sources/macro.SECTOR_ETFS`. |
| `treasury_yield_10y` | `fred` | `_MACRO_` | FRED series `DGS10`. Sign-inverted (rising yields = bearish for equities). |
| `treasury_yield_2y` | `fred` | `_MACRO_` | FRED series `DGS2`. Sign-inverted. |
| `ted_spread` | `fred` | `_MACRO_` | FRED series `T10Y2Y` (10y − 2y yield-curve slope). Used as the **substitute** for the discontinued 3M-LIBOR–3M-T-bill TED spread; sign convention: positive slope = bullish, inversion = bearish. |

---

## `raw_articles`

**Purpose.** Stores the text-bearing inputs to the Narrative channel. One row per article. Layer 05 reads this table, scores each row with FinBERT, computes `w_conf` from the class probabilities, applies relevance filtering, and assembles the narrative sub-index.

The unique index `(ticker, content_hash)` provides hash-based dedup at insert time (Stage 1). Semantic dedup (Stage 2) runs after the fetch phase of `narrative_job` and writes the cluster ID back to existing rows — see `event_cluster_id` below.

**Retention (changed 2026-07-22):** rows are kept **365 days** (was 30). Rows older than 30 days have `title`/`summary`/`source_url` blanked in place by the daily retention job (`title` set to `''` — it is NOT NULL); `published_at`, the FinBERT tone scores, `relevance_score`, `source`, `ticker`, `event_cluster_id`, `content_hash`, and `language` are all kept — the fields research feature-backfills need. `created_at` is the verified **ingestion timestamp** (`DEFAULT NOW()` at insert; 0 NULLs, 0 anomalies as of 2026-07-22) — `created_at − published_at` is the publication→ingestion latency measure used by the E002 audit.

| Column | Type | Nullable | Meaning |
|---|---|---|---|
| `id` | `SERIAL PRIMARY KEY` | NO | Surrogate key. |
| `ticker` | `VARCHAR(10)` | NO | The ticker the article is about. Tickers are written per AV's `ticker_sentiment` array or via Finnhub's per-symbol news endpoint — the article-to-ticker mapping is supplied by the upstream provider, not inferred locally. |
| `title` | `TEXT` | NO | Article headline. Used by the FinBERT scorer (concatenated with `summary`) and by the sentence-transformer encoder for semantic dedup. |
| `summary` | `TEXT` | YES | Article summary / lead paragraph. Not always present (Finnhub frequently returns title only). |
| `source` | `VARCHAR(50)` | NO | Data provider label. Used by Layer 07 to look up `w_src` from `_SOURCE_WEIGHTS`. Current valid values: `alpha_vantage`, `finnhub`. |
| `source_url` | `TEXT` | YES | Canonical URL to the original article. Used to compute `content_hash`. |
| `published_at` | `TIMESTAMPTZ` | NO | Publication timestamp as reported by the provider. Drives the time decay `e^(−λΔt)` in the narrative weight formula (half-life 12 h per paper, layer default). |
| `provider_sentiment` | `FLOAT` | YES | The provider's own sentiment score (AV's `ticker_sentiment_score`, range roughly `[−1, +1]`). **No longer used in scoring** as of Sprint A — FinBERT replaced it. Retained for audit and future comparison. Finnhub articles arrive with this column NULL. |
| `relevance_score` | `FLOAT` | YES | The provider's per-ticker relevance score (`[0, 1]`). Drives `w_rel`. Sprint D set the inclusion threshold to `relevance_score >= 0.60`; articles below the threshold or without an explicit score are excluded entirely (no default fill). Finnhub articles arrive with this column NULL and are therefore excluded by the threshold unless and until a Finnhub-side relevance proxy is added. |
| `content_hash` | `VARCHAR(64)` | YES | SHA-256 of the article URL (or title fallback). Backs the unique `(ticker, content_hash)` index for Stage 1 dedup (`ON CONFLICT DO NOTHING` on insert). |
| `event_cluster_id` | `VARCHAR(100)` | YES | Stage-2 semantic dedup output. Set by `pipeline/nlp/dedup.cluster_articles` after `narrative_job` completes its fetch phase. Articles whose titles are encoded by `all-MiniLM-L6-v2` and clustered within a 4-hour window at cosine similarity > 0.85 receive the same `event_cluster_id`. At read time, `get_articles_since()` uses `DISTINCT ON (COALESCE(event_cluster_id, id::text))` to pick the highest-relevance row per cluster — duplicate event coverage is collapsed to one weighted contribution. |
| `finbert_score` | `FLOAT` | YES | FinBERT-derived sentiment: `P(positive) − P(negative)`, range `[−1, +1]`. Paper line 238. Replaced `provider_sentiment` as the canonical narrative score in Sprint A. |
| `finbert_pos` | `DOUBLE PRECISION` | YES | `P(positive)` from the FinBERT three-class softmax. |
| `finbert_neg` | `DOUBLE PRECISION` | YES | `P(negative)`. |
| `finbert_neu` | `DOUBLE PRECISION` | YES | `P(neutral)`. The three columns are required for `w_conf` computation: `w_conf = 1 − (−Σ P_i ln P_i) / ln 3`. |
| `language` | `VARCHAR(10)` | YES | Detected language (`langdetect`). FinBERT was trained on English; articles where this column is anything other than `'en'` are excluded from scoring. |
| `created_at` | `TIMESTAMPTZ` | NO | Insertion time. |

### Article scoring pipeline (per article)

```
ingest        →  raw_articles row written, language detected, content_hash set
relevance     →  IF relevance_score < 0.60 OR NULL THEN excluded
                 ELSE w_rel = relevance_score
language      →  IF language != 'en' THEN excluded
finbert       →  finbert_pos, finbert_neg, finbert_neu, finbert_score computed
w_conf        →  w_conf = 1 − entropy(pos, neg, neu) / ln(3)
weight        →  w = w_src(source) · w_rel · w_conf · exp(−λ_narrative · Δt)
                 where λ_narrative = ln(2)/12h
dedup         →  DISTINCT ON (event_cluster_id) → only the highest-relevance
                 row per cluster contributes
sub-index     →  Σ(w · finbert_score) / Σ(w), then volume-shrinkage
```

---

## `sentiment_history`

**Purpose.** Append-only record of every scored output. One row per ticker per `scoring_tick_job` execution (**every 15 minutes during US market hours, 30 minutes off-hours**). This table backs the `/v1/sentiment/{ticker}/history` API endpoint and is the canonical source for offline backtests and the eval harness (`scripts/eval/`).

Composite scores are stored twice: `composite_score` is the raw weighted average; `composite_score_smoothed` is the EMA-smoothed value that the API serves as `score`. The `ema_obs_count` column is the per-ticker monotonic update counter that determines when EMA leaves cold-start.

| Column | Type | Nullable | Meaning |
|---|---|---|---|
| `id` | `SERIAL PRIMARY KEY` | NO | Surrogate key. |
| `ticker` | `VARCHAR(10)` | NO | Ticker scored. |
| `composite_score` | `FLOAT` | NO | The **raw** composite — `0.35·M + 0.30·N + 0.25·I + 0.10·Mac`, with missing-layer redistribution applied. The Pro tier returns this as `score_raw`. |
| `composite_score_smoothed` | `DOUBLE PRECISION` | YES | The EMA-smoothed composite (**2-hour half-life since 2026-07-22 per experiment E004**; 4 h before that — historical rows written pre-change carry the 4 h smoothing). The API returns this as `score`. NULL only for pre-EMA-era rows. (Added in migration 006.) |
| `composite_score_exo` | `DOUBLE PRECISION` | YES | The **exogenous sentiment-only composite**: narrative/influencer/macro renormalized with the price-derived market layer excluded. Served as `score_exo` (pro). NULL = all three exo layers missing — never a fabricated neutral. Unsmoothed, no divergence cap. (Migration 010.) |
| `narrative_surprise` | `DOUBLE PRECISION` | YES | **Research-only** (flag `ENABLE_NARRATIVE_SURPRISE`): z-score of the last-24h relevance-weighted FinBERT tone vs the ticker's own trailing-14-day baseline, mapped to [0, 100]. Never served, never enters the composite. Accumulating since 2026-07-21. (Migration 011.) |
| `research_features` | `JSONB` | YES | **Research-only feature store** (migration 012): `{feature_name: value}` — one column for all present and future research features, no schema change per feature. Current keys: `short_vol_z`, `insider_net_z` (`pipeline/features/positioning.py`, flag `ENABLE_POSITIONING_FEATURES`; point-in-time backfilled by `scripts/eval/backfill_features.py`). Read only by the eval harness, which auto-registers every key; never serialized into any API response (test-guarded). |
| `ema_obs_count` | `INTEGER` | NO (default 0) | Monotonic count of EMA updates for this ticker. Used to determine when the smoother leaves cold-start (raw-composite seeding) and starts applying the exponential blend. (Added in migration 006.) |
| `market_index` | `FLOAT` | YES | Market sub-index (`compute_market_sub_index`). NULL when 5 of 6 components were missing. |
| `narrative_index` | `FLOAT` | YES | Narrative sub-index. NULL when no scored articles within the lookback. |
| `influencer_index` | `FLOAT` | YES | Influencer sub-index. NULL when no signals from any of the four influencer channels. |
| `macro_index` | `FLOAT` | YES | Macro sub-index (`compute_macro_sub_index`). NULL when no recognized macro signals are fresh. |
| `confidence_score` | `SMALLINT` | NO, CHECK 0–100 | Layer-09 output: 100 minus penalties for missing layers, stale data, low signal volume, high divergence. Integer; no bonuses. |
| `confidence_flags` | `JSONB` | YES | Array of penalty-flag strings active at the time of scoring (e.g., `["macro_layer_missing", "narrative_stale"]`). |
| `top_drivers` | `JSONB` | YES | Ranked driver list. **Two storage formats by row age.** Rows ≤30 days old (and all new writes): array of objects `{signal, description, direction, magnitude, source_layer, confidence}` — Layer 08 builds this; Layer 10 templates verbalize it. Rows >30 days old: compacted by the daily retention job to fixed-order arrays `[signal, direction, magnitude, confidence, source_layer]` (`description` dropped — originals archived to `exports/top_drivers_verbose_*.csv.gz` before the 2026-07-20 backfill). Discriminate by first element type; encode/decode via `pipeline/scoring/driver_codec.py`. Export tools re-expand compact rows with `description: null`. |
| `divergence` | `VARCHAR(20)` | YES | One of `aligned`, `moderate_divergence`, `high_divergence`, or `null`. Set when sub-index spread > 20 (moderate) or > 40 (high). |
| `market_as_of`, `narrative_as_of`, `influencer_as_of`, `macro_as_of` | `TIMESTAMPTZ` | YES | Per-layer freshness timestamps — the maximum observation time across signals that contributed to each sub-index. Used by clients to surface data age. |
| `timestamp` | `TIMESTAMPTZ` | NO | The scoring-tick time (when this row's score was computed). |
| `created_at` | `TIMESTAMPTZ` | NO (default NOW) | DB insertion time. Usually within a few ms of `timestamp`. |

---

## `ticker_universe`

**Purpose.** The list of tickers that the scoring engine actively recomputes every 30 minutes. Tier 1 is the manually curated S&P 500 snapshot (502 rows as of 2026-04-24). Tier 2 is reserved for ticker-on-demand expansion that has not yet been wired up — no Tier-2 rows are written in production today.

| Column | Type | Nullable | Meaning |
|---|---|---|---|
| `id` | `SERIAL PRIMARY KEY` | NO | Surrogate key. |
| `ticker` | `VARCHAR(10)` | NO, UNIQUE | Symbol. Uppercase. |
| `tier` | `VARCHAR(20)` | NO, CHECK | `'tier1_supported'` (in the active S&P 500 snapshot) or `'tier2_requested'` (reserved; not currently populated). |
| `added_at` | `TIMESTAMPTZ` | NO (default NOW) | When the ticker entered the universe. |
| `last_requested_at` | `TIMESTAMPTZ` | YES | Reserved for Tier-2 staleness tracking. Not written by current code paths. |
| `company_name` | `VARCHAR(200)` | YES | Human-readable name. Backs the API's `/v1/tickers` response and the explanation templates. (Added in migration 005; seeded by `tools/seed_company_names.py`.) |
| `sector` | `VARCHAR(50)` | YES | GICS sector name — one of the 11 standard classifications. Joins to `pipeline.sources.macro.SECTOR_ETFS` to route each ticker to its sector ETF for the per-ticker macro sub-index (P4.2). 19 known-stale tickers (renamed / delisted / acquired since the 2024 snapshot) have their sector seeded from a hand-curated fallback in `tools/generate_sector_map.py`. (Added in migration 008; seeded by `tools/seed_sectors.py`.) |

---

## `api_keys`

**Purpose.** API authentication. Stores SHA-256 hashes of the keys minted by `tools/generate_keys.py`; the plaintext is printed once at creation and never recoverable from the DB. The `tier` column gates response filtering: free-tier responses omit sub-indices, drivers, and the explanation.

| Column | Type | Nullable | Meaning |
|---|---|---|---|
| `id` | `SERIAL PRIMARY KEY` | NO | |
| `key_hash` | `VARCHAR(128)` | NO, UNIQUE | SHA-256 hex digest of the plaintext bearer token. The auth dependency in `api/auth.py` hashes the incoming `Authorization` header value and looks it up here. |
| `tier` | `VARCHAR(20)` | NO, CHECK | `'free'` (10 req/min) or `'pro'` (600 req/min). |
| `owner_email` | `VARCHAR(255)` | YES | Free-form contact for the key holder. Optional. |
| `is_active` | `BOOLEAN` | NO (default TRUE) | Soft-delete flag. Inactive keys 401 regardless of hash match. |
| `created_at` | `TIMESTAMPTZ` | NO (default NOW) | |
| `last_used_at` | `TIMESTAMPTZ` | YES | Updated on every authenticated request. Diagnostic only. |

---

## `price_snapshots`

**Purpose.** Captures the price at the moment each `sentiment_history` row was written, so that backtest forward-return computation can join to a price that genuinely existed at the score timestamp (rather than relying on later-vintage data that may include corporate-action adjustments).

**Write-only research data.** Nothing in the app reads this table. Since 2026-07-20 it has **no primary key and no indexes** (dropped for storage — eval queries are sequential scans by design) and receives rows **only during US market hours** (off-hours rows were ~98% exact repeats of the prior close). Never purged.

| Column | Type | Nullable | Meaning |
|---|---|---|---|
| `id` | `SERIAL PRIMARY KEY` | NO | |
| `ticker` | `VARCHAR(10)` | NO | |
| `close` | `FLOAT` | NO | Latest close from `get_latest_close()` at the scoring tick. |
| `volume` | `BIGINT` | YES | |
| `timestamp` | `TIMESTAMPTZ` | NO | The scoring-tick time, matching the paired `sentiment_history` row. |
| `created_at` | `TIMESTAMPTZ` | NO (default NOW) | |

---

## `backtest_results`

**Purpose.** Precomputed forward returns, populated asynchronously some time after the original score row was written. **Currently unused (0 rows)** — the async backfill was never built; forward-return analysis is done by the eval harness (`scripts/eval/analyze.py`) directly from `sentiment_history` + `raw_signals` closes instead. Retained in the schema for a possible future materialization.

| Column | Type | Nullable | Meaning |
|---|---|---|---|
| `id` | `SERIAL PRIMARY KEY` | NO | |
| `ticker` | `VARCHAR(10)` | NO | |
| `score_timestamp` | `TIMESTAMPTZ` | NO | The `sentiment_history.timestamp` this row corresponds to. |
| `composite_score` | `FLOAT` | NO | Snapshot of the raw composite at score time. Denormalized to avoid a join during analysis. |
| `forward_return_1d` | `FLOAT` | YES | `(close(t+1d) − close(t)) / close(t)`. NULL until 1 day after `score_timestamp` has elapsed. |
| `forward_return_5d` | `FLOAT` | YES | 5-day return. |
| `forward_return_20d` | `FLOAT` | YES | 20-day return. |
| `created_at` | `TIMESTAMPTZ` | NO (default NOW) | |

---

## Indexes

| Index | Table | Columns | Backs |
|---|---|---|---|
| `idx_raw_signals_lookup` | `raw_signals` | `(ticker, signal_type, timestamp DESC)` | Per-signal history fetches by Layer 07 (`get_signal_history`). |
| `idx_raw_signals_ticker_ts` | `raw_signals` | `(ticker, timestamp DESC)` | Generic per-ticker recency scans. |
| `idx_raw_articles_ticker_ts` | `raw_articles` | `(ticker, published_at DESC)` | Narrative lookback (`get_articles_since`). |
| `idx_raw_articles_cluster` | `raw_articles` | `(ticker, event_cluster_id)` | Stage-2 dedup writes back the cluster ID; also used by the `DISTINCT ON` cluster collapse at read time. |
| `idx_raw_articles_hash` | `raw_articles` | `(ticker, content_hash)`, UNIQUE | Stage-1 dedup at insert (`ON CONFLICT DO NOTHING`). |
| `idx_sentiment_history_ticker_ts` | `sentiment_history` | `(ticker, timestamp DESC)` | The `/history` API endpoint. |
| `idx_backtest_ticker_ts` | `backtest_results` | `(ticker, score_timestamp DESC)` | Validation queries (table currently unused). |

Dropped 2026-07-20 (storage reclaim; nothing read them): `raw_signals_pkey`, `price_snapshots_pkey`, `idx_price_snapshots_ticker_ts`.
| `idx_ticker_universe_tier` | `ticker_universe` | `(tier)` | Filtered scans of the active universe in `scoring_tick_job`. |

---

## Reading order for paper auditors

Reviewers cross-checking the paper against the schema should approach the tables in this order:

1. **`raw_signals`** — verify that every signal the paper names exists with the expected `signal_type` label and that `source` matches the Data Collection table in the paper. See the [Signal type catalog](#signal-type-catalog) above.
2. **`raw_articles`** — verify the FinBERT class-probability columns exist and that `relevance_score >= 0.60` filtering is enforced at the query layer.
3. **`sentiment_history`** — verify the four sub-index columns (`market_index`, `narrative_index`, `influencer_index`, `macro_index`) exist and that both `composite_score` (raw) and `composite_score_smoothed` (EMA) are persisted.
4. **`ticker_universe.sector`** — verify GICS sector assignment for the per-ticker macro routing introduced in P4.2.
5. **`api_keys`** — verify hashes-not-plaintext.

For each row in `sentiment_history`, the corresponding `raw_signals` + `raw_articles` rows that contributed to it can be recovered via the per-layer `*_as_of` timestamps and the standard lookback windows declared in `pipeline/confidence/staleness.py`. This is the audit path for "for any given score, show me the underlying observations and their weights".
