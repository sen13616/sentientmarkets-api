# SentimentAPI — Scoring Methodology

This document is the complete, code-accurate description of how SentimentAPI turns raw
external data into the published 0–100 sentiment scores. Every constant, formula, weight,
schedule, and threshold below is transcribed from the implementation (file references given
throughout). All timestamps are UTC.

> **Guiding statement (nowcasting plan, 2026-07-21):** *Nowcast first — every published
> number is an honest, calibrated, responsive description of current cross-sectional
> sentiment. Anything that claims to predict must first beat the scorecard.* Every scoring
> change runs `scripts/eval/run.py` against the committed baseline before release.

---

## 1. Architecture Overview

Two decoupled systems:

- **System A (pipeline)** — APScheduler background jobs. **Ingestion jobs are data-only**:
  they fetch from external APIs and write to PostgreSQL; they never score. A single
  **`scoring_tick_job`** recomputes all four scoring layers for all ~502 tickers from
  current DB state. No score is ever computed in response to a user request.
- **System B (API)** — FastAPI serves the pre-computed state from Redis
  (`sentiment:{ticker}`), falling back to the latest `sentiment_history` row if Redis is
  cold.

The scoring pipeline for one ticker on one tick (`pipeline/orchestrator.py`):

```
raw_signals / raw_articles (DB)
        │
        ▼
1. Staleness filtering            (pipeline/confidence/staleness.py)
2. Normalization → scored 0–100   (pipeline/features/normalize.py)      "Layer 07"
3. Per-layer sub-indices          (pipeline/scoring/subindices.py)      "Layer 08"
4. Composite (4-layer weighted)   (pipeline/scoring/composite.py)
5. Divergence check + cap         (pipeline/scoring/divergence.py)
6. EMA smoothing                  (pipeline/scoring/ema.py)
7. Confidence penalties           (pipeline/confidence/scorer.py)       "Layer 09"
8. Driver extraction              (pipeline/scoring/drivers.py)
9. Templated explanation          (pipeline/explanation/templates.py)   "Layer 10"
        │
        ▼
Redis (sentiment:{ticker}) + PostgreSQL (sentiment_history, price_snapshots)
```

All four layers are **always recomputed from DB state — no carry-forward**. If a layer has
no fresh data, the last Redis-cached sub-index is reused *only* while it is within that
layer's staleness window (§8.3); otherwise the layer is marked missing and its composite
weight is redistributed.

---

## 2. Job Schedule (data ingestion timings)

All jobs are defined in `pipeline/scheduler.py` on an `AsyncIOScheduler(timezone="UTC")`.
US market hours are treated as **weekdays 14:30–21:00 UTC** (9:30 am–4:00 pm Eastern)
throughout the codebase.

| Job | Trigger (UTC) | What it does |
|---|---|---|
| `scoring_tick_job` | Every 30 min at :00/:30 around the clock, **plus** :15/:45 fills weekdays 14:45–20:45 (`OrTrigger`) → effective **15 min during market hours, 30 min off-hours** | The only scoring job. Recomputes all 4 layers for every active ticker, then publishes universe stats (§12) |
| `market_job` | Weekdays, hours 14–20, every 15 min (`*/15`) | One batched yfinance OHLCV download for all tickers, then per-ticker derived signals (RSI, returns, order flow, volume ratio, bid-ask) |
| `market_eod_job` | Weekdays 21:15 | Same as `market_job`; captures definitive closing prices 15 min after the close so the ~21:30 scoring tick produces the end-of-day state that stays fresh overnight/weekend |
| `narrative_job` | Every 30 min, 24/7 (interval) | 3 phases: (1) fetch news from Alpha Vantage + Finnhub, (2) semantic dedup clustering, (3) FinBERT scoring |
| `influencer_job` | Cron 00:20 / 06:20 / 12:20 / 18:20 (cron so deploys don't reset the 6 h cadence) | Insider transactions + analyst signals |
| `macro_daily_job` | Daily 02:00 | FRED Treasury signals (10y, 2y, 10y−2y slope) |
| `macro_intraday_job` | Weekdays, hourly 14:00–20:00 | VIX + 11 sector ETF closes / 20-day returns |
| `short_volume_job` | Weekdays 21:30 | FINRA REGSHO daily short volume (published ~21:30) |
| `retention_job` | Daily 03:30 | Tiered purges (§14) — never touches `sentiment_history` or `price_snapshots` |

All jobs run with `max_instances=1` and `coalesce=True`.

**Scoring concurrency:** `_score_all()` scores all tickers concurrently, bounded by
`Semaphore(10)` to stay inside the asyncpg pool. The ticker→GICS-sector map and the
1-day-change baselines are each preloaded **once per tick** (not per ticker).

### External API rate limiting (`pipeline/rate_limits.py`)

Every outbound `client.get()` goes through `guarded_get()` behind per-provider semaphores:

| Provider | Concurrency | Inter-request delay | Effective throughput |
|---|---|---|---|
| Alpha Vantage (premium) | Sem(1) | 0.85 s | ≈70/min |
| Finnhub (free) | Sem(1) | 2.1 s | ≈28/min |
| SEC EDGAR | Sem(5) | 0.5 s/slot | ≈8/sec |
| Polygon | Sem(1) | 0.85 s | — |
| FRED | Sem(2) | 0.5 s | well under 120/min |
| yfinance `Ticker.info` | Sem(10) | — | thread-pool bound |

Retry policy: HTTP 429 → backoff 2 s / 4 s / 8 s (max 3 retries); timeout/connect error →
one retry after 2 s; 403/404 (auth failures, don't consume quota) → 50 ms courtesy delay.

---

## 3. Signal Catalog — what is ingested and how each raw value is computed

### 3.1 Market layer (`pipeline/sources/market.py`)

| Signal | Source | Computation |
|---|---|---|
| `yf_open/high/low/close/volume` | yfinance batch `yf.download(period="5d", interval="1d", auto_adjust=True)` | Latest non-NaN daily bar. Polygon `/prev` exists only for backfills — it is **not** a live fallback (it returns yesterday's bar, which would poison returns/order-flow). Class shares map dots→dashes at the yfinance boundary only (BRK.B → BRK-B) |
| `bid_ask_spread_bps` | yfinance `Ticker.info` (market hours only) | `(ask − bid) / midpoint × 10,000`, midpoint = (bid+ask)/2. Rejected if bid/ask ≤ 0 or ask < bid. Only bps is persisted (raw bid/ask dropped 2026-07-20) |
| `rsi_14` | Computed from DB close history (50 rows) + current close | **Wilder's RSI(14)**: SMA seed over the first 14 changes, then exponential smoothing `avg = (avg×13 + change)/14`; `RSI = 100 − 100/(1+RS)`, `RS = avg_gain/avg_loss` (100 if avg_loss = 0). Stored `source='computed'` |
| `order_flow_imbalance` | Computed from current OHLCV bar | Close Location Value (Lee-Ready OHLCV proxy): `CLV = (2·close − high − low)/(high − low)` ∈ [−1, +1]. Skipped if volume ≤ 0 or high == low |
| `buy_pressure` / `sell_pressure` | Same bar | `buy = (1 + CLV)/2`; `sell = 1 − buy` |
| `return_1d/5d/20d` | Current close vs DB close history | **Log returns**: `ln(close_now / close_{−n})` for n = 1, 5, 20 sessions |
| `volume_ratio` | Current volume vs DB volume history | `volume_now / mean(last 20 sessions' volume)` |
| `short_volume_otc`, `short_volume_total_otc`, `short_volume_ratio_otc` | FINRA REGSHO daily files (`pipeline/sources/short_volume.py`) | CNMS consolidated file first; fallback sums the three TRFs (FNYX + FNSQ + FNQC). Walks back up to 4 calendar days for the latest file. `ratio = short_volume / total_volume`. Off-exchange volume only (hence `_otc`). Timestamped at the trading day's 21:00 close. Source `finra_regsho` |

DB queries accept both `yf_*` and legacy `ohlcv_*` naming via `IN (...)`.

### 3.2 Narrative layer (`pipeline/sources/narrative.py`)

- **Sources:** Alpha Vantage `NEWS_SENTIMENT` (limit 50; provides per-ticker
  `relevance_score` and provider sentiment) and Finnhub `/company-news` (last **3 days**;
  no provider sentiment; `relevance_score = 1.0` because the endpoint is ticker-keyed).
- **Stage 1 dedup (exact):** SHA-256 hash of article URL; batch-level in-memory dedup and
  a DB `hash_exists` check / `ON CONFLICT DO NOTHING`.
- **Language detection:** `langdetect` over title+summary at ingest (needs ≥ 20 chars);
  FinBERT only scores `language='en'`.
- **Stage 2 dedup (semantic event clustering, `pipeline/nlp/dedup.py`):** for each ticker,
  unclustered articles from the last **48 h** are encoded with sentence-transformers
  **`all-MiniLM-L6-v2`** (titles, normalized embeddings). Union-Find joins every pair with
  publication times within **4 hours** of each other and title cosine similarity
  **> 0.85**. Multi-member groups get a UUID `event_cluster_id`; singletons stay NULL.
- **FinBERT scoring (`pipeline/nlp/finbert.py`):** `ProsusAI/finbert` over
  `title + " " + summary` (truncated to 512 tokens, batches of 32, up to 500 articles per
  narrative run). Stored per article:
  `finbert_score = P(positive) − P(negative)` ∈ [−1, +1], plus the three class
  probabilities (`finbert_pos/neg/neu`).
- **Cluster collapse at scoring time** (`scripts/db/queries/raw_articles.py`
  `get_articles_since`): `DISTINCT ON (COALESCE(event_cluster_id, id::text))` ordered by
  `relevance_score DESC NULLS LAST, published_at DESC` — i.e. one article per event
  cluster, the **highest-relevance** one, and only rows with a non-null `finbert_score`.

### 3.3 Influencer layer (`pipeline/sources/influencer.py`)

| Signal | Source | Computation |
|---|---|---|
| `insider_net_shares` | Finnhub `/stock/insider-transactions` (30-day lookback; timestamped by transaction date). The SEC EDGAR Form 4 primary path was removed (P3.4) — its SGML wrapper breaks XML parsing and Finnhub carried 100% of the load | Raw `change` (net shares) per transaction row |
| `analyst_buy_pct` | Finnhub `/stock/recommendation` (most recent period) | `(strongBuy + buy) / (strongBuy + buy + hold + sell + strongSell)` ∈ [0, 1] |
| `analyst_target_price` | yfinance `Ticker.info["targetMeanPrice"]` | Raw mean target (must be > 0) |
| `analyst_eps_estimate_mean` | yfinance `get_earnings_estimate()`, `0q` row, `avg` column | Raw current-quarter mean EPS estimate; the *scored* signal is derived from it (§5.4) |

### 3.4 Macro layer (`pipeline/sources/macro.py`, `pipeline/sources/fred.py`)

Macro signals are **not per-ticker**. Market-wide rows are stored under the sentinel
ticker `_MACRO_`; sector ETF rows are stored under the ETF symbol.

| Signal | Source | Notes |
|---|---|---|
| `vix` | yfinance `^VIX` (primary) → Finnhub `/quote` → Alpha Vantage `GLOBAL_QUOTE` | Hourly during market hours |
| ETF `ohlcv_close` (11 rows) | Alpha Vantage `GLOBAL_QUOTE` per sector ETF | Stored as `ohlcv_close` so shared close-history queries find it |
| `sector_etf_return_20d` (per ETF) | Computed | `(close − close_{−20})/close_{−20}` — simple (not log) return; needs ≥ 20 stored sessions |
| `treasury_yield_10y` | FRED series `DGS10` | Daily 02:00; fetches a page of 10 observations so `.`-padded holidays can't blank the day; one 15 s retry per series |
| `treasury_yield_2y` | FRED series `DGS2` | idem |
| `ted_spread` | FRED series `T10Y2Y` | **TED-substitute**: the LIBOR TED spread was discontinued in 2022; this is the 10y − 2y yield-curve *slope* (positive/steep = bullish, inversion = bearish) — same credit-stress role, same sign convention |

GICS sector → ETF routing (`SECTOR_ETFS`): Communication Services→XLC, Consumer
Discretionary→XLY, Consumer Staples→XLP, Energy→XLE, Financials→XLF, Health Care→XLV,
Industrials→XLI, Information Technology→XLK, Materials→XLB, Real Estate→XLRE,
Utilities→XLU. Each ticker resolves via `ticker_universe.sector`; `sector IS NULL` drops
the ETF component silently.

---

## 4. Event-Level Weighting (per-signal weight `w_i`)

Every scored signal carries a weight (`pipeline/features/normalize.py`):

```
w_i = w_src · w_rel · w_conf · w_author · exp(−λ · Δt_i),   λ = ln(2) / half_life
```

- `w_rel` and `w_conf` apply **only to the narrative channel**; elsewhere they are 1.0.
- `w_author` applies only to the influencer channel and is uniformly **1.00** today
  (scaffold for a future CEO→CFO→Director role hierarchy).
- Weights are floored at `1e-6`.

### 4.1 Source-credibility weights `w_src` (`_SOURCE_WEIGHTS`)

| Source | Weight |
|---|---|
| polygon | 0.90 |
| yfinance | 0.90 |
| finra_regsho | 0.90 |
| computed | 0.85 |
| alpha_vantage | 0.75 |
| finnhub | 0.65 |
| *(unknown)* | 0.70 |

**Influencer override** — the influencer layer keys `w_src` by *signal channel*, not
provider (`_INFLUENCER_SIGNAL_WEIGHT`): `insider_net_shares` 1.00, `analyst_buy_pct` 0.85,
`analyst_target_price` 0.85, `earnings_estimate_revision` 0.80.

### 4.2 Time-decay half-lives

Layer defaults (`_LAYER_HALF_LIFE_H`): **market 1 h, narrative 12 h, influencer 72 h
(analyst default), macro 336 h (14 d)**. Signal-channel override:
`insider_net_shares` → **168 h (7 d)** regardless of provider. Fresh signal → weight
factor 1.0; a signal exactly one half-life old → 0.5.

### 4.3 Relevance weight `w_rel` (narrative only)

Per-article `relevance_score` from the provider. Articles with **no relevance score are
excluded**, and articles with **relevance < 0.60 are excluded** (inclusion threshold) —
both before any weighting.

### 4.4 Model-confidence weight `w_conf` (narrative only)

Inverse normalized entropy of FinBERT's 3-class output:

```
w_conf = 1 − ( −Σ Pₖ ln Pₖ ) / ln 3        ∈ [0, 1]
```

A dominant class (0.92/0.05/0.03) → w_conf ≈ 0.74; a uniform spread (⅓,⅓,⅓) → 0.
Missing probabilities fall back to `w_conf = 1.0`; invalid probabilities → 0.0.

---

## 5. Normalization — raw value → direction-corrected score in [0, 100]

Convention everywhere: **score > 50 = bullish, < 50 = bearish, 50 = neutral.** NaN/Inf
values are dropped.

### 5.1 Primary path: rolling z-score (`RollingZScorer`)

For each configured signal type, history is fetched from `raw_signals` and:

```
z = (x − μ) / max(σ, 1e-4)        (population σ over the window)
z clamped to [−3, +3];  negated for bearish-when-high signals
score = 50 + 50 · (z / 3)
```

Activation requires `len(history) ≥ max(min_obs, ⌈window × fill_threshold⌉)` with defaults
`min_obs = 30`, `fill_threshold = 0.5`. Returns None (→ parametric fallback) on
insufficient history or σ below the floor.

Per-signal z-score config (`_ZSCORE_CONFIG`):

| Signal | Window | Negated |
|---|---|---|
| `rsi_14` | 500 | ✔ (contrarian: high RSI = bearish) |
| `return_1d/5d/20d` | 500 | — |
| `volume_ratio` | 500 | — |
| `order_flow_imbalance` | 500 | — |
| `bid_ask_spread_bps` | 500 | ✔ (wide spread = bearish) |
| `short_volume_ratio_otc` | 90 | ✔ |
| `insider_net_shares` | 90 | — |
| `analyst_buy_pct` | 90 | — |
| `analyst_target_price` | 90 | — (z-scored on *upside*, see §5.4) |
| `earnings_estimate_revision` | 90 | — |
| `vix` | 90 | ✔ |
| `sector_etf_return_20d` | 90 (history keyed under the **ETF symbol**, not the ticker) | — |
| `treasury_yield_10y` | 90 | ✔ (rising yields bearish) |
| `treasury_yield_2y` | 90 | ✔ |
| `ted_spread` | 90 | — (it's the *slope*: steep = bullish) |

Macro global types (`vix`, treasuries, `ted_spread`) look up history under `_MACRO_`
regardless of the ticker being scored.

### 5.2 Fallback path: parametric scorers (`_SIMPLE_SCORERS`)

Used until a signal accumulates enough history. All clamp to [0, 100].

| Signal | Formula |
|---|---|
| `rsi_14` | `50 − 1.25·(RSI − 50)` — **contrarian** (RSI 70 → 25, RSI 30 → 75) |
| `return_1d` | `50 + 50·tanh(r / 0.02)` |
| `return_5d` | `50 + 50·tanh(r / 0.05)` |
| `return_20d` | `50 + 50·tanh(r / 0.10)` |
| `volume_ratio` | `50 + 15·tanh(vr − 1)`, clamped to **[20, 80]** (elevated volume = mild positive) |
| `order_flow_imbalance` | `50 + 50·CLV` |
| `buy_pressure` | `100·x` |
| `sell_pressure` | `100·(1 − x)` (inverted) |
| `bid_ask_spread_bps` | `50 − 50·tanh((bps − 10)/30)` — 10 bps neutral, wider = bearish |
| `insider_net_shares` | `50 + 50·tanh(shares / 100,000)` |
| `analyst_buy_pct` | `25 + 50·pct` (0% → 25, 50% → 50, 100% → 75) |
| `analyst_target_price` | upside `= (target − price)/price`; `50 + 50·tanh(upside / 0.15)`; skipped without a current price |
| `earnings_estimate_revision` | `50 + 50·tanh(Δ / 0.05)` — half-scale at ±5% revision |
| `vix` | `50 − (VIX − 22)/8 × 25` — linear, neutral at VIX 22 |
| `sector_etf_return_20d` | `50 + 50·tanh(r / 0.10)` |
| `treasury_yield_10y` | `50 − 50·tanh((y − 4.0)/1.5)` — neutral at 4.0% |
| `treasury_yield_2y` | `50 − 50·tanh((y − 4.5)/1.5)` — neutral at 4.5% |
| `ted_spread` | `50 + 50·tanh(slope / 1.0)` |

### 5.3 Narrative scoring

Per surviving (deduped, relevance ≥ 0.6, English, FinBERT-scored) article:

```
score  = 50 + 50 · finbert_score            (finbert_score = P(pos) − P(neg))
weight = w_src(source) · exp(−λ·Δt) · relevance · w_conf     (λ from 12 h half-life)
```

Emitted as signal type `finbert_sentiment` — each article/event is one signal.

### 5.4 Influencer derivations

- **`analyst_target_price`** stores the raw target, but the quantity z-scored is the
  **upside vs the current close**: history rows are transformed to
  `(target_h − price_now)/price_now` and the current upside is z-scored against them.
  Cold start falls back to the parametric upside formula.
- **`earnings_estimate_revision`** is *derived* at scoring time from the raw
  `analyst_eps_estimate_mean` history (up to 120 rows): current delta
  `Δ = (latest − prior)/|prior|`; a delta history is built from consecutive pairs
  (excluding the final pair, which equals Δ itself and would bias the z-score), then
  z-scored, with the tanh parametric as fallback.

### 5.5 RSI sign-convention divergence (intentional)

The **normalizer** treats RSI as **contrarian** (RSI 70 → score 25, bearish overbought).
The **market sub-index** (§7.2) uses the raw RSI value as a **momentum** indicator
(RSI 70 → +1, bullish). Both are deliberate: the sub-index is trend-following; the
contrarian read surfaces in driver descriptions.

---

## 6. Staleness (`pipeline/confidence/staleness.py`)

### 6.1 Layer-level thresholds (drive confidence penalties)

| Source | Stale after |
|---|---|
| market | 90 min *(market-hours-aware — see below)* |
| news | 6 h |
| analyst | 3 d |
| insider | 30 d |
| macro | 72 h (covers weekends when macro_intraday pauses) |

`as_of = None` (never received) counts as stale. **Market-hours awareness:** during market
hours the 90-minute rule applies; outside hours a market timestamp is fresh iff it is at or
after `last_market_close − 30 min` (EOD grace) — so the 21:15 EOD snapshot stays fresh all
night and all weekend.

### 6.2 Per-signal staleness (filters individual market rows before scoring)

| Signal type(s) | Max age (market hours) | Outside market hours |
|---|---|---|
| `yf_*`, `ohlcv_*`, `order_flow_imbalance`, `buy/sell_pressure` | 30 min | fresh if from the most recent session (close − 30 min grace) |
| `rsi_14` | 60 min | fresh if from the most recent session |
| `bid_ask_spread_bps` | 30 min | **always stale** (quotes meaningless after close) |
| `short_volume_*` | dedicated daily-cadence logic | expected publish 22:00 + 2 h grace; the freshest expected file's trading-day close is the cutoff; Friday's file is valid through Monday morning |
| anything else | 90 min default | fresh if from the most recent session |

### 6.3 DB fetch lookbacks (orchestrator)

| Layer | Raw-data query window |
|---|---|
| market | 90 min during hours; back to the last session's **open** (14:30) outside hours |
| narrative | 3 days (matches the fetcher's article lookback) |
| influencer | 30 days (matches insider ingest window; a 3-day window would silently drop all insider rows) |
| macro | 72 h |

---

## 7. Sub-Indices (Layer 08) — one 0–100 value per layer

### 7.1 Generic aggregator — narrative & influencer (`compute_sub_index`)

```
raw   = Σ(wᵢ · scoreᵢ) / Σ(wᵢ)                weighted average
value = 50 + min(1, n/5) · (raw − 50)          volume shrinkage toward neutral
```

The `min(1, n/5)` shrinkage pulls thin layers toward 50 — fewer than 5 signals means
proportionally less conviction; a single signal only moves the layer 20% of the way from
neutral. Returns None (missing layer) if no signal has positive weight.

### 7.2 Market sub-index — structured 6-component aggregation (`compute_market_sub_index`)

Components are each mapped to [−1, +1], then combined with explicit weights
(`MARKET_COMPONENT_WEIGHTS`, sum = 1.0):

| Component | Weight | Input → [−1, +1] mapping |
|---|---|---|
| returns | 0.30 | mean over available `return_1d/5d/20d` of `(score − 50)/50` (most recent row per type) |
| momentum | 0.15 | **raw RSI**, momentum convention: `clamp((RSI − 50)/20, −1, 1)` |
| order_flow | 0.20 | `(score − 50)/50` of `order_flow_imbalance` |
| liquidity | 0.10 | `(score − 50)/50` of `bid_ask_spread_bps` (already inverted in scorer) |
| short_volume | 0.15 | `(score − 50)/50` of `short_volume_ratio_otc` (already negated by normalizer) |
| volume | 0.10 | `(score − 50)/50` of `volume_ratio` |

Missing components have their weight **redistributed proportionally** across present ones
(no bias toward neutral). If **≤ 1 component** is present the whole layer is missing
(None). Final value: `50 + 50 · clamp(Σ wₑ·cₑ, −1, 1)`. No shrinkage. `buy_pressure` /
`sell_pressure` are scored but excluded from the sub-index (driver extraction only).

### 7.3 Macro sub-index — paper-direct weights, no shrinkage (`compute_macro_sub_index`)

Per-signal-type weights (`_MACRO_SIGNAL_WEIGHTS`; one row per type, newest kept):

| Signal | Weight |
|---|---|
| `sector_etf_return_20d` | **1.5** (only per-ticker macro input — carries the most per-ticker macro information) |
| `vix` | 1.0 |
| `treasury_yield_10y` | 1.0 |
| `ted_spread` (10y−2y slope) | 1.0 |
| `treasury_yield_2y` | 0.75 (down-weighted: highly correlated with the 10y) |

`value = Σ (wₜ / Σ present w) · scoreₜ`. **No `min(1, n/d)` shrinkage** — macro signals are
market-wide state variables present at every tick; the epistemic-caution argument for
shrinkage in event-driven channels doesn't apply. Missing signals are absorbed by
renormalizing over present weights.

---

## 8. Composite Score

### 8.1 Layer weights (`compute_composite`)

```
composite = 0.35·market + 0.30·narrative + 0.25·influencer + 0.10·macro
```

Missing layers have their weight **redistributed proportionally** over present layers
(weights always renormalize to 1.0). All layers missing → neutral 50.0.

### 8.2 Exogenous composite `score_exo` (`compute_exo_composite`)

The sentiment-only view: same computation with the price-derived **market layer forcibly
excluded** and weights renormalized over narrative/influencer/macro
(≈ 0.46 / 0.38 / 0.15 when all three are present). Returns **None — never a fabricated
50 —** when all three exo layers are missing, so the API serves null. No EMA, no
divergence cap. Stored as `composite_score_exo` (migration 010).

### 8.3 Redis fallback for a missing layer

If a layer's fresh computation yields nothing, the last Redis-cached sub-index is reused
**iff** its `{layer}_as_of` is within the layer staleness window (market: market-hours-aware
check; others: 90 min / 6 h / 3 d / 72 h per `_LAYER_LOOKBACK`). Beyond that the layer is
genuinely missing → weight redistribution + confidence penalty.

---

## 9. Divergence (`pipeline/scoring/divergence.py`)

Over the **present** sub-index values:

```
spread = max(values) − min(values)
flag   = spread > 40 → "high_divergence"
         spread > 20 → "moderate_divergence"
         otherwise   → "aligned"
```

**Extreme-imbalance cap:** if any sub-index > 85 **and** any sub-index < 30, the composite
is capped at **75** (prevents runaway optimism against one strongly bearish layer).
Fewer than two present layers → spread 0, "aligned", no cap. The capped value
("effective score") is what flows into EMA smoothing and is served as `score_raw`.

---

## 10. EMA Smoothing (`pipeline/scoring/ema.py`)

Variable-timestep exponential moving average applied to the (divergence-capped) raw
composite:

```
smoothed_t = α · raw_t + (1 − α) · smoothed_{t−1}
α = 1 − 0.5^(dt / T½),   T½ = 4 hours   (env-tunable: EMA_HALF_LIFE_HOURS)
```

- **Cold start:** first-ever score → `smoothed = raw` (seeded, `ema_obs_count = 1`).
- **Gaps:** handled naturally — as dt → ∞, α → 1 (after a 24 h gap α ≈ 0.984, effectively
  a reset). No threshold-based reset logic.
- `dt = 0` → previous smoothed value unchanged.
- `ema_obs_count` increments every tick and **never resets**.
- The 4 h default only changes if `scripts/eval/experiments/ema_halflife.py` wins on the
  eval scorecard.

**`score` (served) = smoothed. `score_raw` (served) = unsmoothed effective composite.**
In the DB, `sentiment_history.composite_score` stores the **raw** value and
`composite_score_smoothed` the EMA value.

---

## 11. Confidence (Layer 09, `pipeline/confidence/scorer.py`)

Start at 100, subtract penalties, clip to [0, 100], return an integer:

| Condition | Penalty |
|---|---|
| Missing layer | −15 **per layer** |
| Stale source (per §6.1 layer thresholds over 5 sources: market/news/analyst/insider/macro) | −10 **per source** |
| Low signal volume (total positive-weight signals across all layers < 5) | −20 |
| `high_divergence` flag | −15 |

Active penalties are exposed as `confidence_flags` (e.g. `missing_layer:macro`,
`stale:news`, `low_signal_volume`, `high_divergence`).

---

## 12. Drivers & Explanation

### 12.1 Driver extraction (`pipeline/scoring/drivers.py`)

Every scored signal across all layers is ranked by:

```
importance = weight × |score − 50| / 50
```

(high weight alone isn't enough — a neutral signal doesn't drive the composite).
Deduplicated to the highest-importance instance per signal type; top **5** kept. Each
driver record carries:

- `direction`: bullish if score > 52, bearish if score < 48, else neutral
- `magnitude`: `|score − 50| / 50` ∈ [0, 1]
- `confidence`: `min(1, weight)`
- a templated plain-English `description` built from the raw value.

### 12.2 Explanation (`pipeline/explanation/templates.py`)

Rule-based, 1–3 sentences, built **only** from the extracted drivers (never invents
reasons): top ≤ 3 drivers are split into primary-direction and counter-direction; a lead
sentence is built from the top 1–2 same-direction drivers, with a counter-signal sentence
appended when a counter driver has magnitude ≥ 0.3. Strength qualifiers
(strong/moderate/mild) are derived from magnitude.

---

## 13. Derived Serving Metrics

### 13.1 1-day change

`score_change_1d = smoothed_now − baseline`, where the baseline is the ticker's newest
smoothed score aged **24–48 h** (`get_baseline_scores`, one query per tick).
`score_change_1d_pct` is the same as a percentage of the baseline. If no row falls in the
24–48 h window (new ticker, or a data gap) both fields are **null — the change is never
computed across a gap**.

### 13.2 Universe / cross-sectional stats (end of every scoring tick)

Computed in-memory over **only the tickers scored this tick**
(`pipeline/scoring/market_overview.py`) and cached in Redis:

- **`pipeline:universe_percentile`** — percentile of the *smoothed* score:
  `100 × (# strictly below) / (n − 1)`; ties share; single ticker → 50.
- **`pipeline:universe_xs`** — cross-sectional stats on the **raw** score (the most
  responsive nowcast lens): `raw_z = (x − μ)/σ` (sample σ, unclamped; null if n < 2 or
  σ ≈ 0), `raw_pctl`, `sector_pctl` (percentile within the GICS sector; null if the sector
  has < 3 members this tick), and `exo_pctl` (percentile of `score_exo` among tickers with
  a non-null exo score).
- **`pipeline:market_overview`** — average score, breadth (% above 50, % improving among
  tickers with a 1-day change), top/bottom 10 movers by 1-day change (mover count halved
  when few tickers have baselines so no ticker appears on both lists), per-sector averages
  and within-sector ranks, plus a templated summary narrative. Served at
  `GET /v1/market/overview` (pro).

### 13.3 Label thresholds (`api/response/labels.py`)

0–20 Strongly Bearish · 21–40 Bearish · 41–60 Neutral · 61–80 Bullish · 81–100 Strongly
Bullish (applied to the integer-rounded smoothed score).

### 13.4 Tier field filtering (`api/response/assembler.py`)

- **Free:** `score` (smoothed, int), `score_raw`, `score_change_1d(_pct)`, `label`,
  `confidence`, `timestamp`, `cache_age_seconds`, `market_hours`.
- **Pro (`detail=full`):** everything above plus `sub_indices`, `missing_layers`,
  `divergence`, `top_drivers`, `explanation`, `freshness` (per-layer as-of),
  `confidence_flags`, `ema_obs_count`, `universe_percentile`, `score_raw_z`,
  `score_raw_percentile`, `sector_percentile`, `score_exo`, `score_exo_percentile`.

Serving order: Redis `sentiment:{ticker}` → latest `sentiment_history` row →
`insufficient_data` response.

---

## 14. Data Retention (affects z-score windows and history depth)

Daily 03:30 UTC (`retention_job`):

| Data | Retained |
|---|---|
| OHLCV rows (`yf_*`/`ohlcv_*`) | 365 d (50-day close lookback, 7× margin) |
| Derived intraday signals (returns, RSI, order flow, etc.) | 45 d (z-window 500 obs ≈ 20 trading days, 2× margin) |
| Quote telemetry | 14 d (no longer written) |
| All other raw signals | 90 d (covers the 90-obs z-score windows) |
| `raw_articles` | 30 d |
| `sentiment_history`, `price_snapshots` | **never deleted** (research data); `top_drivers` older than 30 d are compacted to a fixed-order array encoding (`pipeline/scoring/driver_codec.py`) |

Idempotency: `raw_signals` inserts are deduplicated via a `NOT EXISTS` guard on the
natural key (ticker, signal_type, timestamp, value, source).

---

## 15. Research-Only: Narrative Surprise (flag-gated, never served)

`pipeline/features/surprise.py`, active only when `ENABLE_NARRATIVE_SURPRISE=1`. Rationale:
the narrative *level* is already priced; only *new* information should carry forward
signal.

```
current  = relevance-weighted mean finbert_score over the last 24 h
baseline = daily relevance-weighted means over the trailing 14 days (excluding the window)
surprise = z-score of current vs baseline via RollingZScorer(window=14, min_obs=5,
           sigma_floor=1e-3) → mapped to [0, 100], 50 = in line with own baseline
```

Requires ≥ 10 baseline articles and ≥ 7 populated baseline days; otherwise None. Written
to Redis state and `sentiment_history.narrative_surprise` (migration 011) for eval
accumulation only — it never enters the composite or any API response until it beats the
`narrative_index` baseline on the eval scorecard.

---

## 16. Change Control & Research Discipline

### 16.1 Release gate

Any change to this methodology must pass the eval gate before release:

```
python3 -m scripts.eval.run --out exports/eval \
    --baseline scripts/eval/baselines/BASELINE_2026-07-21.json
```

The harness reads the production DB read-only and compares the candidate against the
committed baseline scorecard (`scripts/eval/baselines/README.md`). New features are
research-only — shadow, off by default, never served or added to the composite — until
they beat the incumbent on this gate.

### 16.2 Holdout split (frozen 2026-07-22, `scripts/eval/HOLDOUT.md`)

The backtest study (`docs/SUMMARYOFTESTING.md`) found the published scores
coincident-to-lagging with price. The research program iterating toward a leading signal
runs under a frozen train/holdout split, with the 2026-06-23 → 07-03 ingestion outage as
the natural boundary:

| Window | Dates |
|---|---|
| Research (development + ranking) | 2026-04-24 → 2026-06-22 |
| Holdout (confirmation only) | 2026-07-03 → present, open-ended |

`scripts/eval/run.py` enforces this via `--window research|holdout|all` (**default
`research`**): `--start/--end` default to the selected window's bounds and are rejected
outside them. `--window holdout` may only be run to confirm a candidate that already won
on the research window, and every holdout run is logged in `HOLDOUT.md`. `--window all`
requires explicit dates and is for reproduction/diagnostics, never candidate ranking.

### 16.3 Experiment log (`scripts/eval/EXPERIMENTS.md`)

Every configuration evaluated gets one row — including failures — so the
multiple-comparisons denominator N is always known when judging whether a candidate's
edge is significant. E000 is the incumbent baseline; a candidate progresses
`promising → won-research → confirmed (holdout) → shipped`.

### 16.4 Known constraints (from the backtest study)

- **News ingestion latency is the binding problem**: median publication→ingestion is
  ~8.4 h (Alpha Vantage) / ~2.7 h (Finnhub) on a 30-min polling sweep — most news is
  already priced by the time it is scored.
- **Re-weighting existing lagging layers cannot create lead** — only new, faster, or
  differently-timed information can.
- ~3 months of a single regime is thin; that is why the holdout freeze and experiment
  log exist.
