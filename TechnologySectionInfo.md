# Technology Page — Content Brief

> **For the website builder:** This document contains the real, verified content for the Technology page, sourced directly from the API codebase and methodology. Everything below is accurate to the deployed system (Phase 4, as of 2026-05-16) — no filler. Section headings suggest a page structure, but feel free to adapt layout, visuals, and ordering to the site's design. Suggested visuals are marked with 💡. Don't invent additional technical claims beyond what's here.

---

## Hero / Intro

**Headline idea:** One score. Four channels. Every 30 minutes.

SentientMarkets distills thousands of market signals into a single 0–100 sentiment score for every stock in the S&P 500 (502 tickers). A continuously running pipeline ingests price action, financial news, insider and analyst activity, and macroeconomic data — then normalizes, weighs, and blends them into one number you can act on, refreshed every 30 minutes, around the clock.

The methodology is fully documented in our research paper, *"How to Quantify Stock Sentiment"*, and this API is its reference implementation: every formula, weight, and threshold described below exists in the code.

---

## The Four Channels

The composite score is a weighted blend of four independent sentiment channels:

| Channel | Weight | What it measures |
|---|---|---|
| **Market** | 35% | What the price action itself is saying |
| **Narrative** | 30% | What the financial press is saying |
| **Influencer** | 25% | What insiders and analysts are doing |
| **Macro** | 10% | What the broader economy is saying |

If a channel has no fresh data for a ticker, its weight is redistributed proportionally across the remaining channels — the score degrades gracefully rather than going stale.

💡 *Suggested visual: a donut/stacked-bar showing the 35/30/25/10 split, with each segment expandable into the channel detail below.*

### 1. Market (35%)

Six components of price-and-volume behavior, each mapped to a bullish–bearish scale and weighted:

| Component | Weight | Signal |
|---|---|---|
| Returns | 30% | 1-day, 5-day, and 20-day returns |
| Order flow | 20% | Intraday buying vs. selling pressure (close-location value) |
| Momentum | 15% | RSI-14 (Wilder's) |
| Short volume | 15% | FINRA daily short-volume ratio |
| Liquidity | 10% | Bid–ask spread (wider spreads read bearish) |
| Volume | 10% | Volume relative to recent average |

Market data updates every 15 minutes during US trading hours, with a definitive end-of-day close capture after the bell.

### 2. Narrative (30%)

Financial news, scored by a transformer model — not keyword counting.

- **Ingestion:** News for all 502 tickers is pulled every 30 minutes, 24/7, from two independent providers (Alpha Vantage and Finnhub).
- **Relevance filtering:** Articles must clear a relevance threshold (score ≥ 0.60) to count toward a ticker at all — passing mentions don't move the score.
- **Event deduplication:** When ten outlets cover the same story, that's one event, not ten signals. Articles published within a 4-hour window whose titles are semantically near-identical (sentence-embedding cosine similarity > 0.85) are clustered, and only the most relevant article in each cluster is scored.
- **Sentiment model:** Each surviving article is scored with **FinBERT** (ProsusAI/finbert), a BERT-family transformer fine-tuned on financial text. The sentiment score is the model's positive-class probability minus its negative-class probability, giving a continuous value from −1 to +1.
- **Model confidence:** FinBERT's own uncertainty (the entropy of its class probabilities) down-weights ambiguous articles — a confidently bullish article counts more than a hedged one.

### 3. Influencer (25%)

Not social media chatter — the people with real skin in the game:

| Signal | Weight within channel | Source |
|---|---|---|
| Insider net buying/selling | 1.00 (highest) | Regulatory insider-transaction filings |
| Analyst buy/hold/sell consensus | 0.85 | Analyst consensus data |
| Analyst price targets vs. current price | 0.85 | Analyst target data |
| Earnings estimate revisions | 0.80 | Forward EPS estimate changes |

Insider transactions get the highest weight and the longest memory (a 7-day half-life vs. 3 days for analyst signals) — insiders trade on longer horizons than headlines.

### 4. Macro (10%)

Market-wide state variables that apply to every ticker:

- **Sector momentum** — each stock is mapped to its GICS sector ETF (XLK for tech, XLV for healthcare, etc.), and the ETF's 20-day return is the highest-weighted macro input. This makes the macro channel partially *per-ticker*, not one-size-fits-all.
- **VIX** — the market's fear gauge (elevated VIX reads bearish).
- **Treasury yields** — 10-year and 2-year yields from the Federal Reserve's FRED database.
- **Yield-curve slope** — the 10y−2y spread, a classic recession-watch indicator.

---

## How Every Signal Is Weighted

Every individual signal — a news article, an insider trade, an RSI reading, a VIX print — enters the system with a weight built from five factors:

```
weight = source credibility × relevance × model confidence × author credibility × time decay
```

- **Source credibility** — every data provider carries a fixed trust weight (e.g., exchange-derived price data at 0.90, news providers at 0.65–0.75).
- **Relevance** — how directly a news article concerns the ticker (narrative channel only; sub-threshold articles are dropped entirely).
- **Model confidence** — FinBERT's certainty about its own classification (narrative channel only).
- **Author credibility** — reserved for a planned role-based hierarchy (CEO filings vs. director filings); currently neutral.
- **Time decay** — every signal fades exponentially. Half-lives are tuned per channel:

| Channel | Half-life |
|---|---|
| Market | 1 hour |
| Narrative | 12 hours |
| Analyst signals | 3 days |
| Insider transactions | 7 days |
| Macro | 14 days |

💡 *Suggested visual: an exponential decay curve with the five half-lives plotted on it.*

---

## Normalization: Making Signals Comparable

An RSI reading, a short-volume ratio, and a Treasury yield live on completely different scales. Before aggregation, every numeric signal is converted to a common 0–100 scale (50 = neutral) using a **rolling z-score** against that signal's own recent history:

```
z = (value − rolling mean) / rolling std dev, clamped to ±3
score = 50 + 50 × (z / 3)
```

The rolling window is 500 observations for intraday market signals and 90 for daily-cadence signals. Signals where "high" means "bearish" (VIX, short volume, bid–ask spread, yields) are sign-inverted so that above 50 always means bullish. When a signal doesn't yet have enough history for a reliable z-score, a calibrated parametric fallback takes over instead of producing a noisy score.

**Small-sample protection:** when a ticker has fewer than 5 signals in a channel, the channel's score is shrunk toward neutral (50) — one lone article can't swing a stock to "Strongly Bullish."

---

## From Signals to the Score You See

1. **Aggregate** — each channel's weighted signals combine into a 0–100 sub-index.
2. **Blend** — the four sub-indices combine at 35/30/25/10 into the raw composite.
3. **Smooth** — an exponential moving average with a 4-hour half-life is applied, so the published score reflects sustained shifts in sentiment rather than tick-to-tick noise. (Pro-tier responses include the unsmoothed raw score too.)
4. **Label** — the score maps to a plain-English label:

| Score | Label |
|---|---|
| 0–20 | Strongly Bearish |
| 21–40 | Bearish |
| 41–60 | Neutral |
| 61–80 | Bullish |
| 81–100 | Strongly Bullish |

Every response also carries a **confidence** value (0–100): it starts at 100 and takes penalties for stale data, missing channels, or channels that sharply disagree with each other. A score of 72 with confidence 90 and a score of 72 with confidence 55 are different animals, and the API tells you which one you're holding.

Pro-tier responses additionally expose the full breakdown: per-channel sub-indices, the top drivers behind the score, a generated explanation, divergence between channels, and data-freshness metadata.

---

## Data Sources

| Source | Provides |
|---|---|
| Yahoo Finance / Polygon | Price, volume, order-flow data (primary + fallback) |
| Alpha Vantage | Financial news (primary narrative source) |
| Finnhub | News fallback, insider transactions, analyst consensus |
| FINRA | Official daily short-volume files |
| FRED (Federal Reserve) | Treasury yields, yield-curve spread |
| CBOE (via market data) | VIX |

Redundant providers on the critical paths (price data, news) mean a single vendor outage doesn't blind the system.

---

## Architecture & Freshness

Two systems run side by side, separated by a cache boundary:

- **The pipeline** ingests raw data on per-source schedules (price data every 15 minutes during market hours; news every 30 minutes around the clock; insider/analyst data every 6 hours; macro daily) and recomputes all four channels for all 502 tickers **every 30 minutes**.
- **The API** is a read-only layer serving the latest scores from an in-memory cache, with full score history preserved in PostgreSQL — which powers the history endpoint and, over time, backtesting.

Because scoring reads from the cache rather than calling vendors per-request, responses are fast and every response includes `cache_age_seconds` so you always know exactly how fresh the score is.

💡 *Suggested visual: a simple left-to-right flow — data sources → ingestion → scoring engine (normalize → weigh → aggregate → smooth) → cache → API → you.*

---

## Honest Limitations (optional section — builds trust)

- Coverage is the S&P 500 (502 tickers); smaller caps aren't scored yet.
- Sentiment is a measurement of *current signal alignment*, not a price prediction.
- Scores update every 30 minutes; this is not a tick-level feed.
- Social-media sentiment is not currently an input — the influencer channel tracks insiders and analysts, whose actions are regulated and verifiable, rather than anonymous posts.
