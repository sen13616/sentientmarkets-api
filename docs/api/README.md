# SentimentMarkets Sentiment API — Usage Guide

**Base URL:** `https://sentimentapi-p.up.railway.app`

---

## Authentication

All requests require a Bearer token in the `Authorization` header.

```
Authorization: Bearer sk-sm-your-api-key
```

API keys are available in two tiers:

| Tier | Rate Limit | Access |
|---|---|---|
| Free | 10 requests/min | Composite score (`score` + unsmoothed `score_raw`), label, 1-day change, confidence |
| Pro | 600 requests/min | Full breakdown: sub-indices, drivers, explanation, cross-sectional stats, `score_exo`, market overview |

---

## Endpoints

### GET /v1/sentiment/{ticker}

Returns the latest pre-computed sentiment score for a US-listed equity ticker.

**Parameters**

| Parameter | Type | Location | Required | Description |
|---|---|---|---|---|
| ticker | string | path | yes | US equity ticker symbol e.g. AAPL |
| detail | string | query | no | `summary` (default) or `full` — `full` requires Pro tier |

> **Note:** Scores are pre-computed by the background pipeline. There is no on-demand refresh — the API only serves the latest cached score. A Pro key that omits `detail=full` receives the same summary body as the Free tier; the sub-indices, drivers, and explanation are only returned when `detail=full` is set.

**Free Tier Response**

```bash
curl -H "Authorization: Bearer sk-sm-your-key" \
  https://sentimentapi-p.up.railway.app/v1/sentiment/AAPL
```

```json
{
  "ticker": "AAPL",
  "score": 72,
  "score_change_1d": 3.25,
  "score_change_1d_pct": 4.73,
  "label": "Bullish",
  "confidence": 81,
  "timestamp": "2026-04-24T14:32:00Z",
  "cache_age_seconds": 480,
  "market_hours": {
    "is_open": true,
    "next_open": "2026-04-27T14:30:00Z",
    "last_close": "2026-04-24T21:00:00Z"
  }
}
```

`score_change_1d` / `score_change_1d_pct` (both tiers): smoothed score vs the most recent tick aged 24–48h. `null` when no such baseline exists (new ticker or a data gap) — the change is never computed across a gap.

**Pro Tier Response**

```bash
curl -H "Authorization: Bearer sk-sm-your-key" \
  "https://sentimentapi-p.up.railway.app/v1/sentiment/AAPL?detail=full"
```

```json
{
  "ticker": "AAPL",
  "score": 72,
  "score_raw": 70,
  "score_change_1d": 3.25,
  "score_change_1d_pct": 4.73,
  "universe_percentile": 87.3,
  "ema_obs_count": 143,
  "label": "Bullish",
  "confidence": 81,
  "sub_indices": {
    "market": 78.0,
    "narrative": 69.0,
    "influencer": 80.0,
    "macro": 61.0
  },
  "missing_layers": [],
  "divergence": "aligned",
  "top_drivers": [
    {
      "signal": "Insider transaction",
      "description": "Insider purchased 12,000 shares",
      "direction": "bullish",
      "magnitude": 0.8,
      "source_layer": "influencer"
    },
    {
      "signal": "RSI(14)",
      "description": "RSI(14) = 71.2 (overbought)",
      "direction": "bearish",
      "magnitude": 0.45,
      "source_layer": "market"
    }
  ],
  "explanation": "Sentiment is primarily driven by strong insider conviction. Near-term technical conditions show mild overbought pressure.",
  "freshness": {
    "market_as_of": "2026-04-24T14:30:00Z",
    "narrative_as_of": "2026-04-24T14:00:00Z",
    "influencer_as_of": "2026-04-24T08:00:00Z",
    "macro_as_of": "2026-04-24T02:00:00Z"
  },
  "confidence_flags": [],
  "timestamp": "2026-04-24T14:32:00Z",
  "cache_age_seconds": 480,
  "market_hours": {
    "is_open": true,
    "next_open": "2026-04-27T14:30:00Z",
    "last_close": "2026-04-24T21:00:00Z"
  }
}
```

Pro-tier fields beyond the Free set: `universe_percentile` (percentile of this ticker's smoothed score within the latest scoring tick; `null` if the ticker was absent from that tick), the cross-sectional raw-score stats `score_raw_z` / `score_raw_percentile` / `sector_percentile`, `score_exo` + `score_exo_percentile` (sentiment-only composite, price-derived market layer excluded), `ema_obs_count` (EMA update counter), `sub_indices`, `missing_layers`, `divergence`, `top_drivers`, `explanation`, `freshness`, and `confidence_flags`. (`score_raw` itself is on **both** tiers since 2026-07-21.) Each driver has exactly `signal`, `description`, `direction`, `magnitude`, and `source_layer` — there is no per-driver `confidence`.

> Storage note: API responses are always built from the most recent scoring tick, which stores drivers in the full format above. Rows older than 30 days are re-encoded server-side to a compact storage format, but those rows are never served by any endpoint — response shapes are unaffected.

---

### GET /v1/sentiment/{ticker}/history

Returns historical sentiment scores for a ticker. **Pro tier only.**

```bash
curl -H "Authorization: Bearer sk-sm-your-key" \
  "https://sentimentapi-p.up.railway.app/v1/sentiment/AAPL/history?days=30"
```

**Query Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| days | integer | 30 | Lookback window in days (max 365) |
| interval | string | _see note_ | `daily` (one record per day), `hourly` (one record per hour), or `raw` (every scoring cycle). When omitted, defaults to `raw` for `days=1` and `daily` for all other windows. |

**Response**

```json
{
  "ticker": "AAPL",
  "history": [
    {
      "timestamp": "2026-04-23T14:30:00Z",
      "score": 68,
      "score_raw": 66,
      "label": "Bullish",
      "confidence": 79,
      "sub_indices": {
        "market": 71.0,
        "narrative": 65.0,
        "influencer": 74.0,
        "macro": 58.0
      },
      "missing_layers": []
    }
  ]
}
```

---

### GET /v1/tickers

Returns the list of tickers in the supported universe.

```bash
curl -H "Authorization: Bearer sk-sm-your-key" \
  https://sentimentapi-p.up.railway.app/v1/tickers
```

**Response**

```json
{
  "universe_size": 502,
  "tickers": [
    { "ticker": "AAPL", "name": "Apple Inc.", "sector": "Information Technology" },
    { "ticker": "ABBV", "name": "AbbVie Inc.", "sector": "Health Care" }
  ]
}
```

Each entry is an object with `ticker`, `name` (company name, may be `null` if not yet seeded), and `sector` (GICS sector, may be `null`).

---

### GET /v1/market/overview

Universe-level statistics for the latest scoring tick. **Pro tier only** (free keys receive 403). Served from a single per-tick cached blob; returns 503 `temporarily_unavailable` before the first tick after a deployment.

```bash
curl -H "Authorization: Bearer sk-sm-your-key" \
  https://sentimentapi-p.up.railway.app/v1/market/overview
```

**Response**

```json
{
  "timestamp": "2026-07-15T14:30:00Z",
  "universe_scored": 502,
  "average_score": 55.2,
  "breadth_above_50_pct": 63.5,
  "breadth_improving_pct": 48.2,
  "summary": "Market sentiment is mildly bullish (avg 55, 64% of names above neutral). Information Technology and Financials lead; Energy lags. Biggest movers: AAPL (+3.2), MSFT (-1.5).",
  "top_movers": [
    { "ticker": "AAPL", "score": 72.0, "score_change_1d": 3.25, "score_change_1d_pct": 4.73 }
  ],
  "bottom_movers": [
    { "ticker": "MSFT", "score": 55.0, "score_change_1d": -1.5, "score_change_1d_pct": -2.65 }
  ],
  "sectors": [
    {
      "sector": "Information Technology",
      "average_score": 58.4,
      "size": 68,
      "tickers": [
        { "ticker": "NVDA", "score": 74.1, "rank": 1 },
        { "ticker": "AAPL", "score": 72.0, "rank": 2 }
      ]
    }
  ]
}
```

`timestamp` is the scoring tick the blob was computed at. `summary` is a deterministic, template-generated plain-English narrative of the tick — market mood (from `average_score`), breadth, leading/lagging sectors, and the largest up/down movers — suitable for display as a market sentiment summary; it only ever references figures already in the blob and is `""` in the rare pre-first-tick blob. `breadth_improving_pct` counts tickers with a positive `score_change_1d` among those with a 1-day baseline and is `null` when none have one; movers exclude tickers with `null` change (both can be empty right after a data gap). `sectors[].tickers[].rank` is the within-sector rank by score (1 = highest) — combine with `size` to render "#4 of 68 in Information Technology".

---

### GET /v1/status

Returns API health and last pipeline run timestamps. **Requires a valid API key (any tier)** and counts against the key's rate limit — use `/health` for unauthenticated liveness checks.

```bash
curl -H "Authorization: Bearer sk-sm-your-key" \
  https://sentimentapi-p.up.railway.app/v1/status
```

**Response**

```json
{
  "status": "operational",
  "market_is_open": true,
  "last_market_run": "2026-04-24T14:30:00Z",
  "last_narrative_run": "2026-04-24T14:00:00Z",
  "last_influencer_run": "2026-04-24T08:00:00Z",
  "last_macro_run": "2026-04-24T02:00:00Z",
  "last_eod_run": "2026-04-23T21:15:00Z",
  "last_scoring_tick_run": "2026-04-24T14:30:00Z"
}
```

Any `last_*_run` field is `null` if the corresponding job has no recorded run in Redis. Requests without a valid key receive 401.

---

### GET /health

Basic health check. No authentication required.

```bash
curl https://sentimentapi-p.up.railway.app/health
```

```json
{ "status": "ok" }
```

---

## Score Labels

| Score | Label |
|---|---|
| 0 – 20 | Strongly Bearish |
| 21 – 40 | Bearish |
| 41 – 60 | Neutral |
| 61 – 80 | Bullish |
| 81 – 100 | Strongly Bullish |

---

## Confidence Score

The `confidence` field (0–100) indicates how reliable the score is. It starts at 100 and is reduced by:

| Condition | Penalty |
|---|---|
| Missing data layer | −15 per layer |
| Stale data source | −10 per source |
| Low signal volume | −20 |
| High divergence between layers | −15 |

A confidence below 60 means the score is based on limited or outdated data and should be interpreted cautiously.

---

## Sub-Indices (Pro Tier)

The composite score is built from four independent sub-indices, each scored 0–100:

| Sub-Index | What It Measures | Sources |
|---|---|---|
| market | What money is doing — returns, momentum (RSI), order flow, liquidity, short volume | yfinance (OHLCV, bid-ask), Polygon (fallback), FINRA REGSHO (short volume); RSI computed locally |
| narrative | What the public information environment is saying — news sentiment (FinBERT-scored) | Alpha Vantage NEWS_SENTIMENT, Finnhub news |
| influencer | What analysts and insiders are doing — ratings, targets, EPS estimates, insider transactions | Finnhub (insider transactions, recommendations, price targets, EPS estimates) |
| macro | Whether the broader market environment is supportive — VIX, sector trends, yield curve | Finnhub / Alpha Vantage (VIX), Alpha Vantage (sector ETFs), FRED (Treasury yields) |

> Options positioning, put/call ratio, short interest, and implied volatility are **not** part of the implemented methodology. The SEC EDGAR Form 4 path was retired (Sprint P3.4); Finnhub is now the sole insider provider.

---

## Divergence Field (Pro Tier)

| Value | Meaning |
|---|---|
| `aligned` | All layers broadly agree (spread ≤ 20 points) |
| `moderate_divergence` | Layers show some disagreement (spread > 20 points) |
| `high_divergence` | Layers strongly disagree (spread > 40 points) — interpret with caution |

where spread = max(sub-index) − min(sub-index) across the available layers.

**Extreme-imbalance cap:** independently of the divergence flag, if any layer's sub-index is above 85 *and* any other layer's is below 30, the composite score is capped at 75. This cap is triggered by that specific high/low contradiction, not by the `high_divergence` flag itself.

---

## Freshness (Pro Tier)

The `freshness` object shows when each layer's data was last updated:

- **market_as_of** — refreshes every 15 minutes during market hours
- **narrative_as_of** — refreshes every 30 minutes
- **influencer_as_of** — refreshes every 6 hours
- **macro_as_of** — VIX and sector ETFs refresh hourly during market hours; FRED Treasury yields refresh daily at 02:00 UTC

If a layer's `as_of` timestamp is `null`, that layer had no data available and its weight was redistributed to the remaining layers.

---

## No-Data Response

Tickers with insufficient history or outside the supported universe return:

```json
{
  "ticker": "XYZ",
  "status": "insufficient_data",
  "message": "Not enough historical data to compute a reliable sentiment score yet."
}
```

Possible status values:

| Status | Meaning |
|---|---|
| `insufficient_data` | Ticker is supported but has no scored data yet |
| `ticker_not_found` | Ticker is not in the supported universe |
| `temporarily_unavailable` | Temporary service issue |

---

## Error Responses

| HTTP Status | Error Code | Meaning |
|---|---|---|
| 401 | unauthorized | Missing or invalid API key |
| 403 | forbidden | Endpoint requires a higher tier |
| 429 | rate_limit_exceeded | Too many requests — slow down |
| 500 | internal_error | Unexpected server error |

---

## Code Examples

**Python**

```python
import requests

API_KEY = "sk-sm-your-key"
BASE_URL = "https://sentimentapi-p.up.railway.app"

headers = {"Authorization": f"Bearer {API_KEY}"}

# Free tier
response = requests.get(f"{BASE_URL}/v1/sentiment/AAPL", headers=headers)
data = response.json()
print(f"AAPL sentiment: {data['score']} ({data['label']})")

# Pro tier full detail
response = requests.get(
    f"{BASE_URL}/v1/sentiment/AAPL",
    headers=headers,
    params={"detail": "full"}
)
data = response.json()
print(f"Market sub-index: {data['sub_indices']['market']}")
print(f"Explanation: {data['explanation']}")
```

**JavaScript**

```javascript
const API_KEY = "sk-sm-your-key";
const BASE_URL = "https://sentimentapi-p.up.railway.app";

// Free tier
const response = await fetch(`${BASE_URL}/v1/sentiment/AAPL`, {
  headers: { Authorization: `Bearer ${API_KEY}` }
});
const data = await response.json();
console.log(`AAPL: ${data.score} (${data.label})`);

// Pro tier full detail
const proResponse = await fetch(
  `${BASE_URL}/v1/sentiment/AAPL?detail=full`,
  { headers: { Authorization: `Bearer ${API_KEY}` } }
);
const proData = await proResponse.json();
console.log(proData.explanation);
```

**curl**

```bash
# Quick score check
curl -s -H "Authorization: Bearer sk-sm-your-key" \
  https://sentimentapi-p.up.railway.app/v1/sentiment/TSLA | python3 -m json.tool

# Full pro breakdown
curl -s -H "Authorization: Bearer sk-sm-your-key" \
  "https://sentimentapi-p.up.railway.app/v1/sentiment/NVDA?detail=full" | python3 -m json.tool

# Historical scores
curl -s -H "Authorization: Bearer sk-sm-your-key" \
  "https://sentimentapi-p.up.railway.app/v1/sentiment/MSFT/history?days=7" | python3 -m json.tool
```

---

## Supported Universe

502 US-listed equities covering the S&P 500. Full list available at:

```
GET /v1/tickers
```

Any ticker not in the supported universe returns a `ticker_not_found` response.

---

## Data Update Schedule

| Layer | Frequency | Coverage |
|---|---|---|
| Market data | Every 15 min (market hours) | Price, volume, RSI, order flow, bid-ask |
| News sentiment | Every 30 min | Alpha Vantage NEWS_SENTIMENT, Finnhub news |
| Analyst & insider | Every 6 hours | Finnhub insider transactions, recommendations, targets |
| Macro context | VIX + sector ETFs hourly (market hours); FRED yields daily at 02:00 UTC | VIX, sector ETF trends, Treasury yield curve |
| Short volume | Weekdays after close (21:30 UTC) | FINRA REGSHO daily short volume |
| Scoring tick | Every 15 min (market hours) / 30 min (off-hours) | Recomputes all four layers from stored data |

Scores are pre-computed and cached — API responses are served from the cache regardless of which data sources are involved.

---

*SentimentMarkets Sentiment API — built on FastAPI, PostgreSQL, and Redis. Deployed on Railway.*
