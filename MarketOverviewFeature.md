# Market Overview — Market Mood Card

Reference for building a **market mood card** on the SentientMarkets website. It
describes the data source, the exact payload, and a suggested card layout. The
feature is **Pro tier** and updates every scoring tick.

---

## What it is

A single, universe-wide snapshot of S&P 500 sentiment for the latest scoring
tick — one average score, market breadth, the day's biggest movers, a per-sector
breakdown, and a ready-to-display plain-English `summary`. It's the market-level
analog of a single ticker's sentiment: instead of "how does AAPL feel," it
answers "how does the whole market feel right now."

The card's headline can be driven entirely by two fields — `summary` (the
narrative) and `average_score` (the number/gauge) — with the rest available for a
richer expanded view.

---

## Endpoint

```
GET /v1/market/overview
Authorization: Bearer <pro-key>
```

- **Tier:** Pro only. Free keys receive **403** (`{"error": "forbidden"}`).
- **Freshness:** Served from a per-tick cached blob; the `timestamp` field is the
  scoring tick it was computed at. Ticks run every 15 min during market hours and
  every 30 min off-hours, **24/7** (so the card is live on weekends too, built on
  the most recent market data — see *Weekend behavior* below).
- **Cold start:** Before the first tick after a deployment, returns **503**
  (`{"error": "temporarily_unavailable"}`). The card should show a "warming up"
  state on 503.
- **Cost:** One Redis read, no database work — cheap to poll. Refresh the card on
  the tick cadence (e.g. every 5 min client-side is plenty).

---

## Response payload

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

### Field reference

| Field | Type | Meaning / display use |
|---|---|---|
| `timestamp` | ISO-8601 | Tick the blob was computed at. Show as "as of HH:MM UTC". |
| `universe_scored` | int | Tickers scored this tick (≈502). Use for "N of 502 names". |
| `average_score` | float \| null | **Primary gauge value**, 0–100. Drives the mood color/needle. |
| `breadth_above_50_pct` | float \| null | % of names above neutral (bullish participation). Good as a secondary bar. |
| `breadth_improving_pct` | float \| null | % of names improving vs their 1-day baseline. `null` right after a data gap. |
| `summary` | string | **Headline narrative** — render verbatim as the card's caption. `""` only in the rare pre-first-tick blob. |
| `top_movers` / `bottom_movers` | Mover[] | Up to 10 each, largest 1-day gains / losses. Use the first 3–5 as chips. |
| `sectors[]` | Sector[] | Per-sector average, size, and each member's within-sector rank. Powers a sector heatmap / expandable list. |

**Mover** = `{ ticker, score, score_change_1d, score_change_1d_pct }`.
**Sector** = `{ sector, average_score, size, tickers: [{ ticker, score, rank }] }`
(`rank` 1 = highest in the sector; combine with `size` for "#4 of 68").

---

## The `summary` field

`summary` is a **deterministic, template-generated** sentence or three — no LLM,
no per-request cost, stable across renders for the same tick. It only ever
references figures already present in the blob (never invents numbers). Shape:

1. **Mood + breadth** — `"Market sentiment is <mood> (avg NN, NN% of names above neutral[ and improving | but weakening])."`
2. **Sectors** — `"<leader(s)> lead; <laggard> lags."`
3. **Movers** — `"Biggest movers: TICK (+x.x), TICK (-y.y)."`

Mood words map to `average_score` (bands are finer near 50 because a universe
*average* compresses toward the middle):

| avg score | mood word |
|---|---|
| ≥ 65 | strongly bullish |
| 58–65 | bullish |
| 53–58 | mildly bullish |
| 47–53 | neutral |
| 42–47 | mildly bearish |
| 35–42 | bearish |
| < 35 | strongly bearish |

Use the same bands to pick the card's accent color if you want the color and the
words to agree.

---

## Suggested card layout

```
┌─────────────────────────────────────────────┐
│  MARKET MOOD                     as of 14:30 │
│                                              │
│        ◑  55   Mildly Bullish                │  ← average_score + mood
│        ▓▓▓▓▓▓▓░░░  64% above neutral          │  ← breadth_above_50_pct
│                                              │
│  "Market sentiment is mildly bullish…        │  ← summary (verbatim)
│   Information Technology and Financials       │
│   lead; Energy lags."                         │
│                                              │
│  ▲ AAPL +3.2   NVDA +2.9   ...                │  ← top_movers chips
│  ▼ MSFT -1.5   XOM  -2.1   ...                │  ← bottom_movers chips
│                                              │
│  [ Sectors ▾ ]                                │  ← expand → sectors[]
└─────────────────────────────────────────────┘
```

Rendering notes:
- **Gauge / needle:** `average_score` on a 0–100 scale; color by the mood band.
- **Caption:** drop `summary` in verbatim — it's written to be display-ready.
- **Mover chips:** green for positive `score_change_1d`, red for negative; label
  with `ticker` and the signed change. `score_change_1d_pct` is available if you
  prefer a percentage.
- **Sector view (expanded):** sort `sectors` by `average_score` for a leaders→
  laggards list or a heatmap; each row can drill into member tickers by `rank`.

---

## Edge cases the card must handle

| Situation | Payload signal | Card behavior |
|---|---|---|
| Cold start (no tick yet) | HTTP **503** | "Warming up — check back shortly." |
| Free-tier key | HTTP **403** | Upsell / hide the card. |
| First tick after a data gap | `breadth_improving_pct: null`, empty `top_movers`/`bottom_movers` | Hide the movers row and the improving/weakening clause; level stats (avg, breadth-above-50) still valid. |
| Empty tick (no names scored) | `average_score: null`, `summary: ""` | Show a neutral placeholder; don't render a gauge. |

### Weekend / after-hours

Scoring runs 24/7, so the card is always populated. **Between Friday close and
Monday open the market layer is frozen at Friday's close**, so scores drift toward
neutral and the mood may read flatter — this is expected, not a fault. The
`timestamp` still advances each tick; consider a subtle "market closed" hint when
appropriate so users don't read a flat weekend reading as stale data.

---

## Backend references (for maintainers)

- Blob built each tick in `pipeline/scoring/market_overview.py` (`build_overview`).
- Narrative in `pipeline/scoring/market_summary.py` (`build_summary`).
- Cached to Redis by `pipeline/scheduler.py` (`_publish_universe_stats`).
- Served by `api/routes/market.py`; schema `MarketOverviewResponse` in
  `api/response/schemas.py`.
- Full API reference: `docs/api/README.md` → *GET /v1/market/overview*.
