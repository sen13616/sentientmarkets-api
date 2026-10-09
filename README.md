# SentientMarkets Sentiment API

[![CI](https://github.com/sen13616/sentientmarkets-api/actions/workflows/ci.yml/badge.svg)](https://github.com/sen13616/sentientmarkets-api/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A four-channel sentiment scoring engine for US equities. A background pipeline ingests
**market**, **narrative** (news), **influencer** (insider + analyst) and **macro** signals,
normalizes them, and serves a 0–100 composite score per ticker through a read-only FastAPI
service.

This repository is the reference implementation for the research paper **"How to Quantify
Stock Sentiment"**. Every formula, weight and threshold the paper describes is in this code.
[`METHODOLOGY.md`](METHODOLOGY.md) maps them line by line.

**Status (October 2026):** in production, scoring **586 tickers**: every current S&P 500
member plus still-trading former names. Scores are recomputed every 15 minutes during US
market hours and every 30 minutes otherwise. The research record runs continuously from
2026-05-21. Outage periods were repaired offline and are tagged as such
([METHODOLOGY §16.5](METHODOLOGY.md)).

---

## Contents

- [Using the API](#using-the-api)
- [How the score works](#how-the-score-works)
- [Architecture](#architecture)
- [Development](#development)
- [Research and strategy testing](#research-and-strategy-testing)
- [Repository layout](#repository-layout)
- [Deployment](#deployment)
- [Documentation map](#documentation-map)

---

## Using the API

Access is invite-only. Requests carry a bearer token:

```bash
curl -H "Authorization: Bearer $SENTIMENT_API_KEY" \
  https://sentimentapi-p.up.railway.app/v1/sentiment/AAPL
```

```json
{
  "ticker": "AAPL",
  "score": 54,
  "score_raw": 55,
  "label": "Neutral",
  "confidence": 100,
  "timestamp": "2026-10-03T02:00:00Z"
}
```

| Endpoint | Tier | Returns |
|---|---|---|
| `GET /v1/sentiment/{ticker}` | free / pro | Latest score. Pro with `?detail=full` adds sub-indices, drivers, explanation and cross-sectional percentiles |
| `GET /v1/sentiment/{ticker}/history` | pro | Score history (`days`, `interval=daily\|hourly\|raw`) |
| `GET /v1/tickers` | free / pro | Active universe with company name, GICS sector and `in_sp500` |
| `GET /v1/market/overview` | pro | Universe statistics, movers, sector breakdown |
| `GET /v1/status` | free / pro | Pipeline job freshness |
| `GET /health`, `GET /health/pipeline` | public | Liveness; pipeline health — every job on schedule, fresh scores (`ok` 200 / `degraded`·`down` 503; 30 req/min per IP) |

Retired symbols (acquired, merged, renamed) return `status: "delisted"` with their last
trading day and successor; their history stays available. Rate limits: free 10 req/min,
pro 600 req/min. The full reference with field definitions is in [`docs/api/README.md`](docs/api/README.md).

---

## How the score works

```
composite = 0.35·market + 0.30·narrative + 0.25·influencer + 0.10·macro
```

1. **Signals** (prices, RSI, order flow, short volume; FinBERT-scored news; insider trades and
   analyst revisions; VIX, Treasury yields, sector ETFs) are normalized to 0–100 with rolling
   z-scores (parametric fallbacks while history accrues) and weighted by source credibility,
   relevance, model confidence and exponential time decay.
2. Each **layer** aggregates its signals into a sub-index. A missing layer's weight is
   redistributed across the layers that are present.
3. The **composite** is capped under extreme layer divergence and smoothed with a 2-hour-half-life
   EMA. `score` is the smoothed value; `score_raw` is the unsmoothed one.
4. **Confidence** starts at 100 and is reduced for missing layers, stale sources, low signal
   volume and high divergence.

The complete, code-accurate specification is [`METHODOLOGY.md`](METHODOLOGY.md).

---

## Architecture

```
External APIs ──► Ingestion jobs (data only) ──► PostgreSQL (raw_signals, raw_articles)
                                                        │
                        scoring_tick_job (every 15/30 min, all tickers)
                                                        │
                              ┌─────────────────────────┴───────────┐
                              ▼                                     ▼
                       Redis (current state)            PostgreSQL (sentiment_history)
                              │
                     FastAPI read-only service (/v1/*)
```

- **Pipeline** (`pipeline/`): APScheduler jobs. Ingestion and scoring are decoupled: only
  `scoring_tick_job` computes scores, and nothing is scored in response to a request. Every job
  has a hard timeout, and every DB query has a command timeout, so a stuck call can't stall the
  pipeline.
- **API** (`api/`): authenticated reads from Redis; per-key rate limiting via an atomic Redis script.
- **Storage**: schema in numbered migrations ([`scripts/migrations/`](scripts/migrations/)), documented
  column by column in [`docs/DATA_DICTIONARY.md`](docs/DATA_DICTIONARY.md). `sentiment_history`
  is never purged; raw signals follow a tiered retention policy (METHODOLOGY §14).

---

## Development

Requires Python 3.12.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt

pytest                      # unit tests; no database, Redis or API keys needed
ruff check . && ruff format --check .
```

Integration tests (`@pytest.mark.integration`) are deselected by default and need live
infrastructure: `pytest -m integration`.

Running the full pipeline locally needs PostgreSQL, Redis and provider API keys. Copy
[`.env.example`](.env.example) to `.env` and fill it in. Then:

```bash
for f in scripts/migrations/0*.sql; do psql "$DATABASE_URL" < "$f"; done
python3 scripts/tools/update_universe.py       # seed the ticker universe
python3 -m uvicorn main:app --reload
```

Changes that affect served scores must pass the evaluation gate before release; see
[`CONTRIBUTING.md`](CONTRIBUTING.md).

---

## Research and strategy testing

- **Evaluation harness** ([`scripts/eval/`](scripts/eval/)): the release gate, which compares a
  scorecard of served scores (information coefficients, quintile spreads, lead-lag) against a
  committed baseline. It also includes a pre-registered experiment ledger and a frozen holdout.
- **Strategy toolkit** ([`research/`](research/README.md)): export a point-in-time snapshot,
  write a strategy against a small interface, and backtest it with next-close execution, costs and
  delisting handling. No look-ahead, and no production DB access needed.

```bash
python -m research.snapshot --start 2026-05-21 --end 2026-10-06
python -m research.run --snapshot data/snapshots/2026-10-06 \
    --strategy research.examples.quintile_ls:QuintileLongShort --cost-bps 15
```

---

## Repository layout

```
api/                 FastAPI service: auth, rate limiting, routes, response assembly
pipeline/            Scoring engine: scheduler, sources, nlp, features, scoring,
                     confidence, explanation, persistence
research/            Strategy-testing toolkit (snapshots, Strategy interface, backtester)
scripts/
  db/                asyncpg pool, Redis client, per-table query modules
  migrations/        Numbered SQL schema migrations
  eval/              Evaluation harness, experiment ledger, baselines
  backfill/          Historical loaders and offline repair/replay tools
  tools/             Operations tools (keys, universe, DB viewer); oneoff/ = finished migrations
docs/                API reference, data dictionary, reproducibility notes, history
tests/               Unit tests (integration tests marked and deselected by default)
main.py              App entrypoint (DB pool → Redis → scheduler)
```

---

## Deployment

Production runs on [Railway](https://railway.app/) (`railway.toml`, nixpacks) with managed
PostgreSQL and Redis. `main` deploys automatically. **Apply any new migration before merging
code that depends on it.** The deploy health check is `/health`; monitor `/health/pipeline` for job schedules and
score freshness.

---

## Documentation map

| Document | For |
|---|---|
| [`METHODOLOGY.md`](METHODOLOGY.md) | The complete scoring specification, plus a dated log of data-quality events and method changes (§16.5) |
| [`docs/api/README.md`](docs/api/README.md) | API reference |
| [`docs/DATA_DICTIONARY.md`](docs/DATA_DICTIONARY.md) | Every table and column, and how to filter for research |
| [`docs/REPRODUCIBILITY.md`](docs/REPRODUCIBILITY.md) | What a reviewer can reproduce, and how |
| [`research/README.md`](research/README.md) | Strategy testing |
| [`scripts/eval/EXPERIMENTS.md`](scripts/eval/EXPERIMENTS.md), [`HOLDOUT.md`](scripts/eval/HOLDOUT.md) | Research discipline |
| [`CHANGELOG.md`](CHANGELOG.md) | Change history |
| [`CONTRIBUTING.md`](CONTRIBUTING.md), [`SECURITY.md`](SECURITY.md) | Working on the code; reporting vulnerabilities |

## License

[MIT](LICENSE) © 2026 Aayudh Sen
