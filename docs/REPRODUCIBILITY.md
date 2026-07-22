# Reproducibility note for reviewers

**Updated:** 2026-07-22 (research-program build-out: eval harness, replay scorer, holdout discipline)
**Audience:** Academic reviewers auditing the implementation against the research paper "How to Quantify Stock Sentiment".

This repository implements a live multi-source sentiment scoring pipeline. The implementation is real software running in production on Railway; it is not a static snapshot. As a result there is a hard split between *what a reviewer can verify offline by cloning this repo* and *what they cannot reproduce without the production infrastructure*. This note tells you which is which, so you can audit the parts that are reproducible and trust the parts that aren't with appropriate skepticism.

---

## What you can reproduce locally

### The scoring math

Every formula the paper describes — z-score normalization, the per-signal weight `w_i = w_src · w_rel · w_conf · w_author · e^(−λΔt)`, FinBERT class-probability entropy weighting, sub-index aggregation (volume-weighted average + shrinkage for narrative/influencer; 6-component for market; paper-direct weighted average without shrinkage for macro), composite weighting, missing-layer redistribution, EMA smoothing, semantic dedup clustering — is implemented in pure Python with no live-infrastructure dependency. All of it is exercised by the unit-test suite.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest                  # ~50 unit-test files, 770+ tests, all green; ~25 s on a modern laptop
```

`conftest.py` auto-deselects tests marked `@pytest.mark.integration`. The deselected files (`integration_test_connections.py`, `integration_test_sources.py`) are the only places that need a live database, Redis, or external API; everything else runs purely from in-memory test fixtures. The result of a clean run is the canonical evidence that the scoring math behaves as the paper specifies.

To run integration tests explicitly (only if you have set up the infrastructure):

```bash
pytest -m integration
```

### The schema

The full PostgreSQL schema lives in the numbered migrations under `scripts/migrations/` (`000_initial_schema.sql` base; `005`–`012` additive). [`docs/DATA_DICTIONARY.md`](DATA_DICTIONARY.md) walks through every column and ties it to the methodology. You do not need to actually apply the schema to inspect it.

### The methodology document

[`METHODOLOGY.md`](../METHODOLOGY.md) at the repository root is the complete, code-accurate description of the scoring methodology — every constant, formula, weight, schedule, and threshold, with file references. It supersedes the retired per-channel audit files (`docs/audit_*` were local working documents and are no longer shipped).

### The code

All paper-mentioned modules are tracked in this repo:

- `pipeline/features/normalize.py` — the per-signal weight formula and the z-score normalizer.
- `pipeline/scoring/subindices.py` — the generic, market-specific, and macro-specific aggregators.
- `pipeline/scoring/composite.py` — the channel weights, missing-layer redistribution, and the exogenous (market-free) composite.
- `pipeline/scoring/ema.py` — the variable-timestep EMA (half-life 2 h since 2026-07-22 per experiment E004; env-tunable).
- `pipeline/nlp/finbert.py` — FinBERT inference, batch chunking, `inference_mode`.
- `pipeline/nlp/dedup.py` — semantic clustering (4-hour window, cosine > 0.85, `all-MiniLM-L6-v2`).
- `pipeline/sources/*.py` — per-channel ingesters (`market`, `narrative`, `influencer`, `macro`, `short_volume`, `fred`, `options` — the last is research-only).
- `pipeline/features/surprise.py`, `pipeline/features/positioning.py` — flag-gated research-only features (never served).

You can read these files in isolation. The README links each formula to the file where it lives.

### The research program (added 2026-07-22)

The predictive-research workflow is itself reproducible and disciplined:

- **Eval harness** — `scripts/eval/run.py` computes the calibration/IC/lead-lag/quintile scorecard from stored history (read-only) and gates releases against a committed baseline (`scripts/eval/baselines/`). The `--window research|holdout|all` flag enforces the frozen train/holdout split (`scripts/eval/HOLDOUT.md`).
- **Replay scorer** — `scripts/eval/replay.py` reconstructs counterfactual score series under candidate configs from stored sub-indices, validated by an identity check that reproduces stored history to rounding precision (`tests/test_replay.py` is the synthetic CI version).
- **Experiment ledger** — `scripts/eval/EXPERIMENTS.md`: one row per configuration evaluated, pre-registration committed before results, verdicts per pre-registered criteria. The row count is the multiple-comparisons denominator.

---

## What you cannot reproduce locally

### The live pipeline

The pipeline ingests live market data, news articles, insider filings, and macroeconomic series from five third-party APIs (Alpha Vantage, Finnhub, FRED, FINRA REGSHO file feed, yfinance), persists them to a PostgreSQL database, and serves cached results from Redis. None of those components are mocked or stubbed in a way that runs locally without keys. Specifically:

- **No SQLite or DuckDB fallback.** All queries are written against asyncpg with PostgreSQL-specific features (`ON CONFLICT`, `DISTINCT ON`, `JSONB`). `scripts/db/connection.py` reads `DATABASE_URL` and fails if unset.
- **No in-memory Redis.** `scripts/db/redis.py` reads `REDIS_URL`.
- **Score replay requires the production DB.** `scripts/eval/replay.py` can reconstruct counterfactual score series deterministically — but from *stored* per-tick sub-indices, so it needs read access to the production `sentiment_history`. Backfill scripts under `scripts/backfill/` seed historical raw data when first wiring up an environment, but they call live API endpoints and require keys.
- **No public anonymized data dump.** The production database is on Railway; we do not currently publish a redacted snapshot. Reviewers who need to verify outputs against a sample of real data should request a snapshot directly.

### Live scores

A reviewer cannot, by cloning this repo, produce a composite score for `AAPL` matching the score the live API would return at the same time. The composite is the output of a continuous process: scoring ticks (15-minute cadence during market hours, 30 off-hours) read from a `raw_signals` table whose history goes back to April 2024, apply rolling z-scores over hundreds of past observations per signal, and blend EMA-smoothed composites against the previous tick. None of that state is checked in; recreating it would require running the pipeline against the same upstream APIs for an extended period.

### The FinBERT model

`pipeline/nlp/finbert.py` lazy-loads `ProsusAI/finbert` (~440 MB) from Hugging Face on first call. Pulling the model requires network access. Once cached locally, FinBERT *can* be exercised by writing a small driver script — but no unit test depends on having the model available, because tests stub the scoring function.

---

## Required environment variables

A canonical `.env.example` lives at the repository root. Reviewers who want to actually run the live pipeline must populate at minimum:

| Variable | Why required | Where it's read |
|---|---|---|
| `DATABASE_URL` | Postgres pool | `scripts/db/connection.py` |
| `REDIS_URL` | Current-state cache + rate limiter | `scripts/db/redis.py` |
| `ALPHA_VANTAGE_KEY` | Narrative news + relevance scores | `pipeline/sources/narrative.py`, `pipeline/sources/macro.py`, `scripts/backfill/{ohlcv,etf}_backfill.py` |
| `FINNHUB_KEY` | Narrative fallback + insider + analyst | `pipeline/sources/{narrative,influencer,macro}.py` |
| `FRED_API_KEY` | Treasury yields + TED-substitute | `pipeline/sources/fred.py` |

Optional tuning / research flags (all read at process start): `EMA_HALF_LIFE_HOURS` (default 2.0 since the E004 change; the production value is set explicitly), `ENABLE_NARRATIVE_SURPRISE` and `ENABLE_POSITIONING_FEATURES` (research-only feature accumulation — off by default; when on, values land only in research columns, never in any served field), `LOG_LEVEL`/`LOG_FORMAT`.

`NEWSAPI_KEY` and `OPENAI_API_KEY` are listed in `.env.example` for completeness but are not read by any current code path (NewsAPI was excluded from the implemented methodology; the OpenAI-backed Pro-tier explanation is planned but not deployed — today both tiers use the template-based explainer).

For the API itself you do not need to set keys via environment variables: API tokens are minted by `tools/generate_keys.py`, stored as SHA-256 hashes in the `api_keys` table, and looked up by `api/auth.py` on every request. The plaintext is printed once at generation time and never recoverable from the database.

---

## Reading order

The reading order I would recommend for a paper reviewer:

1. **[`README.md`](../README.md)** — top-down system description with current weights and links to the modules that implement each formula.
2. **This file (`REPRODUCIBILITY.md`)** — what you can and cannot run.
3. **[`METHODOLOGY.md`](../METHODOLOGY.md)** — the complete code-accurate methodology: every constant, formula, weight, schedule, threshold, with file references. The single most complete document in the repo.
4. **[`docs/DATA_DICTIONARY.md`](DATA_DICTIONARY.md)** — every column in every table, tied back to the methodology.
5. **[`scripts/eval/EXPERIMENTS.md`](../scripts/eval/EXPERIMENTS.md)** + **[`scripts/eval/HOLDOUT.md`](../scripts/eval/HOLDOUT.md)** — the pre-registered experiment ledger and the frozen holdout discipline governing all predictive-research claims.
6. **[`CHANGELOG.md`](../CHANGELOG.md)** and **[`docs/CHANGES.md`](CHANGES.md)** — phase-by-phase history; the latter maps the 2026-07-21 nowcasting refactor to the external backtest study's findings.

(The historical per-channel audit files `docs/audit_*` cited by earlier revisions of this note were local working documents from the Phase-1–5 reconciliation cycles; they are superseded by `METHODOLOGY.md` and are no longer shipped. Their four-state vocabulary — RESOLVED / ACCEPTED / OPEN / REGRESSION — survives in the deviations list below: ACCEPTED items are places where the deployed system deliberately differs from the paper text, e.g. the TED-substitute discussion in `pipeline/sources/fred.py`.)

---

## Things the paper claims that the code does not implement

For transparency, these are the methodology elements the paper describes that have **not** landed in the deployed code yet, listed so a reviewer doesn't waste time hunting for them:

- **SEC EDGAR 8-K narrative source.** Sprint C drafted but never merged. Retracted in Phase 5 (2026-05-16) and moved to Future Additions. The source label `sec_edgar` has been removed from `_SOURCE_WEIGHTS`. The paper text needs to be reconciled in the next paper-edit cycle.
- **ApeWisdom retail-sentiment ingester.** Moved to Future Additions per the Sprint E paper-vs-code reconciliation (2026-05-12).
- **Role-based `w_author` hierarchy.** The paper explicitly defers this. Today `w_author = 1.00` uniformly. The scaffold is in place (`_INFLUENCER_W_AUTHOR` in `normalize.py`).
- **GPT-4o-mini Pro-tier explanations.** Planned. Today both tiers use the template-based explainer in `pipeline/explanation/templates.py`.
- **`backtest_results` forward-return population.** The table exists and is queryable; the asynchronous backfill that fills `forward_return_*` columns has not been built.

The audit files track several smaller gaps under "OPEN" and "ACCEPTED" — see audits A–D for the line-item inventory.
