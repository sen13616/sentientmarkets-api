# Changelog

## Phase 6

### Sprint P6.1 — Storage reclaim + compact driver encoding (2026-07-20)

Two-part storage overhaul of the production database (5,475 MB → ~3.0 GB expected). **Part A — reclaim:** deleted 3,452,964 historical natural-key duplicate rows from `raw_signals` (27% of the table — pre-guard insider/yfinance/ETF re-inserts; every row archived to `exports/raw_signals_dupes_20260720.csv.gz` first via new `scripts/tools/dedupe_raw_signals.py`); dropped three never-read indexes (`raw_signals_pkey`, `price_snapshots_pkey`, `idx_price_snapshots_ticker_ts` — ~450 MB; `id` columns retained, nothing queries by them); `VACUUM FULL` on both big tables; `price_snapshots` writes are now market-hours-only (`is_market_hours` guard in `pg_writer.persist_scored_state` — off-hours rows were 98% exact repeats of the prior close). TOAST compression on `sentiment_history` was investigated and proven impossible: rows (~1.2 KB) never reach PostgreSQL's compile-time ~2 KB toast trigger, which `toast_tuple_target` cannot lower. **Part B — compact driver encoding:** `sentiment_history.top_drivers` (981 of every 1,203-byte row; ~5 driver objects with repeated JSON keys) is now re-encoded for rows older than 30 days to fixed-order arrays `[signal, direction, magnitude, confidence, source_layer]` — `description` dropped after archiving originals to `exports/top_drivers_verbose_20260720.csv.gz` (descriptions embed raw signal values stored nowhere else, so the archive is the permanent record). New `pipeline/scoring/driver_codec.py` (compact/expand/is_compact), `compact_drivers_before()` in `scripts/db/queries/sentiment_history.py` (id-cursor batched, idempotent via `jsonb_typeof` guard), a fourth `retention_job` phase (`DRIVER_COMPACT_DAYS = 30`, daily 03:30 UTC), one-off `scripts/tools/compact_drivers_backfill.py`, and export normalization in `db_exports` (compact rows re-expand with `description: null`). API responses are unaffected — endpoints only serve the latest tick, which stays verbose. No `sentiment_history`/`price_snapshots` rows are ever deleted; scores are never modified.

## Phase 5

### Phase 5 — GitHub cleanup + Sprint C retraction (2026-05-16)

Reviewer-facing documentation and repository hygiene pass ahead of the SSRN submission. Group A retracted Sprint C (SEC EDGAR 8-K narrative source) — the draft on local-only branch `worktree-sprint-c-edgar-8k` was never merged to main, and the `sec_edgar` source-weight entry that the now-retired Form 4 path had also written has been removed from `_SOURCE_WEIGHTS`; tests updated to assert absence; 8-K filings moved to Future Additions. README rewritten end-to-end (1308 → 392 lines) with paper reference, current four-channel weights, per-channel deployed state, audit links, and unit-test quick-start. Group B added `.env.example`, `docs/DATA_DICTIONARY.md`, and `docs/REPRODUCIBILITY.md`; consolidated the per-signal weight formula into a single docstring section in `normalize.py`; strengthened paper citations on `_MACRO_SIGNAL_WEIGHTS` (subindices.py) and the T10Y2Y-TED-substitute decision (fred.py). Group C deleted 18 merged local branches and 8 merged remote branches; tagged `v3.0-phase3-influencer` (`abb6008`) and `v4.0-phase4-macro` (`ba580fc`); updated this CHANGELOG. 558/558 unit tests pass.

## Phase 4

### Sprint P4.4 — Paper-direct macro aggregator (2026-05-15)

Dedicated `compute_macro_sub_index` in `pipeline/scoring/subindices.py` replaces the generic `compute_sub_index` for the macro layer. Implements the per-signal weight table from paper §Macroeconomic Signals (`vix=1.0, sector_etf_return_20d=1.5, treasury_yield_10y=1.0, treasury_yield_2y=0.75, ted_spread=1.0`) with proportional re-normalization for missing signals. **No volume shrinkage** — paper rationale: macro signals are market-wide state variables available at every scoring tick, so the epistemic-caution motivation for shrinkage in event-driven layers does not apply. Audit D refreshed and clean.

### Sprint P4.3 — FRED Treasury / yield-curve signals (2026-05-15)

Three new macro signal types from FRED (St. Louis Fed): `treasury_yield_10y` (`DGS10`), `treasury_yield_2y` (`DGS2`), and `ted_spread` (`T10Y2Y` — 10y−2y yield-curve slope, used as the substitute for the discontinued LIBOR-based TED spread). All three stored under `ticker='_MACRO_'` matching the VIX convention, sign-inverted (rising yields = bearish for equities). Hourly cadence via new `fred_job`. Rolling z-score with `window=90` per signal; parametric cold-start fallbacks for the first ~90 trading days. Paper text reconciliation of the TED-substitute is queued for the next paper-edit cycle. New env var: `FRED_API_KEY`.

### Sprint P4.2 — Per-ticker macro via GICS sector routing (2026-05-15)

The macro layer was previously global (one sub-index applied to every ticker). P4.2 makes it per-ticker by routing each ticker to its sector ETF (XLK / XLV / XLE / …) via `pipeline.sources.macro.SECTOR_ETFS` keyed on `ticker_universe.sector` (Option C from the sprint plan). The macro aggregator now reads `sector_etf_return_20d` from the row stored under the ticker's sector ETF symbol. Shrinkage denominator temporarily set to 2 for macro pending the P4.4 paper-direct aggregator that drops shrinkage entirely.

### Sprint P4.1 — GICS sector column on ticker_universe (2026-05-15)

Schema migration 008 adds `ticker_universe.sector VARCHAR(50)` (idempotent via `IF NOT EXISTS`). Seeded via `tools/seed_sectors.py` from `tools/sector_map.py` (502 S&P 500 tickers, 2026-05-15 point-in-time snapshot). 19 renamed / acquired / delisted tickers have their sector backfilled from a hand-curated `_FALLBACK_SECTORS` table in `tools/generate_sector_map.py`. API schemas (`api/response/schemas.py`), the `/v1/tickers` route, and test fixtures updated to surface the new column.

## Phase 3

### Sprint P3.4 — Remove EDGAR Form 4 primary path (2026-05-14)

Paper-vs-code reconciliation on insider data source. Paper's Data Collection table names Finnhub as the insider provider; the EDGAR Form 4 primary path in `pipeline/sources/influencer.py` produced 0 rows in production (SGML wrapper breaks `xml.etree.ElementTree.fromstring`); all 254 096 insider rows came from the Finnhub fallback. Deleted ~152 lines: `_load_cik_map`, `_get_cik`, `_fetch_form4_transactions`, EDGAR URL constants, `_EDGAR_HEADERS`, `_cik_cache`, `_cik_lock`, the `xml.etree.ElementTree` import. Promoted `_insider_finnhub` to the only insider path. Deleted the `("influencer", "sec_edgar")` half-life override; 168h insider half-life now applied via the signal-channel-keyed `_INFLUENCER_SIGNAL_HALF_LIFE_H["insider_net_shares"]` (P3.1 I4). Retained `_SOURCE_WEIGHTS["sec_edgar"] = 1.0` and the `EDGAR_SEM` / `EDGAR_DELAY` rate-limit primitives pending Sprint C resolution; the weight entry was finally retracted in Phase 5 after Sprint C was confirmed unmerged.

### Sprint P3.3 — Earnings-estimate revisions signal (2026-05-14)

Fourth influencer signal added per paper §Influencer Based Signals. yfinance fetcher writes `analyst_eps_estimate_mean`; normalizer derives `earnings_estimate_revision` as the period-over-period relative delta; `RollingZScorer(window=90)` is the primary path with `_score_earnings_revision_delta(delta) = clamp(50 + 50·tanh(delta/0.05), 0, 100)` as the parametric cold-start. The cold-start formula is not specified in paper Appendix 1.4 — flagged for paper update during the Phase-5 reconciliation cycle.

### FinBERT memory fix (2026-05-13)

Out-of-memory crash on the production scoring tick traced to per-batch padding inflating peak transient activations. Fix in `pipeline/nlp/finbert.py`: process articles in fixed-size chunks (`batch_size=32`); padding is now per-chunk via `tokenizer(..., padding=True)` so a single long article only pads its own 32-row chunk rather than the entire input. Switched from `torch.no_grad()` to `torch.inference_mode()` (also disables view tracking; ~10–15% faster). Per-tick article limit lowered to 500 to give headroom. No methodology change.

### Sprint P3.2 — analyst_target_price via yfinance + z-score (2026-05-13)

`analyst_target_price` (mean analyst 12-month target) re-enabled via yfinance after the Finnhub free-tier 403 made it unreachable. Normalizer computes `(target − current_price) / current_price` upside and z-scores per-ticker via `RollingZScorer(window=90, fill_threshold=0.5)`. Influencer signal-channel weight 0.85 (from P3.1).

### Sprint P3.1 — Influencer signal-channel weights + half-lives (2026-05-13)

Per paper §Event-Level Weighting, the influencer channel keys weights by signal channel (insider / analyst-consensus / target-price / earnings-revisions), not by data provider. Adds `_INFLUENCER_SIGNAL_WEIGHT` and `_INFLUENCER_SIGNAL_HALF_LIFE_H` override tables in `pipeline/features/normalize.py`. New values: `insider_net_shares` w=1.00/hl=168h, `analyst_buy_pct` w=0.85/hl=72h, `analyst_target_price` w=0.85/hl=72h, `earnings_estimate_revision` w=0.80/hl=72h. `w_author` and `w_conf` scaffolds at 1.00 (paper: role-based hierarchy deferred to Future Additions; non-textual signals do not use confidence weighting).

## Phase 2

### Sprint Logging — Severity re-leveling + noise suppression (2026-05-12)

Sprint Logging: severity re-leveling, httpx noise suppression, silent exception logging, LOG_LEVEL env var support. ~26 statements across 7 files. Cuts production log volume ~60% during normal operation.

### Sprint A — FinBERT integration + w_conf + source weight reconciliation (2026-05-11)

Sprint A: All narrative articles now scored by ProsusAI/finbert (paper Stage 3). AV's provider_sentiment retired from scoring path. Finnhub articles now contribute to narrative sub-index (~18k/week unlocked). Weight formula updated to paper spec: w_i = w_src * w_rel * w_conf * e^(-lambda * dt). Source weights reconciled: AV 0.70→0.75, Finnhub 0.80→0.65. Language detection (langdetect) at ingestion filters non-English articles. Schema migration 007 adds finbert_pos/neg/neu + language columns.

### Sprint D — Relevance threshold tightening (2026-05-11)

Sprint D: Narrative relevance threshold tightened from 0.10 to 0.60 per paper Stage 2. Articles without explicit relevance_score now excluded rather than defaulted. ~16% reduction in AV article volume per coverage analysis 2026-05-11.

### Sprint C — SEC EDGAR 8-K narrative source (drafted, NOT merged)

Drafted on local-only branch `worktree-sprint-c-edgar-8k` (commit `891b3c0`, 2026-05-12) but never merged to main. Phase 5 investigation confirmed the branch is dormant; 8-K filings moved to Future Additions and the branch deleted. Listed here for historical traceability — anyone reading older sprint plans or the masterchecklist will see references to Sprint C and should understand they refer to retracted work.

## Phase 1

### Sprint 7 — Phase 1 methodology compliance gate (2026-05-10)
### Sprint 6 — Semantic deduplication wired into narrative path (2026-05-10)
### Sprint 5a — EMA smoothing on composite score (2026-05-09)
### Sprint 4 — Z-score normalization + per-source half-lives (2026-05-08)
### Sprint 3 — Global scoring tick (2026-05-07)
### Sprint 2 — Volume ratio in market sub-index (2026-05-07)
### Sprint 1 — Math correctness fixes (2026-05-07)
