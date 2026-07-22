# CHANGES — Nowcasting-First Refactor (2026-07-21)

Everything here responds to the predictive-value analysis in
`docs/SUMMARYOFTESTING.md`, which tested whether the API's scores predict
forward returns. Its verdict: the scores are **coincident-to-lagging with
price** (a mirror, not a headlight) but have real value as a
**descriptive / nowcasting** product. The user-approved direction:
nowcasting-first, with predictive groundwork laid now, rolled out
**additively** — no existing field changed meaning, and the headline `score`
is byte-identical before and after.

## Guiding statement

> **Nowcast first: every published number is an honest, calibrated,
> responsive description of current cross-sectional sentiment. Anything that
> claims to predict must first beat the scorecard.**

Every change below was built against that acceptance criterion; the whole
refactor shipped as 7 commits (`50a98aa..e19da81`), 706 tests passing
(up from 648), migrations 009–011 applied to Railway production.

---

## Phase 0 — Eval harness: the release gate
*(addresses report §3.1 — "Put a forward-return eval in your release loop
(highest leverage)")*

The report's highest-leverage suggestion was to gate every scoring change on
measured forward-return evidence. The external backtest engines were ported
into the repo as `scripts/eval/`, reading **served outputs** directly from
the DB (`sentiment_history` + `raw_signals` closes) — no API round-trips, no
rate-limit contention, deterministic and offline.

```bash
python3 -m scripts.eval.run --start 2026-04-24 --end <today> \
    --out exports/eval --baseline scripts/eval/baselines/BASELINE_2026-07-21.json
```

- Measures: per-layer calibration (mean vs 50), dispersion (std +
  cross-sectional dispersion), daily cross-sectional Spearman IC by
  feature × horizon, quintile long-short spreads, and the **lead-lag peak
  location** (the mirror-vs-headlight diagnostic).
- Exits non-zero on regression vs the committed baseline
  (`scripts/eval/baselines/BASELINE_2026-07-21.json`); tolerances documented
  in `scripts/eval/baselines/README.md`.
- The ported engine **reproduces the study**: composite std 8.3 (report:
  8.2), lead-lag peak at offset 0 with corr +0.40 (report: +0.43), best IC
  `dexo_3` ≈ 0.036 / t 4.3 (report: 0.033 / t 4.2).
- Also found & fixed during validation: DB close rows carry the yfinance bar
  date at midnight-UTC, so trade dates must be read as UTC dates (an ET
  conversion mislabels every close a day early).

Files: `scripts/eval/{data,analyze,intraday,scorecard,run}.py`,
`scripts/eval/baselines/`, `tests/test_eval_harness.py`.

## Phase 1 — Per-consumer API keys
*(addresses report §4 — "Issue separate keys per consumer")*

The report's backtest data pulls contended with the live website on a shared
pro key. Now each consumer gets its own key and rate-limit bucket:

- Migration **009**: `api_keys.label` column (applied).
- `scripts/tools/generate_keys.py` rewritten:
  `create --tier {free,pro} --label L [--owner E]` / `list` / `revoke --id N`.
  Plaintext printed once; hash-only storage unchanged; auth path untouched.

## Phase 2 — Cross-sectional calibration outputs
*(addresses §2.5 + §3.6 — "every layer averages 56–59, neutral-50 is
miscalibrated; zero-center within universe and sector")*

Because the universe sits structurally bullish, absolute levels mislead —
relative position is the honest reading. At the end of every scoring tick the
raw (divergence-capped, unsmoothed) scores are standardized cross-sectionally
and served on the **pro** response:

- `score_raw_z` — (raw − μ)/σ over the tick's universe, unclamped.
- `score_raw_percentile` — percentile within the universe.
- `sector_percentile` — percentile within the ticker's GICS sector (null if
  sector unknown or < 3 scored members).

Computed on the **raw** score because the report showed the smoothed score is
the laggiest view. Published to Redis `pipeline:universe_xs` per tick;
`null` = "not in the latest tick", never zero.
Files: `pipeline/scoring/market_overview.py` (`compute_cross_sectional`),
`pipeline/scheduler.py`, `api/response/{schemas,assembler}.py`.

## Phase 3 — Exogenous sentiment-only composite: `score_exo`
*(addresses §2.2 + §3.3 — "your heaviest-weighted layer (market, 0.35) is
price in disguise; split it out of the sentiment composite")*

The market sub-index correlates +0.54 with same-day returns because it *is*
trailing price — it inflated the "sentiment tracks returns" story while
adding no information. The composite the report actually wanted to test is
now a first-class output:

- `compute_exo_composite()` (`pipeline/scoring/composite.py`):
  narrative/influencer/macro with market excluded, weights renormalized to
  ≈ 0.46/0.38/0.15.
- **Raw only** — no EMA (smoothing is what made the headline score the worst
  predictor) and no divergence cap (a market-vs-composite construct).
- Returns `null` when all three exo layers are missing — never a fabricated
  neutral 50.
- Persisted to `sentiment_history.composite_score_exo` (migration **010**,
  applied); served as `score_exo` + `score_exo_percentile` (pro) and
  `score_exo` on history entries.
- The `market` sub-index is re-documented in `INTEGRATION.md` as a
  **technical/price overlay**, not exogenous sentiment.

The headline composite still includes market at 0.35 — removing it is a
future breaking change requiring frontend coordination.

## Phase 4 — Smoothing repositioned as a display choice
*(addresses §2.4 + §3.7 — "the smoothed score is the worst predictor; expose
score_raw as primary, smooth for display only")*

- `score_raw` is now served to **both tiers** (was pro-only). `score` is
  re-documented as the display-stable smoothed view — "lags by design, not a
  timing signal". The recommended nowcast reading is `score_raw` +
  `score_raw_percentile`.
- EMA half-life is env-tunable (`EMA_HALF_LIFE_HOURS`), **default 4.0
  unchanged**. The default only changes if the offline experiment
  (`scripts/eval/experiments/ema_halflife.py` — re-smooths history under
  candidate half-lives and measures lag/IC) wins on the scorecard.
- `score_exo` is never smoothed, by design.

## Phase 5 — Predictive groundwork (gated, off by default)
*(addresses §3.2 latency + §3.4 narrative surprise — the only paths that
could make the signal lead)*

**5a — Information-time stamping.** Verified the pipeline already stamps
narrative signals with provider publication time; guard tests
(`tests/test_narrative_info_time.py`) now lock that in: unparseable
timestamps skip the article (never default to now), and `narrative_as_of` =
max `published_at`. The scorecard's new `information_latency` section
measures the predictive budget — currently **median 8.4 h (Alpha Vantage)
and 2.7 h (Finnhub)** from publication to ingestion, which quantifies
exactly why the narrative layer can't lead yet.

**5b — Narrative surprise.** The report's diagnosis: narrative moves
~34×/name/day but its *level* carries no forward signal, because persistent
coverage is already priced. `pipeline/features/surprise.py` scores the
**deviation** of the last-24h relevance-weighted FinBERT tone from the
ticker's own trailing 14-day baseline (z-scored; null on thin coverage or
cold start). It is:

- **Off by default** — computed only when `ENABLE_NARRATIVE_SURPRISE=1`;
- research-only — written to `sentiment_history.narrative_surprise`
  (migration **011**, applied) and the Redis state, never the API or the
  composite;
- promotion-gated — it becomes a served field only if, after ≥ 3–4 weeks of
  accumulation, it beats `narrative_index` forward IC on the eval harness.

---

## What deliberately did NOT change

| Not changed | Why |
| --- | --- |
| Headline `score` (values, smoothing, weights) | Additive rollout — live frontends consume it; a flip needs a coordinated release |
| `LAYER_WEIGHTS` (0.35/0.30/0.25/0.10) | Learning weights on ~3 months of one regime is an overfitting machine (report §5); revisit with 6–12 months + walk-forward validation |
| EMA default half-life | Gated on the `ema_halflife.py` experiment winning on the scorecard |
| Market layer inputs (options flow, short-interest changes, etc.) | Requires paid data sources — a separate decision |
| Event-driven scoring (score on news arrival) | Real architectural change; only worth it if 5a/5b show latency is the binding constraint |

## API surface added (all additive, all nullable)

| Field | Tier | Where |
| --- | --- | --- |
| `score_raw` | free + pro (was pro) | sentiment |
| `score_raw_z`, `score_raw_percentile`, `sector_percentile` | pro | sentiment |
| `score_exo`, `score_exo_percentile` | pro | sentiment |
| `score_exo` | pro | history entries |

`INTEGRATION.md` (the consumer contract, untracked) has changelog entries
for each.

## Operational follow-ups

1. Deploy to Railway (migrations already applied — safe in any order).
2. Mint per-consumer keys and swap the website off the shared key:
   `python3 scripts/tools/generate_keys.py create --tier pro --label website`.
3. Optionally set `ENABLE_NARRATIVE_SURPRISE=1` to start the surprise
   accumulation clock.
4. Re-run the eval gate before any future scoring change; re-baseline only on
   intentional, reviewed semantic changes.
