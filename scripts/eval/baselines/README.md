# Eval harness — the release gate for scoring changes

## Guiding statement

> **Nowcast first: every published number is an honest, calibrated, responsive
> description of current cross-sectional sentiment. Anything that claims to
> predict must first beat the scorecard.**

Every scoring change must pass this gate before release. The harness reads the
**served outputs** (`sentiment_history`, `raw_signals` closes) directly from
the DB — it measures what was served, not what the code intends — and never
imports `pipeline/` scoring code.

Ported from the original backtest study (`docs/SUMMARYOFTESTING.md`); the
ported engine reproduces the study's headline numbers (composite std 8.3 vs
8.2, lead-lag peak at offset 0 with corr +0.40 vs +0.43, best IC `dexo_3`
≈ 0.036/t 4.3 vs 0.033/t 4.2).

## Running the gate

```bash
# full run + gate against the committed baseline
python3 -m scripts.eval.run \
    --start 2026-04-24 --end <today> \
    --out exports/eval \
    --baseline scripts/eval/baselines/BASELINE_2026-07-21.json

# optional intraday lead-lag (heavier: pulls every scoring tick)
#   --intraday [--prices yfinance]
# extra data-gap windows (repeatable; the 2026 outage is excised by default)
#   --gap 2026-06-23:2026-07-03
```

Exit code 0 = gate passed; 1 = regression vs baseline; 2 = no data.
Outputs land in `--out`: `scorecard.json`, `scorecard.md`, plus
`panel.csv`, `ic_table.csv`, `quintile_ls.csv`, `leadlag_*.csv`.

## What the scorecard measures

| section | metric | why |
|---|---|---|
| calibration | per-layer & composite mean, `abs_dev_from_50` | neutral must mean 50 |
| dispersion | pooled std + `xs_dispersion` (mean per-day cross-sectional std) | averaging must not crush the tails |
| leadlag | peak offset + curve for raw Δ and exogenous Δ | mirror (peak ≤ 0) vs headlight (peak > 0) |
| predictive | daily cross-sectional Spearman IC × horizon, quintile L/S | forward information, market-neutral |
| quintile_ls | gross AND net L/S spreads (E001 cost overlay: round-trip bps × leg turnover per rebalance, default 15) | does the spread survive trading costs |
| information_latency | `created_at − published_at` per source | the predictive budget (Phase 5a) |

**Reading the IC table** (E001 clarification): `mean_IC`, `IC_t`, and `hit_rate`
all describe the **daily IC series**. `hit_rate` = fraction of DAYS whose
cross-sectional IC was positive — NOT per-trade accuracy; it does not translate
to a win rate on positions. A hit rate of 0.7 with IC ≈ 0.03 means "most days
the ranking tilts the right way, faintly", not "70% of trades win".

## Gate rules (`scorecard.DEFAULT_RULES`)

A change regresses when it moves a metric the wrong way beyond tolerance:

- `composite_raw.std` / `xs_dispersion` must not shrink > 10% (relative)
- each layer's `abs_dev_from_50` must not grow > 1.0 (absolute)
- lead-lag `peak_offset` (raw and exo) must never move further into the past

Metrics missing from either card are skipped — new metrics have no baseline
and that's fine.

## Updating the baseline

Only after an intentional, reviewed change to scoring semantics: run the
harness, review the diff, commit the new `BASELINE_<date>.json` (in `scripts/eval/baselines/`) alongside the
change, and keep the old baseline file for the record.
