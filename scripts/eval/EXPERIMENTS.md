# Experiment Log

One row per configuration evaluated on the research window — including failures,
dead ends, and re-runs of variants. The row count IS the multiple-comparisons
denominator: when judging whether a candidate's edge is real, N is the number of rows
in this table, not the number of ideas you remember trying.

Rules
-----
- Log the row **when the experiment runs**, not when it succeeds.
- All rows use `--window research` (the default). Holdout confirmations are logged in
  `HOLDOUT.md`, not here — a candidate only goes there after winning here.
- `Key result` = the headline scorecard numbers that motivated the verdict (e.g.
  lead-lag peak offset & value, IC at h=1/3/5, quintile L/S). Full scorecards live in
  `exports/eval/` (gitignored) — record the output dir or commit the scorecard JSON if
  it matters.
- Every scoring change that ships must still pass the release gate vs the committed
  baseline (`scripts/eval/baselines/`).

Status legend: `baseline` · `rejected` · `promising` (iterate further) ·
`won-research` (eligible for one holdout evaluation) · `confirmed` (won holdout) ·
`shipped`

| ID | Date | Config / hypothesis | Code (branch@commit) | Key result (research window) | Status | Notes |
|---|---|---|---|---|---|---|
| E000 | 2026-07-22 | Incumbent scoring as shipped (nowcasting refactor, no logic change) | main@24e1c0a | Research window (502 tkr, 30,120 tkr-days): lead-lag peak at **offset 0** (raw Δ corr +0.396, exo Δ +0.135); max future-side corr 0.064 (raw) / 0.035 (exo); best ICs are dexo_3/dexo_5 at h=1–3, IC ≈ 0.032–0.036, t ≈ 3.8–4.2, hit ≈ 0.68–0.73. Scorecard: `scripts/eval/baselines/RESEARCH_E000_2026-07-22.json` | baseline | The number to beat on the research window: peak right of zero, cost-surviving. Full-history release-gate baseline remains `BASELINE_2026-07-21.json`. In-window article latency (created−published): AV median 118 min / p90 619; Finnhub 132 / 504 |
| E001 | 2026-07-22 | dexo (3/5-day change of the market-free exo composite) as tracked candidate. **In-sample rediscovery — no new evidence**: the original study found and dismissed this same feature on this same window. Pre-registered promotion test below (§E001); log-and-measure only, no pipeline/scoring/serving changes | harness@35150da (pre-reg f566d92) | **Criterion (b) FAILED, (c) marginal-fail** (both in-sample diagnostics). Intraday (15-min snapshot bars): dexo future-side lead-lag ≈ 0 — max +0.0028 all-bars, +0.0035 RTH-only, curve flat noise, while the dmarket/dscore_raw k=−1 mirror spike (+0.10) proves the harness detects real structure ⇒ **"drift or noise"**, confirming the study. Overnight EOD dexo vs next-day return: +0.0071 (n=27.7k) ≈ 0. Cost overlay (15 bps RT on leg turnover; mkt-neutral; pre-reg cells): dexo_3×h2 gross 0.00257/period (t 2.30) → net 0.00021 (t 0.19); dexo_3×h3 gross 0.00225 (t 1.74) → net **−0.00053**; dexo_5×h1 gross 0.00208 (t 2.48) → net 0.00042 (t 0.49); dexo_5×h2 gross 0.00349 (t 2.97) → net 0.00141 (t 1.20). Leg turnover 0.56–0.93/rebalance eats the spread. Artifacts: `exports/eval/E001/` (scorecard, quintile_ls.csv, intraday_leadlag_*.csv, intraday_rth_*) | rejected (drift or noise) — in-sample rediscovery, no new evidence | Per §E001 pre-registration, failing any criterion ⇒ rejected. Criterion (a) holdout was **not** evaluated and no holdout peek will be spent on this candidate. Any revival (e.g. lower-cost execution assumption, different cells) is a NEW experiment ID with its own registration |
| E002 | 2026-07-22 | **Latency audit** (measurement only, no pipeline builds): reconcile Phase 5a's 8.4h AV / 2.7h Finnhub vs E000's ~118/~132 min publication→ingestion medians — 4× apart with flipped source ranking. Outage-backlog + lookback-effect controls, full-chain stage decomposition. Pre-registered decision rule below (§E002) | pre-reg 81fbdf9; measured @eef5126 | **Both old numbers reproduced and both wrong.** E000's 118/132 min = **survivorship artifact**: created_at ∈ research window has only ~6,080 survivors (~1.8% of ~345k ingested) — all created in the window's final 1.9 days with published_at ≥ the 30-day purge cutoff (matched to 4 min), mechanically capping latency at ~44h. Phase 5a's 8.4h AV = **outage-inflated**: recovery cohort (n=20,591 AV, median 81.5h fake latency) is ~25% of its AV sample. **Operative steady state** (created ∈ [07-06, 07-22), outage cohort excluded): **AV 243.6 min (4.1h) / p90 25.1h; FH 141.0 min (2.4h) / p90 11.3h**. Freshness split explains the ranking flip: fresh-at-fetch (≤6h) AV 96.1 min < FH 121.6 min — AV is *faster on new news*; AV's all-articles median is dragged by its 50-article lookback back-catalog (42.6% old-at-fetch, 13.3% of all AV >24h stale). Stage decomposition: ingest→FinBERT ≈0–5 min (same job run; +30 min worst case), FinBERT→tick 7.5–15 min, then ~4h EMA half-life on served `score` (score_raw unlagged). Fresh path end-to-end to score_raw: ~105–165 min. Artifacts: `exports/eval/E002/` (findings.md, CSVs, stage_decomposition.md) | audit complete — **verdict: middle band, defer heavy builds** | **Decision per §E002 rule:** FH (the fast-poll target) at 2.4h and AV's fresh path at 1.6h are in/below the 2–4h band ⇒ **defer the heavy latency builds** (Finnhub fast-poll, immediate FinBERT, event ticks); **cheap fixes proceed** (publication-age handling); **re-audit after ≥30 days** of clean accumulation. AV's nominal 4.1h sits at the "~4 h" boundary but its excess is back-catalog staleness at the *source* — no ingestion-speed build touches it. Deviation from pre-reg, documented: the registered "research window" is unmeasurable (survivorship); operative window moved to steady state per the "honest number that survives reconciliation" clause. Side finding: AV deep-backfill articles (13.3% of AV volume) are silently never FinBERT-scored (`get_unscored_articles` 48h filter) — currently sane (stale news shouldn't enter the narrative layer) but should be a conscious roadmap choice. The dominant *serving* lag is the 4h EMA, not ingestion |

## E001 pre-registration — dexo promotion test (registered 2026-07-22, BEFORE any E001 run)

Candidate: `dexo_3` / `dexo_5` (3- and 5-day changes of the exogenous
narrative/influencer/macro composite). In-sample leading cells from E000, fixed here:
**dexo_3 × h∈{2,3} and dexo_5 × h∈{1,2}** (both raw and market-neutral targets).

Promotion of dexo to a served/traded signal requires **ALL** of:

1. **(a) Holdout confirmation** — one evaluation on the post-outage holdout
   (`--window holdout`), run only once **≥ 40 trading days** have accumulated there
   (holdout starts 2026-07-03 → earliest evaluation ~end of August 2026; do NOT run
   before then). Pass = **IC ≥ 0.02 with t ≥ 2.0** in the pre-registered cells above.
   Exactly one evaluation, logged in `HOLDOUT.md`, win or lose. Research-window numbers
   are in-sample and count for nothing.
2. **(b) Intraday footprint** — exo-change features must show **positive future-side
   lead-lag correlation at 15-min or hourly resolution**. A multi-day daily effect with
   zero intraday footprint is classified as **"drift or noise"** and fails.
3. **(c) Cost survival** — the dexo quintile long-short spread must remain **positive
   net of the cost overlay** (default 15 bps round-trip charged on leg turnover at each
   rebalance; `analyze.quintile_ls`).

Failing any criterion → status `rejected`. No re-running with tweaked thresholds; a
changed threshold is a new experiment ID with its own registration.

## E002 pre-registration — latency audit decision rule (registered 2026-07-22, BEFORE measurement)

E002 is an audit, not a candidate. Instead of promotion criteria it pre-registers the
decision rule its numbers feed. **Operative number**: median publication→ingestion
latency (`created_at − published_at`) per source, research window, **outage-recovery
cohort excluded** (articles whose published_at falls in the 2026-06-23→07-03 gap but
were ingested after it), all articles — with the fresh-at-fetch vs old-at-fetch split
reported alongside to show where the latency comes from.

- **Operative median > ~4 h** → the latency builds (Finnhub fast-poll, immediate
  FinBERT on ingest, event-triggered scoring ticks) proceed as planned.
- **Operative median ≤ ~2 h** → the fast-poll job is deferred; only the cheap fixes
  (publication-age decay in the narrative weight) stay on the roadmap.
- **Between 2 h and 4 h** → defer the heavy builds, implement the cheap fixes, and
  re-audit after ≥30 more days of accumulation.

The rule binds on the honest number that survives reconciliation, whichever
measurement that turns out to be. Verdict and full decomposition go in the row above;
artifacts to `exports/eval/E002/`.
