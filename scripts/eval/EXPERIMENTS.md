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
| E001 | 2026-07-22 | dexo (3/5-day change of the market-free exo composite) as tracked candidate. **In-sample rediscovery — no new evidence**: the original study found and dismissed this same feature on this same window. Pre-registered promotion test below (§E001); log-and-measure only, no pipeline/scoring/serving changes | main@8b454f9 + harness | *(pending — cost overlay + intraday diagnostic to be recorded here)* | in-sample rediscovery — no new evidence | Research-window dexo statistics are in-sample and can NEVER count toward promotion. Promotion requires all three §E001 criteria |

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
