# Holdout Split — frozen 2026-07-22

## The split

| Window | Dates (UTC) | Notes |
|---|---|---|
| **Research** | **2026-04-24 → 2026-06-22** (inclusive; `--end 2026-06-23` exclusive) | All candidate development, ranking, and iteration happens here, and only here |
| *(excised)* | 2026-06-23 → 2026-07-03 | Ingestion outage — no usable data; natural boundary between the windows (`DEFAULT_GAPS` in `scripts/eval/analyze.py`) |
| **Holdout** | **2026-07-03 → present**, plus everything that accumulates going forward | Never used for development. Evaluated only to confirm an already-won candidate |

## The rule

> **Candidates are developed and ranked on the research window only. The holdout is
> evaluated only when a candidate has already won on the research window, and every
> holdout evaluation is logged here with a date and result.**

Rationale: the backtest study (`docs/SUMMARYOFTESTING.md`) covers ~3 months of a single
market regime. Iterating candidate configurations against the full history guarantees
overfitting — with enough tries, something will look predictive by chance. The holdout
exists to answer one question per candidate, once: *does the research-window win
generalize?* Each additional peek at the holdout burns some of its statistical value, so
peeks are rationed and recorded.

## Enforcement

`scripts/eval/run.py` takes `--window research|holdout|all` with **`research` as the
default**. Explicit `--start/--end` outside the selected window are rejected. Running
`--window holdout` prints a reminder to log the evaluation below. `--window all` exists
for reproducing the original full-history study and for non-candidate diagnostics only.

The frozen dates live in `scripts/eval/run.py` (`RESEARCH_START`, `RESEARCH_END`,
`HOLDOUT_START`). Do not move them. If a new outage ever splits the holdout itself, add a
gap via `--gap`, do not redraw the windows.

## Booked confirmations (won-research, awaiting the ≥40-trading-day mark)

Earliest evaluation date: **~2026-08-28** (40 trading days after 2026-07-03).
One evaluation each, then a row in the log below — win or lose.

| Booked | Candidate (experiment ID) | Cells to confirm | Pass threshold |
|---|---|---|---|
| 2026-07-22 | `insider_net_z_lag2` (E003) | IC × h∈{2,3,5}, market-neutral, levels | \|IC\| ≥ 0.02, \|t\| ≥ 2.0, positive sign (per §E003 pre-registration) |

## Holdout evaluation log

Every `--window holdout` run against a candidate gets one row — no exceptions, including
failures and "just checking" runs.

| Date | Candidate (experiment ID) | Commit | Research-window result | Holdout result | Verdict |
|---|---|---|---|---|---|
| — | — | — | — | — | *(no holdout evaluations yet)* |
