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
| E000 | 2026-07-21 | Incumbent scoring as shipped (nowcasting refactor, no logic change) | main@e19da81 | Baseline scorecard `scripts/eval/baselines/BASELINE_2026-07-21.json`; coincident-to-lagging, no predictive lead (docs/SUMMARYOFTESTING.md) | baseline | The number to beat: lead-lag peak must move right of zero, out-of-sample, cost-surviving |
