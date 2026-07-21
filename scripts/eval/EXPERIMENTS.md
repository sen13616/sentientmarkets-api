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
