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
| E003 | 2026-07-22 | **Positioning features** short_vol_z / insider_net_z (levels) as candidates — first genuinely new features of the program (never examined by the original study). Backfilled point-in-time into research_features (Track B4); auto-registered by the harness. Pre-registration below (§E003) committed before any evaluation run (32fc9a7) | pre-reg 32fc9a7; evaluated via `scripts/eval/experiments/e003_positioning.py` | **Preflight PASS** (PIT spot-check 6/6 exact; 25 delisted names NaN not zero-filled; coverage: short_vol_z 32/60 days × ~477 tkr — FINRA history only starts 05-07 + 10-obs warmup; insider 59/60 days × ~350 tkr). **short_vol_z: (a) FAIL, (b) FAIL** — IC sign correct (negative) at ALL 4 horizons and both populated sub-periods, but max |IC| 0.0146 < 0.02, max |t| 1.17 < 2.5 on only 22–28 days; sign-aligned net L/S negative everywhere. Lead-lag mass 74% past-side (past 0.144 vs future 0.051; positive past corr = shorting follows rallies) → mirror flag. **insider_net_z_lag2: (a) PASS** — h5 IC +0.0569 t 4.27, h3 +0.0529 t 4.24, h2 +0.0463 t 2.90 (all ≥0.02, ≥2.5, right sign), 3/3 sub-periods positive at each of h∈{2,3,5}; **(b) PASS** at h=3: net L/S +0.00322/period, t_net 1.84 ≥ 1.5, ann Sharpe net 2.36, leg turnover 0.45 (the slow-feature thesis: vs dexo's 0.56–0.93). Diagnostic upper bound confirmed as pre-registered: unlagged IC (h5 0.0764) > lagged (0.0569) — the lagged number is the real one. Lead-lag mass small and mixed (past 0.041 vs future 0.018), no mirror flag tripped. Artifacts: `exports/eval/E003/` | **short_vol_z: rejected** (thresholds unmet on thin sample; holdout unspent) · **insider_net_z_lag2: won-research** — holdout appointment booked (see HOLDOUT.md), NOT evaluated | short_vol_z's consistent right-sign ICs on 28 days are directionally interesting — re-examination once ≥3 months of FINRA history accumulates is a NEW experiment ID. insider_net_z_lag2 caveats for the holdout: 47–53 IC days (~2.5 months, one regime), h1 cell fails (the effect is a multi-day drift), and the ~2-day transactionDate optimism is already corrected by the lag — the holdout must confirm h∈{2,3,5} cells at |IC|≥0.02, |t|≥2.0, positive sign. **Annex (2026-07-22, diagnostics only — verdict/criteria/holdout terms unchanged; `exports/eval/E003/annex/`):** (1) *Sector-neutrality*: raw-value sector×day demeaning collapses the IC (23–50% retained) but this is a heavy-tail artifact — the feature's z-values are pathological (min −30.9k; zero-filled baselines ⇒ tiny σ); outlier-robust **rank** demeaning within sector×day retains **72–104%** (h2 +0.0480 t3.16 / h3 +0.0451 t3.86 / h5 +0.0411 t3.26) ⇒ within-sector signal, not a sector bet — but consume as ranks/winsorized z, never raw-linear. (2) *Concentration*: ex top-5 |IC| days retains 64–80% (t up to 4.56 — big days include negatives); ex top-5 influential tickers simultaneously (CRWD/AMD/NET/TXN/MPWR/HSY/FTNT variants) retains **104–110%** — not living in a handful of observations. (3) *Event-time decay* (research window, 592 net-buy events; 157 >10k-share): material buys drift to **+0.94% by k=3 (t 2.3), +1.24% by k=6 (t 2.0)**, plateau through k≈13, no reversal in 3 weeks; accrual front-loaded in first ~6 trading days; ~+0.35% already gone by the +2-day filing lag ⇒ ~+0.6–0.9% exploitable post-filing. Usage implication if confirmed: hold ~4–7 trading days from signal availability, daily refresh. Cautions for holdout: economics concentrate in ~157 material events (holdout hinges on a modest count of large filings); heavy-tail z argues for a rank/winsorized variant as its own follow-up ID |
| E004 | 2026-07-22 | **EMA half-life** (nowcast experiment via the validated replay scorer). Motivation from E002: fresh information reaches score_raw in ~105–165 min but the served `score` sits behind a 4h-half-life EMA — the dominant serving lag is self-inflicted smoothing. Success metrics are responsiveness + stability, NOT forward IC; the holdout machinery does not apply (it exists for predictive claims). Pre-registration below (§E004) | pre-reg c31b2ee; engine aa282d6; run `scripts/eval/experiments/e004_ema.py` | Full clean history, 2.2M ticks, all candidates on ONE shared replayed raw composite. **Incumbent 4h**: mean \|score−raw\| 1.80, flips 2.53/tkr-wk, tick-std 0.258. **2h**: gap 1.21 (−33%), flips 3.38 (**1.34×** ≤1.5×), std 0.343 (**1.33×** ≤1.75×), guard ICs h1 +0.0043 / h3 −0.0128 vs incumbent +0.0034 / −0.0142 (no degradation — slightly better). **1h**: gap 0.76 but flips 4.37 (**1.73× — FAILS**) and std 0.478 (**1.85× — FAILS**). Exploratory (cannot be recommended): 0.5h gap 0.44, flips 5.51 (2.2×); adaptive gap 1.32 at flips 3.08 (1.22×) — dominates 2h on paper, needs its own ID. Guard computed research-window only (holdout constraint). Artifacts: `exports/eval/E004/` | **shipped** (recommendation T½ = 2h, rule-derived; 1h disqualified on both stability gates) | **SHIPPED 2026-07-22 ~03:25 UTC** with explicit owner go-decision: `EMA_HALF_LIFE_HOURS=2` set on Railway (deployment 75105b9c, SUCCESS; /health ok). **Live verification**: first post-deploy tick (03:30 UTC), implied α median 0.1590 over n=78 tickers with |raw−prev|>2 — matches T½=2h prediction 0.1591 (old 4h would be 0.0830). INTEGRATION.md changelog entry added (value-affecting, semantics unchanged; score_raw/score_exo unaffected). Release gate re-baselined: `BASELINE_2026-07-22_ema2h.json` (full history 04-24→07-22, new file — BASELINE_2026-07-21.json kept for the record). First non-additive semantic change of the program. `score_exo` stays unsmoothed. Exploratory `adaptive` queued as E005 |
| E005 | 2026-07-22 | **QUEUED — not started.** Adaptive EMA: surprise-scaled half-life `T½ = clamp(4h/(1+|raw−prev|/5), 1h, 4h)` (or the equivalent re-based on the 2h incumbent). Motivation: as E004's exploratory arm it dominated the 2h winner on paper — smaller gap to raw (1.32 vs incumbent-relative) at FEWER label flips (1.22× vs 1.34×). Needs its own pre-registration (candidates incl. re-tuned clamp bounds vs the NEW 2h incumbent, same metric battery, same rule shape) before any run | — | — | queued | Do not run without pre-registering. Also queued from E003 annex: rank/winsorized insider_net_z variant (heavy-tail fix) — will get its own ID when picked up |

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

## E003 pre-registration — positioning features (registered 2026-07-22, BEFORE any evaluation)

Candidates: `rf_short_vol_z` and `rf_insider_net_z` **levels** (not diffs), as
backfilled by Track B4. The two features are evaluated and succeed/fail independently.

**Sign priors (from the literature, fixed before looking):**
- `short_vol_z`: elevated off-exchange short-volume pressure → informed short sellers →
  **expected IC NEGATIVE**.
- `insider_net_z`: net insider buying → **expected IC POSITIVE**.
- A statistically strong result with the WRONG sign is a **failure**, not a discovery.

**Look-ahead correction (insider):** `insider_net_z` is built on transactionDate-stamped
rows that can precede the public Form-4 filing by up to ~2 business days
(pipeline/features/positioning.py docstring). Therefore:
- `insider_net_z_lag2` — the feature lagged **2 trading days** per ticker (eval-time
  shift, point-in-time-safe, eval scripts only) — is the ONLY promotable insider
  variant: it represents what was publicly knowable.
- The unlagged `insider_net_z` is computed as a **diagnostic upper bound only**.
  Pre-registered expectation: unlagged IC ≥ lagged IC; the lagged IC is the real number.
- `short_vol_z` needs NO lag: the signal is stamped 21:00 UTC (market close) and FINRA
  publishes the file ~21:30 UTC the same day — both post-close, and the backfill only
  annotates ticks at/after the stamp, so the stored value was publicly knowable at the
  annotated tick.

**Primary cells (small, fixed):**
- `short_vol_z` × horizons {1, 2, 3, 5}d — market-neutral target.
- `insider_net_z_lag2` × horizons {1, 2, 3, 5}d — market-neutral target.
Everything else the auto-registration surfaces (the `drf_*_1` diffs, unlagged insider,
raw-target ICs) is **exploratory**: reported, but can never trigger promotion — at most
a future experiment ID.

**Holding-period variants (E001's turnover lesson):** quintile long-short at rebalance
periods h ∈ {1, 3, 5} days on the primary features, gross AND net of the 15 bps
round-trip cost overlay, with the L/S portfolio **sign-aligned to the prior**
(for short_vol_z the spread is evaluated on the negated feature — long low-pressure /
short high-pressure). The net numbers are the ones that matter.

**Promotion criteria (ALL required, per feature):**
1. **(a) Research-window IC:** pre-registered sign, |IC| ≥ 0.02, |t| ≥ 2.5 on at least
   one primary cell, AND sign consistency: the window split into 3 equal date
   sub-periods must show the registered IC sign in all 3 for that cell.
2. **(b) Cost survival:** sign-aligned net-of-cost L/S spread positive with t ≥ 1.5 at
   at least one pre-registered holding period (1, 3, or 5d).
3. **(c) Holdout:** one evaluation at ≥ 40 accumulated post-outage trading days
   (~end of August 2026, booked next to any other pending confirmation — never early),
   confirming (a)'s winning cell at |IC| ≥ 0.02, |t| ≥ 2.0, correct sign. Failing (a)
   or (b) now ⇒ **rejected without spending the holdout**. Thresholds changed later ⇒
   new experiment ID.

**Lead-lag flag:** both features are daily-frequency, so no intraday criterion applies.
The daily lead-lag curve is still computed; a feature whose correlation mass sits
entirely at negative offsets is flagged as mirror-like even if its forward IC passes —
the flag is recorded in the row and weighs against promotion at the holdout stage.

**Data-quality preflight (must run BEFORE any IC is computed):** per-feature per-day
non-null coverage across the research window (thin insider coverage must be visible);
point-in-time verification on the evaluation path (spot-recompute stored values from
raw signals with timestamp ≤ tick); confirmation that delisted/no-data names appear as
NaN (excluded), never zero-filled.

## E004 pre-registration — EMA half-life decision rule (registered 2026-07-22, BEFORE any candidate run)

Engine: `scripts/eval/replay.py` — the identity check (production config reproduces
stored raw/smoothed/exo within documented tolerances over 2.2M ticks) passed at
commit aa282d6 before this experiment runs. All candidates share the identical
replayed raw composite; only the smoothing differs.

**Candidates:** primary **{1h, 2h}** against the **4h incumbent**; exploratory
**{0.5h, adaptive}** where adaptive = T½ scaled down with surprise,
`T½ = clamp(4h / (1 + |raw − prev_smoothed|/5), 1h, 4h)`. Exploratory candidates are
reported but CANNOT be recommended without a new experiment ID.

**Metrics, per candidate over the FULL clean history via replay:**
1. **Responsiveness** — (i) cross-correlation of Δsmoothed_t vs Δraw_{t−k} at tick
   resolution for k = 0..6: the peak k is the tracking lag in ticks; (ii) mean
   |smoothed − raw|.
2. **Stability** — (i) label-flip frequency: 5-band label (production
   `score_to_label` on the int-rounded smoothed score) transitions per ticker-week —
   the user-facing cost of less smoothing; (ii) tick-to-tick std of the smoothed
   score (pooled std of per-ticker one-tick changes).
3. **Guard** — forward IC (daily cross-sectional Spearman, market-neutral, h ∈ {1,3})
   of the smoothed score LEVEL must not degrade materially: candidate mean_IC ≥
   incumbent mean_IC − 0.01 at both horizons. **Scope note (holdout constraint):**
   the guard is computed on the RESEARCH WINDOW ONLY — running forward-return
   statistics over full history would evaluate on holdout returns, which is
   forbidden; nowcast metrics (1)–(2) use full history as registered.

**Decision rule (fixed):** recommend the SHORTEST primary half-life whose
label-flip frequency ≤ **1.5×** the incumbent's AND tick-to-tick std ≤ **1.75×**
the incumbent's AND guard passes. If none qualifies, the incumbent stands.

**Output:** metrics table for all candidates, the rule-derived recommendation, and —
if a change is recommended — a deployment note covering: it alters served score
values (the first non-additive semantic change of the program), needs an
INTEGRATION.md changelog entry and an explicit go-decision from the owner, ships
later via the `EMA_HALF_LIFE_HOURS` env var (nothing is changed by this
experiment), and requires re-baselining the release gate afterward. `score_exo`
remains unsmoothed regardless — out of scope.
