# Strategy testing (`research/`)

Tools for testing trading strategies on SentimentAPI scores **offline, reproducibly,
and without look-ahead**. Strategies run against a versioned snapshot exported from
the production database, never against the database itself.

```
export snapshot ──► load ──► define a Strategy ──► backtest ──► report
 (research.snapshot)  (research.dataset)  (research.strategy)  (research.backtest)  (research.metrics)
```

## Quick start

```bash
pip install -r requirements-dev.txt

# 1. Export a snapshot (needs DATABASE_URL in .env; read-only queries)
python -m research.snapshot --start 2026-05-21 --end 2026-10-06

# 2. Backtest a strategy on it (offline)
python -m research.run --snapshot data/snapshots/2026-10-06 \
    --strategy research.examples.quintile_ls:QuintileLongShort \
    --param feature=score_raw --cost-bps 15
```

The report (`REPORT.md`, `summary.json`, `returns.csv`) lands in `research/runs/<timestamp>_<strategy>/`
(gitignored). Snapshots live in `data/snapshots/` (gitignored). Share a snapshot by copying its
directory; its `manifest.json` records the window, row counts and the git commit it was exported at.

## Writing a strategy

```python
import pandas as pd
from research.strategy import SignalView, Strategy


class LowConfidenceFade(Strategy):
    name = "low-confidence-fade"

    def __init__(self, threshold: float = 40):
        self.threshold = threshold

    def weights(self, view: SignalView) -> pd.Series:
        today = view.today()  # one row per ticker for the signal date
        picks = today[today["confidence"] < self.threshold].index
        return pd.Series(1 / len(picks), index=picks) if len(picks) else pd.Series(dtype=float)
```

Run it with `--strategy mypackage.module:LowConfidenceFade --param threshold=35`.
`view.history` holds every row up to the signal date (for lookbacks: `view.lookback(days)`).

### Signal columns available per (ticker, date)

| column | meaning |
|---|---|
| `score` | served (EMA-smoothed) composite, 0–100 |
| `score_raw` | unsmoothed composite (divergence-capped) |
| `market`, `narrative`, `influencer`, `macro` | layer sub-indices, 0–100 (NaN = layer missing) |
| `confidence` | 0–100 |
| `xs_pct` | cross-sectional percentile of `score_raw` that day |
| `exo`, `exo_pct` | sentiment-only composite (market layer excluded) and its percentile |
| `dscore_raw_{1,3,5,7}`, `dexo_{1,3,5}`, `d{layer}_1` | changes over 1/3/5/7 rows |
| `replay_run` | NaN = served live; otherwise recomputed offline (see Data caveats) |

These are the same derived features the eval harness uses (`scripts/eval/analyze.py::prepare_daily`).

## Execution model (no look-ahead)

- A date's row is the **last scoring tick of that US/Eastern calendar day**, so it can contain
  news up to midnight ET.
- Weights decided from date *d* are executed at the **close of the first trading day strictly
  after *d*** (`--lag 1`, the default; `lag=0` is rejected). Friday, Saturday and Sunday rows all
  map to Monday's close; the latest one (Sunday) is used.
- Executed weights earn close-to-close returns until the next execution. Costs: `--cost-bps`
  per unit of one-way turnover, charged at execution.
- Only names in the **point-in-time universe** (`added_at` ≤ *d* < last trading day) and priced
  at the execution close can be held. A name that stops trading earns 0 afterwards and is released
  at the next rebalance.
- Signal dates inside a documented data gap keep the previous weights.
- Benchmark: equal-weight long-only portfolio of the point-in-time universe, daily, no costs.

`tests/test_research.py` pins this down. For example, a strategy that "knows" each day's return
earns nothing unless it targets exactly the return its executed weights will earn.

## Data caveats

- **Replayed periods.** 2026-06-23 → 07-03 and 2026-09-17 → 10-02 were scored offline after
  outages, and 2026-08-10 → 09-17 was rebuilt with backfilled news (METHODOLOGY.md §16.5). These rows
  carry `replay_run`. Use `--live-only` (or `Snapshot.load(..., include_replayed=False)`) to test on
  served scores only.
- **Universe.** 586 active tickers since 2026-10-03: all current S&P 500 members plus still-trading
  former names. `--sp500-only` restricts to the `in_sp500` flag, which is a snapshot as of the export,
  **not** historical index membership. 112 names were added on 2026-10-03 and have scores only from that
  date. 28 retired symbols keep their history up to their last trading day.
- **Market-layer methodology change on 2026-10-03.** Before it, the live market layer used
  contaminated close/volume history (METHODOLOGY.md §16.5). Compare periods on either side with care.
- **Prices** are adjusted daily bars from yfinance, as stored by the pipeline.

## Research discipline

Every configuration you try is an experiment. Log each one, including failures, in
`scripts/eval/EXPERIMENTS.md`, so the number of trials behind any "winner" is known. Hold out the
most recent period (`scripts/eval/HOLDOUT.md`) until a strategy has been fixed on the earlier
data. The example strategies in `research/examples/` demonstrate the interface. They are not
performance claims.
