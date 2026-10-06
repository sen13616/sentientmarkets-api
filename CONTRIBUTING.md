# Contributing

## Setup

```bash
python3 -m venv .venv && source .venv/bin/activate     # Python 3.12
pip install -r requirements-dev.txt
cp .env.example .env                                   # only needed for live DB/API work
```

Unit tests need no database, Redis or API keys:

```bash
pytest                              # integration tests are deselected by default
ruff check . && ruff format --check .
```

## Workflow

1. Branch from `main` (`feature/…`, `fix/…`). Never commit to `main` directly. It deploys to
   production automatically.
2. Keep commits focused, with messages that say *why*. Behavior-neutral changes (formatting,
   moves) go in their own commits.
3. Open a pull request. CI (lint, format, tests, secret scan) must pass, and the PR template's
   checklist must be filled in.

## Rules that protect the research record

- **Scoring changes go through the evaluation gate.** Anything that changes served scores
  (weights, normalizers, staleness, EMA, the universe) must run the harness against the
  committed baseline, and the change must be recorded in `METHODOLOGY.md` (dated, in §16.5 for
  method changes):

  ```bash
  python3 -m scripts.eval.run --window all --start 2026-04-24 --end <today> \
      --out exports/eval --baseline scripts/eval/baselines/BASELINE_2026-07-22_ema2h.json
  ```

  The harness reads served history, so it can't preview an undeployed change. Measure after
  deploying, and re-baseline if the intended movement passes review (`scripts/eval/baselines/README.md`).
- **Migrations before code.** Apply a new `scripts/migrations/0NN_*.sql` to production *before*
  merging code that reads or writes its columns. Migrations must be additive and idempotent
  (`ADD COLUMN IF NOT EXISTS`).
- **Never rewrite history tables.** `sentiment_history` and `price_snapshots` are append-only
  research data. Offline repairs must tag rows (`replay_run`) and archive anything they replace.
- **Experiments are logged.** Every strategy or configuration tested goes in
  `scripts/eval/EXPERIMENTS.md`, including failures. The holdout window is spent only per
  `scripts/eval/HOLDOUT.md`.

## Secrets

Never commit `.env` or any key. `.env.example` is the template. GitHub secret scanning with
push protection is enabled, and CI runs gitleaks. If a secret is ever exposed, rotate it first:
removing it from git does not make it safe. See [`SECURITY.md`](SECURITY.md).

## Style

`ruff` is configured in `pyproject.toml` (lint rules E, F, W, I, B, UP; line length 100).
Match the surrounding code's comment density and naming. Every SQL query lives in
`scripts/db/queries/`, except the read-only analytical loaders in `scripts/eval/` and `research/`.
