## What and why

<!-- What does this change, and why is it needed? Link any issue. -->

## Checklist

- [ ] Tests added or updated; `pytest` and `ruff check . && ruff format --check .` pass
- [ ] **Affects served scores?** If yes: eval gate run (attach the scorecard diff) and a dated note in `METHODOLOGY.md`
- [ ] **New migration?** If yes: additive/idempotent, and applied to production **before** merge
- [ ] Docs updated where behavior changed (`README.md`, `docs/`, `METHODOLOGY.md`, `CHANGELOG.md`)
- [ ] No secrets, `.env` files or data exports committed
