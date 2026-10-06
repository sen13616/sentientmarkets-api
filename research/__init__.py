"""
research — strategy-testing toolkit on point-in-time sentiment snapshots.

Workflow (see research/README.md):

    python -m research.snapshot --start 2026-05-21 --end 2026-10-06   # export (needs DB)
    python -m research.run --snapshot data/snapshots/2026-10-06 \\
        --strategy research.examples.quintile_ls:QuintileLongShort      # backtest (offline)

Snapshots are read-only exports; nothing in this package writes to the
production database.
"""
