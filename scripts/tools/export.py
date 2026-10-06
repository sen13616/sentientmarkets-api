#!/usr/bin/env python3
"""
tools/export.py

Standalone database export script for the SentimentAPI pipeline.

Usage
-----
    python3 tools/export.py              # full database export (all tables)
    python3 tools/export.py sentiment    # sentiment history + computed columns
    python3 tools/export.py signals      # raw signals (last 30 days) + summary
    python3 tools/export.py articles     # all raw_articles rows
    python3 tools/export.py scores       # top/bottom 50 + ranked list

All output goes to tools/exports/.
Progress and file paths are printed to stdout.
"""

from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# db_exports handles dotenv loading and all DB connections
from db_exports import (
    EXPORTS_DIR,
    export_articles,
    export_full_database,
    export_raw_signals,
    export_sentiment_history,
    export_top_bottom_scores,
)
from rich.console import Console

console = Console()


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------


async def cmd_full() -> None:
    console.print("[bold yellow]Full database export[/bold yellow] — fetching all tables…\n")

    def _progress(table: str, n: int) -> None:
        console.print(f"  [dim]{table:<28}[/dim] [yellow]{n:>10,}[/yellow] rows")

    folder, total = await export_full_database(_progress)
    console.print(
        f"\n[bold green]Done.[/bold green]  "
        f"[yellow]{total:,}[/yellow] total rows exported to:\n"
        f"  [cyan]{folder}[/cyan]"
    )


async def cmd_sentiment() -> None:
    console.print("[bold yellow]Exporting sentiment history…[/bold yellow]")
    path, n = await export_sentiment_history()
    console.print(
        f"[bold green]Done.[/bold green]  [yellow]{n:,}[/yellow] rows → [cyan]{path}[/cyan]"
    )


async def cmd_signals() -> None:
    console.print("[bold yellow]Exporting raw signals (last 30 days)…[/bold yellow]")
    (raw_path, n_raw), (sum_path, n_sum) = await export_raw_signals()
    console.print(
        f"[bold green]Done.[/bold green]\n"
        f"  Raw data: [yellow]{n_raw:,}[/yellow] rows → [cyan]{raw_path}[/cyan]\n"
        f"  Summary:  [yellow]{n_sum:,}[/yellow] rows → [cyan]{sum_path}[/cyan]"
    )


async def cmd_articles() -> None:
    console.print("[bold yellow]Exporting articles…[/bold yellow]")
    path, n = await export_articles()
    console.print(
        f"[bold green]Done.[/bold green]  [yellow]{n:,}[/yellow] rows → [cyan]{path}[/cyan]"
    )


async def cmd_scores() -> None:
    console.print("[bold yellow]Exporting top / bottom scores (last 24 h)…[/bold yellow]")
    (tp, nt), (bp, nb), (rp, nr) = await export_top_bottom_scores()
    console.print(
        f"[bold green]Done.[/bold green]\n"
        f"  Top 50:  [yellow]{nt}[/yellow] rows → [cyan]{tp}[/cyan]\n"
        f"  Bot 50:  [yellow]{nb}[/yellow] rows → [cyan]{bp}[/cyan]\n"
        f"  Ranked:  [yellow]{nr}[/yellow] rows → [cyan]{rp}[/cyan]"
    )


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

_COMMANDS: dict[str, tuple[str, object]] = {
    "full": ("Full database export (all tables)", cmd_full),
    "sentiment": ("Sentiment history + computed columns", cmd_sentiment),
    "signals": ("Raw signals — last 30 days + summary", cmd_signals),
    "articles": ("All articles", cmd_articles),
    "scores": ("Top / bottom 50 scores today", cmd_scores),
}


def _usage() -> None:
    console.print("\n[bold]Usage:[/bold]")
    console.print("  python3 tools/export.py [command]\n")
    console.print("[bold]Commands:[/bold]")
    for cmd, (desc, _) in _COMMANDS.items():
        console.print(f"  [cyan]{cmd:<12}[/cyan] {desc}")
    console.print(
        f"\n  (no command)   Full database export\n\nOutput directory: [cyan]{EXPORTS_DIR}[/cyan]\n"
    )


async def main() -> None:
    cmd_name = sys.argv[1].lower() if len(sys.argv) > 1 else "full"

    if cmd_name in ("-h", "--help", "help"):
        _usage()
        return

    if cmd_name not in _COMMANDS:
        console.print(f"[red]Unknown command:[/red] {cmd_name}")
        _usage()
        sys.exit(1)

    _, fn = _COMMANDS[cmd_name]
    try:
        await fn()
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted.[/yellow]")
    except Exception as exc:  # noqa: BLE001
        console.print(f"\n[bold red]Error:[/bold red] {exc}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
