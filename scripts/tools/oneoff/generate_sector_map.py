#!/usr/bin/env python3
"""
tools/generate_sector_map.py  (Sprint P4.1, one-shot)

Probe yfinance for `Ticker(t).info["sector"]` across the active ticker
universe and emit a static Python dict to `tools/sector_map.py`.

Run once during sprint setup, then commit the generated dict. Subsequent
deploys use the static map via `tools/seed_sectors.py`; this generator
is not part of the recurring pipeline.

Usage:
    DATABASE_URL=... python3 scripts/tools/oneoff/generate_sector_map.py

The dict values are validated against `pipeline.sources.macro.SECTOR_ETFS`
keys (the canonical 11-class GICS taxonomy). Any ticker whose yfinance
sector does not match the taxonomy is logged and emitted as a TODO
comment so a human can fix the mapping manually.
"""

from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

import yfinance as yf

from pipeline.sources.macro import SECTOR_ETFS
from scripts.tools.company_names import COMPANY_NAMES  # noqa: E402

VALID_SECTORS = set(SECTOR_ETFS.keys())
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "sector_map.py")

# yfinance returns Yahoo Finance's sector taxonomy (e.g. "Healthcare", "Technology").
# Translate to the canonical 11-class GICS names used in SECTOR_ETFS so the
# downstream join in P4.2 works without any string-massaging.
_YAHOO_TO_GICS: dict[str, str] = {
    "Basic Materials": "Materials",
    "Communication Services": "Communication Services",
    "Consumer Cyclical": "Consumer Discretionary",
    "Consumer Defensive": "Consumer Staples",
    "Energy": "Energy",
    "Financial Services": "Financials",
    "Healthcare": "Health Care",
    "Industrials": "Industrials",
    "Real Estate": "Real Estate",
    "Technology": "Information Technology",
    "Utilities": "Utilities",
}

# Tickers that yfinance no longer recognizes because the company has been
# renamed, acquired, or delisted since the April 2024 S&P 500 snapshot
# encoded in tools/company_names.py. Hand-curated GICS sectors based on the
# company's last published sector. Kept inline (not in a separate file) so
# the lookup is reviewable alongside the rest of this script.
_FALLBACK_SECTORS: dict[str, str] = {
    "ABC": "Health Care",  # AmerisourceBergen → Cencora (COR), 2023 rename
    "AMED": "Health Care",  # Amedisys, acquired by UnitedHealth
    "ANSS": "Information Technology",  # Ansys, acquired by Synopsys
    "DFS": "Financials",  # Discover Financial Services, being acquired by Capital One
    "FI": "Information Technology",  # Fiserv post-merger ticker
    "FISV": "Information Technology",  # Fiserv pre-merger ticker
    "HES": "Energy",  # Hess Corporation, acquired by Chevron
    "IPG": "Communication Services",  # Interpublic Group, merging with Omnicom
    "JNPR": "Information Technology",  # Juniper Networks, being acquired by HPE
    "K": "Consumer Staples",  # Kellanova (formerly Kellogg)
    "LSI": "Real Estate",  # Life Storage, acquired by Extra Space
    "MMC": "Financials",  # Marsh McLennan
    "MRO": "Energy",  # Marathon Oil, acquired
    "PARA": "Communication Services",  # Paramount Global, acquired by Skydance
    "PDCO": "Health Care",  # Patterson Companies (dental/animal-health supply)
    "PEAK": "Real Estate",  # Healthpeak Properties (rebranded DOC)
    "PXD": "Energy",  # Pioneer Natural Resources, acquired by ExxonMobil
    "SRCL": "Industrials",  # Stericycle, acquired
    "WBA": "Consumer Staples",  # Walgreens Boots Alliance
}


# yfinance uses "-" rather than "." for class-share suffixes (BRK.B → BRK-B).
def _yf_symbol(ticker: str) -> str:
    return ticker.replace(".", "-")


def _active_tickers() -> list[str]:
    """Active universe is the 502 tickers in tools/company_names.py.

    Sourced from the static file rather than the DB so the generator
    runs locally without Railway access.
    """
    return sorted(COMPANY_NAMES.keys())


def _fetch_sector(ticker: str, max_retries: int = 2) -> tuple[str | None, str | None]:
    """Return (gics_sector, raw_yfinance_value).

    Tries yfinance first (with "." → "-" symbol massage for class shares);
    falls back to the hand-curated `_FALLBACK_SECTORS` table for tickers
    that yfinance no longer recognizes (renamed/acquired/delisted since
    the April 2024 S&P 500 snapshot). Retries with exponential backoff on
    transient errors / rate limits.
    """
    yf_symbol = _yf_symbol(ticker)
    raw: str | None = None
    for attempt in range(max_retries + 1):
        try:
            raw = yf.Ticker(yf_symbol).info.get("sector")
            break
        except Exception:
            if attempt < max_retries:
                time.sleep(2.0 * (attempt + 1))  # 2s, 4s
                continue
            raw = None
            break

    if raw:
        gics = _YAHOO_TO_GICS.get(raw)
        if gics is not None and gics in VALID_SECTORS:
            return gics, raw
        if raw in VALID_SECTORS:
            return raw, raw

    # Manual fallback for stale tickers.
    fallback = _FALLBACK_SECTORS.get(ticker)
    if fallback is not None and fallback in VALID_SECTORS:
        return fallback, f"<fallback: {fallback}>"

    return None, raw


def main_sync() -> int:
    tickers = _active_tickers()
    print(f"Active universe: {len(tickers)} tickers; harvesting yfinance sectors…", file=sys.stderr)

    sectors: dict[str, str] = {}
    unmapped: list[tuple[str, str | None]] = []
    failures: list[str] = []

    t_start = time.monotonic()
    for i, ticker in enumerate(tickers, 1):
        matched, raw = _fetch_sector(ticker)
        time.sleep(0.3)  # Pace requests to avoid yfinance rate-limits
        if matched is not None:
            sectors[ticker] = matched
        elif raw is None:
            failures.append(ticker)
        else:
            unmapped.append((ticker, raw))

        if i % 50 == 0 or i == len(tickers):
            elapsed = time.monotonic() - t_start
            print(
                f"  {i}/{len(tickers)} ({elapsed:.0f}s) — matched={len(sectors)} "
                f"unmapped={len(unmapped)} failed={len(failures)}",
                file=sys.stderr,
            )

    # ── Write tools/sector_map.py ────────────────────────────────────────
    with open(OUTPUT_PATH, "w") as f:
        f.write('"""\n')
        f.write("tools/sector_map.py\n\n")
        f.write("Static GICS sector mapping for all tier-1-supported tickers in\n")
        f.write("ticker_universe. Used by tools/seed_sectors.py to populate the\n")
        f.write("sector column added in migrations/008_add_ticker_sector.sql.\n\n")
        f.write('Generated 2026-05-15 via yfinance Ticker(t).info["sector"].\n')
        f.write("Re-run tools/generate_sector_map.py to regenerate.\n\n")
        f.write("Values must match keys of pipeline.sources.macro.SECTOR_ETFS exactly\n")
        f.write("(11-class GICS taxonomy). Validation is enforced at import time below.\n")
        f.write('"""\n')
        f.write("from __future__ import annotations\n\n")
        f.write("TICKER_SECTORS: dict[str, str] = {\n")
        for ticker in sorted(sectors):
            f.write(f'    "{ticker}": "{sectors[ticker]}",\n')
        f.write("}\n\n")
        if unmapped:
            f.write("# Tickers whose yfinance sector string does not match the canonical\n")
            f.write("# 11-class GICS taxonomy — needs manual review/fix before commit:\n")
            for ticker, raw in unmapped:
                f.write(f"# UNMAPPED: {ticker!r} → yfinance returned {raw!r}\n")
            f.write("\n")
        if failures:
            f.write("# Tickers where yfinance returned None or raised:\n")
            for ticker in failures:
                f.write(f"# FAILED: {ticker!r}\n")
            f.write("\n")
        f.write("# Validate against the canonical taxonomy at import time.\n")
        f.write("from pipeline.sources.macro import SECTOR_ETFS as _SECTOR_ETFS\n")
        f.write("_VALID = set(_SECTOR_ETFS.keys())\n")
        f.write("_invalid = {t: s for t, s in TICKER_SECTORS.items() if s not in _VALID}\n")
        f.write('assert not _invalid, f"sector_map.py contains non-GICS sectors: {_invalid}"\n')

    # ── Summary ──────────────────────────────────────────────────────────
    print("\n=== Generation complete ===", file=sys.stderr)
    print(f"  output:    {OUTPUT_PATH}", file=sys.stderr)
    print(f"  matched:   {len(sectors)}/{len(tickers)}", file=sys.stderr)
    print(f"  unmapped:  {len(unmapped)} (yfinance returned non-GICS string)", file=sys.stderr)
    print(f"  failed:    {len(failures)} (yfinance returned None or raised)", file=sys.stderr)
    if unmapped:
        print("\n  Unmapped detail (first 20):", file=sys.stderr)
        for ticker, raw in unmapped[:20]:
            print(f"    {ticker}: {raw!r}", file=sys.stderr)
    if failures:
        print(f"\n  Failed detail (first 20): {failures[:20]}", file=sys.stderr)

    # Gate per sprint plan: ≥497 of ~502 mapped
    if len(sectors) < 497:
        print(f"\n  ⚠ FAIL: only {len(sectors)} matched — plan gate is ≥497.", file=sys.stderr)
        return 1
    print(f"\n  ✓ Gate met: {len(sectors)} ≥ 497", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main_sync())
