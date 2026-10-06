"""
tests/integration_test_sources.py

Integration tests for the four pipeline source fetchers.
Each test makes real external API calls and writes to the database.

Requires:
  - Local PostgreSQL running (sentimentapi DB seeded with backfill data)
  - Valid API keys in .env

Run with:
    pytest -m integration
"""

import pytest  # noqa: F401  (marker used via decorator)

from pipeline.sources.influencer import fetch_influencer_signals
from pipeline.sources.macro import fetch_macro_signals
from pipeline.sources.market import fetch_market_signals
from pipeline.sources.narrative import fetch_narrative_signals
from scripts.db.connection import close_pool, init_pool

TICKER = "AAPL"


@pytest.mark.integration
async def test_market_signals():
    """fetch_market_signals completes without exception for a live ticker."""
    try:
        await init_pool()
        await fetch_market_signals(TICKER)
    finally:
        await close_pool()


@pytest.mark.integration
async def test_narrative_signals():
    """fetch_narrative_signals completes without exception for a live ticker."""
    try:
        await init_pool()
        await fetch_narrative_signals(TICKER)
    finally:
        await close_pool()


@pytest.mark.integration
async def test_influencer_signals():
    """fetch_influencer_signals completes without exception for a live ticker."""
    try:
        await init_pool()
        await fetch_influencer_signals(TICKER)
    finally:
        await close_pool()


@pytest.mark.integration
async def test_macro_signals():
    """fetch_macro_signals completes without exception."""
    try:
        await init_pool()
        await fetch_macro_signals()
    finally:
        await close_pool()
