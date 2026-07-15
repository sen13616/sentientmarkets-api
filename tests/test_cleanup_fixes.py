"""
tests/test_cleanup_fixes.py

Tests for the 2026-07-15 pipeline-fix/cleanup pass:
  • to_yahoo_symbol() — dot→dash mapping at the yfinance boundary (BRK.B fix)
  • cluster_articles() runs its sentence-transformer encode OFF the event
    loop (asyncio.to_thread), so the in-process API keeps serving during
    the narrative job.
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import numpy as np
import pytest

from pipeline.sources.market import to_yahoo_symbol


# ===========================================================================
# to_yahoo_symbol
# ===========================================================================

class TestToYahooSymbol:

    def test_class_shares_use_dash(self):
        assert to_yahoo_symbol("BRK.B") == "BRK-B"
        assert to_yahoo_symbol("BF.B") == "BF-B"

    def test_plain_tickers_unchanged(self):
        assert to_yahoo_symbol("AAPL") == "AAPL"
        assert to_yahoo_symbol("MSFT") == "MSFT"


# ===========================================================================
# cluster_articles encodes off the event loop
# ===========================================================================

class _StubModel:
    """Records which thread encode() ran on; returns identical unit vectors."""

    def __init__(self):
        self.encode_thread: threading.Thread | None = None

    def encode(self, titles, **kwargs):
        self.encode_thread = threading.current_thread()
        return np.tile(np.array([1.0, 0.0]), (len(titles), 1))


class TestClusterArticlesOffLoop:

    async def test_encode_runs_in_worker_thread(self):
        from pipeline.nlp import dedup

        now = datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)
        articles = [
            {"id": 1, "title": "Apple beats earnings", "published_at": now},
            {"id": 2, "title": "Apple tops estimates", "published_at": now},
        ]
        stub = _StubModel()
        set_ids = AsyncMock()

        with (
            patch.object(dedup, "get_unclustered_articles", AsyncMock(return_value=articles)),
            patch.object(dedup, "set_cluster_ids", set_ids),
            patch.object(dedup, "_get_model", return_value=stub),
        ):
            n_clusters = await dedup.cluster_articles("AAPL")

        # Identical unit vectors within the time window → one 2-article cluster
        assert n_clusters == 1
        set_ids.assert_awaited_once()

        # The load-bearing assertion: encode ran OFF the event-loop thread.
        assert stub.encode_thread is not None
        assert stub.encode_thread is not threading.main_thread()
