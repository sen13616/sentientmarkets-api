"""tests/test_influencer_lookback.py

Regression guard: influencer scoring must fetch a 30-day window, not the
3-day layer-staleness window. Insider rows are dated by transaction date and
stay meaningful for 30 days; a 3-day fetch silently drops them, leaving the
influencer sub-index analyst-only.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

from pipeline import orchestrator
from pipeline.orchestrator import _INFLUENCER_SCORE_LOOKBACK, _score_influencer


def test_influencer_score_lookback_is_30_days():
    assert _INFLUENCER_SCORE_LOOKBACK == timedelta(days=30)


async def test_score_influencer_fetches_30_day_window():
    """_score_influencer must query get_signals_since with a ~30-day cutoff."""
    now = datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)

    with (
        patch.object(orchestrator, "get_signals_since", new_callable=AsyncMock, return_value=[]) as mock_fetch,
        patch.object(orchestrator, "_fallback_subindex", return_value=(None, None)),
    ):
        await _score_influencer("AAPL", now, last_state=None, current_price=100.0)

    mock_fetch.assert_awaited_once()
    since_arg = mock_fetch.call_args.args[1]
    assert since_arg == now - timedelta(days=30)
    # Guard against regressing to the 3-day staleness window.
    assert since_arg <= now - timedelta(days=29)
