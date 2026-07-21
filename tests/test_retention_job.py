"""tests/test_retention_job.py — Unit tests for the data retention job.

Verifies:
  - retention_job calls the three purge queries with the correct cutoff dates
    and signal-type filters.
  - The OHLCV constant covers every yf_*/ohlcv_* signal type used by the
    market layer.
  - The retention job is registered with the scheduler on a daily cron.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest


def test_ohlcv_signal_types_cover_market_layer():
    """OHLCV_SIGNAL_TYPES must include every yf_* and ohlcv_* type written by the pipeline."""
    from scripts.db.queries.raw_signals import OHLCV_SIGNAL_TYPES

    expected = {
        "yf_open", "yf_high", "yf_low", "yf_close", "yf_volume",
        "ohlcv_open", "ohlcv_high", "ohlcv_low", "ohlcv_close",
        "ohlcv_adjusted_close", "ohlcv_volume",
    }
    assert set(OHLCV_SIGNAL_TYPES) == expected


def test_retention_constants():
    """Retention tiers: 365 OHLCV / 45 derived / 14 quotes / 90 catch-all /
    365 articles (text stripped at 30) / 30 driver compaction."""
    from pipeline.scheduler import (
        ARTICLE_RETENTION_DAYS,
        ARTICLE_TEXT_COMPACT_DAYS,
        DERIVED_RETENTION_DAYS,
        DRIVER_COMPACT_DAYS,
        OHLCV_RETENTION_DAYS,
        QUOTE_RETENTION_DAYS,
        SIGNAL_RETENTION_DAYS,
    )

    assert OHLCV_RETENTION_DAYS   == 365
    assert SIGNAL_RETENTION_DAYS  == 90
    assert DERIVED_RETENTION_DAYS == 45
    assert QUOTE_RETENTION_DAYS   == 14
    assert ARTICLE_RETENTION_DAYS == 365
    assert ARTICLE_TEXT_COMPACT_DAYS == 30
    assert DRIVER_COMPACT_DAYS    == 30


def test_tier_signal_type_lists():
    """Tier lists must cover exactly the intended types (guards against drift)."""
    from scripts.db.queries.raw_signals import (
        DERIVED_INTRADAY_SIGNAL_TYPES,
        OHLCV_SIGNAL_TYPES,
        QUOTE_SIGNAL_TYPES,
    )

    assert set(DERIVED_INTRADAY_SIGNAL_TYPES) == {
        "rsi_14", "return_1d", "return_5d", "return_20d",
        "volume_ratio",
        "order_flow_imbalance", "buy_pressure", "sell_pressure",
        "bid_ask_spread_bps",
    }
    assert set(QUOTE_SIGNAL_TYPES) == {"bid", "ask", "bid_ask_spread"}
    # Tiers must be disjoint from each other and from OHLCV
    assert not set(DERIVED_INTRADAY_SIGNAL_TYPES) & set(QUOTE_SIGNAL_TYPES)
    assert not set(DERIVED_INTRADAY_SIGNAL_TYPES) & set(OHLCV_SIGNAL_TYPES)
    assert not set(QUOTE_SIGNAL_TYPES) & set(OHLCV_SIGNAL_TYPES)


def test_research_retain_signal_types():
    """Research-retained types (never purged) cover FINRA short volume + insider,
    and are disjoint from every purge tier."""
    from scripts.db.queries.raw_signals import (
        DERIVED_INTRADAY_SIGNAL_TYPES,
        OHLCV_SIGNAL_TYPES,
        QUOTE_SIGNAL_TYPES,
        RESEARCH_RETAIN_SIGNAL_TYPES,
    )

    assert set(RESEARCH_RETAIN_SIGNAL_TYPES) == {
        "short_volume_otc", "short_volume_total_otc", "short_volume_ratio_otc",
        "insider_net_shares",
    }
    for tier in (OHLCV_SIGNAL_TYPES, DERIVED_INTRADAY_SIGNAL_TYPES, QUOTE_SIGNAL_TYPES):
        assert not set(RESEARCH_RETAIN_SIGNAL_TYPES) & set(tier)


def test_retention_job_registered():
    """retention job must be registered with daily 03:30 UTC cron."""
    from pipeline.scheduler import scheduler

    job = scheduler.get_job("retention")
    assert job is not None, "retention job not found in scheduler"

    fields = {f.name: str(f) for f in job.trigger.fields}
    assert fields["hour"]   == "3"
    assert fields["minute"] == "30"


async def test_retention_job_calls_purges_with_correct_cutoffs():
    """retention_job calls the four tiered purges with cutoffs derived from now()."""
    from pipeline.scheduler import (
        ARTICLE_RETENTION_DAYS,
        DERIVED_RETENTION_DAYS,
        OHLCV_RETENTION_DAYS,
        QUOTE_RETENTION_DAYS,
        SIGNAL_RETENTION_DAYS,
    )
    from scripts.db.queries.raw_signals import (
        DERIVED_INTRADAY_SIGNAL_TYPES,
        OHLCV_SIGNAL_TYPES,
        QUOTE_SIGNAL_TYPES,
    )

    fixed_now = datetime(2026, 5, 16, 12, 0, tzinfo=timezone.utc)

    with (
        patch("pipeline.scheduler.purge_signals_before", new_callable=AsyncMock, return_value=0) as mock_signals,
        patch("pipeline.scheduler.purge_articles_before", new_callable=AsyncMock, return_value=0) as mock_articles,
        patch("pipeline.scheduler.strip_article_text_before", new_callable=AsyncMock, return_value=0) as mock_strip,
        patch("pipeline.scheduler.compact_drivers_before", new_callable=AsyncMock, return_value=0) as mock_compact,
        patch("pipeline.scheduler._record_run", new_callable=AsyncMock),
        patch("pipeline.scheduler.datetime") as mock_dt,
    ):
        mock_dt.now.return_value = fixed_now
        # Make timedelta usable through the patched module
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        from pipeline.scheduler import retention_job
        await retention_job()

    # Four signal purges: OHLCV, derived intraday, quotes (include) + catch-all (exclude)
    assert mock_signals.call_count == 4

    ohlcv_call, derived_call, quote_call, other_call = mock_signals.call_args_list

    # OHLCV purge: cutoff at -365d, OHLCV list, exclude default False
    assert ohlcv_call.args[0] == fixed_now - timedelta(days=OHLCV_RETENTION_DAYS)
    assert ohlcv_call.args[1] == OHLCV_SIGNAL_TYPES
    assert ohlcv_call.kwargs.get("exclude", False) is False

    # Derived intraday purge: cutoff at -45d
    assert derived_call.args[0] == fixed_now - timedelta(days=DERIVED_RETENTION_DAYS)
    assert derived_call.args[1] == DERIVED_INTRADAY_SIGNAL_TYPES
    assert derived_call.kwargs.get("exclude", False) is False

    # Quote purge: cutoff at -14d
    assert quote_call.args[0] == fixed_now - timedelta(days=QUOTE_RETENTION_DAYS)
    assert quote_call.args[1] == QUOTE_SIGNAL_TYPES
    assert quote_call.kwargs.get("exclude", False) is False

    # Catch-all purge: cutoff at -90d, excludes ALL tiered lists AND the
    # research-retained types (never purged)
    from scripts.db.queries.raw_signals import RESEARCH_RETAIN_SIGNAL_TYPES
    assert other_call.args[0] == fixed_now - timedelta(days=SIGNAL_RETENTION_DAYS)
    assert other_call.args[1] == (
        OHLCV_SIGNAL_TYPES + DERIVED_INTRADAY_SIGNAL_TYPES
        + QUOTE_SIGNAL_TYPES + RESEARCH_RETAIN_SIGNAL_TYPES
    )
    assert other_call.kwargs["exclude"] is True

    # Articles purge: cutoff at -365d
    mock_articles.assert_called_once()
    assert mock_articles.call_args.args[0] == fixed_now - timedelta(days=ARTICLE_RETENTION_DAYS)

    # Article text strip: cutoff at -30d
    from pipeline.scheduler import ARTICLE_TEXT_COMPACT_DAYS
    mock_strip.assert_called_once()
    assert mock_strip.call_args.args[0] == fixed_now - timedelta(days=ARTICLE_TEXT_COMPACT_DAYS)

    # Driver compaction: cutoff at -30d
    from pipeline.scheduler import DRIVER_COMPACT_DAYS
    mock_compact.assert_called_once()
    assert mock_compact.call_args.args[0] == fixed_now - timedelta(days=DRIVER_COMPACT_DAYS)


async def test_retention_job_swallows_per_purge_failures():
    """A failure in one purge must not abort the others."""
    with (
        patch(
            "pipeline.scheduler.purge_signals_before",
            new_callable=AsyncMock,
            side_effect=[RuntimeError("boom"), 5, 2, 4],  # OHLCV fails, rest succeed
        ) as mock_signals,
        patch(
            "pipeline.scheduler.purge_articles_before",
            new_callable=AsyncMock,
            return_value=7,
        ) as mock_articles,
        patch(
            "pipeline.scheduler.strip_article_text_before",
            new_callable=AsyncMock,
            return_value=11,
        ) as mock_strip,
        patch(
            "pipeline.scheduler.compact_drivers_before",
            new_callable=AsyncMock,
            return_value=3,
        ) as mock_compact,
        patch("pipeline.scheduler._record_run", new_callable=AsyncMock),
    ):
        from pipeline.scheduler import retention_job
        await retention_job()  # must not raise

    assert mock_signals.call_count == 4
    mock_articles.assert_called_once()
    mock_strip.assert_called_once()
    mock_compact.assert_called_once()


async def test_retention_job_swallows_compaction_failure():
    """A failure in driver compaction must not abort the job or the purges."""
    with (
        patch("pipeline.scheduler.purge_signals_before", new_callable=AsyncMock, return_value=0) as mock_signals,
        patch("pipeline.scheduler.purge_articles_before", new_callable=AsyncMock, return_value=0) as mock_articles,
        patch("pipeline.scheduler.strip_article_text_before", new_callable=AsyncMock, return_value=0) as mock_strip,
        patch(
            "pipeline.scheduler.compact_drivers_before",
            new_callable=AsyncMock,
            side_effect=RuntimeError("boom"),
        ) as mock_compact,
        patch("pipeline.scheduler._record_run", new_callable=AsyncMock) as mock_record,
    ):
        from pipeline.scheduler import retention_job
        await retention_job()  # must not raise

    assert mock_signals.call_count == 4
    mock_articles.assert_called_once()
    mock_strip.assert_called_once()
    mock_compact.assert_called_once()
    mock_record.assert_awaited_once()
