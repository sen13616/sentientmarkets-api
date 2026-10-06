"""
tests/test_macro_unit.py — Unit tests for VIX source attribution & log format.

Covers:
  1. Tuple unpacking when Finnhub succeeds
  2. Tuple unpacking when Finnhub fails and AV succeeds (source="alpha_vantage")
  3. Both VIX fetchers fail → no VIX row
  4. Log format regression — _run_macro completes without ValueError
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, patch

import httpx

# ---------------------------------------------------------------------------
# 1. Finnhub succeeds → source = "finnhub"
# ---------------------------------------------------------------------------


async def test_vix_finnhub_success_returns_tuple():
    """_vix_finnhub returns (value, 'finnhub') on success."""
    from pipeline.sources.macro import _vix_finnhub

    mock_resp = AsyncMock(spec=httpx.Response)
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"c": 18.5}

    client = AsyncMock(spec=httpx.AsyncClient)

    with patch("pipeline.sources.macro.guarded_get", return_value=mock_resp):
        result = await _vix_finnhub(client)

    assert result is not None
    value, source = result
    assert value == 18.5
    assert source == "finnhub"


# ---------------------------------------------------------------------------
# 2. Finnhub fails, AV succeeds → source = "alpha_vantage"
# ---------------------------------------------------------------------------


async def test_vix_av_fallback_returns_alpha_vantage_source():
    """When yfinance and Finnhub fail and AV succeeds, VIX row source is 'alpha_vantage'."""
    from pipeline.sources.macro import _run_macro

    mock_av_resp = AsyncMock(spec=httpx.Response)
    mock_av_resp.status_code = 200
    mock_av_resp.json.return_value = {
        "Global Quote": {"05. price": "22.10"},
    }

    async def fake_guarded_get(client, url, *, params=None, sem=None, delay=None, label=""):
        if "finnhub" in url:
            return None  # Finnhub fails
        return mock_av_resp  # AV succeeds

    client = AsyncMock(spec=httpx.AsyncClient)

    with (
        patch("pipeline.sources.macro._vix_yfinance", new_callable=AsyncMock, return_value=None),
        patch("pipeline.sources.macro.guarded_get", side_effect=fake_guarded_get),
        patch("pipeline.sources.macro.insert_signals", new_callable=AsyncMock) as mock_insert,
        patch("pipeline.sources.macro.get_close_history", new_callable=AsyncMock, return_value=[]),
        patch("pipeline.sources.macro.SECTOR_ETFS", {}),  # skip ETFs to isolate VIX
    ):
        await _run_macro(client)

    assert mock_insert.called
    rows = mock_insert.call_args[0][0]
    vix_rows = [r for r in rows if r[1] == "vix"]
    assert len(vix_rows) == 1
    assert vix_rows[0][3] == "alpha_vantage"


# ---------------------------------------------------------------------------
# 3. Both fail → no VIX row
# ---------------------------------------------------------------------------


async def test_vix_all_fail_no_row():
    """When yfinance, Finnhub, and AV all fail, no VIX row is produced."""
    from pipeline.sources.macro import _run_macro

    client = AsyncMock(spec=httpx.AsyncClient)

    with (
        patch("pipeline.sources.macro._vix_yfinance", new_callable=AsyncMock, return_value=None),
        patch("pipeline.sources.macro.guarded_get", return_value=None),
        patch("pipeline.sources.macro.insert_signals", new_callable=AsyncMock) as mock_insert,
        patch("pipeline.sources.macro.SECTOR_ETFS", {}),
    ):
        await _run_macro(client)

    assert mock_insert.called
    rows = mock_insert.call_args[0][0]
    vix_rows = [r for r in rows if r[1] == "vix"]
    assert len(vix_rows) == 0


# ---------------------------------------------------------------------------
# 4. Log format regression — _run_macro with ETF 20d return doesn't crash
# ---------------------------------------------------------------------------


async def test_log_format_no_valueerror(caplog):
    """The ETF 20d-return log line must not raise ValueError from malformed format."""
    from datetime import datetime, timezone

    from pipeline.sources.macro import _run_macro

    now = datetime.now(timezone.utc)
    # Build 21 close history entries so _compute_etf_return_20d returns a value
    close_history = [(now, 100.0 + i) for i in range(21)]

    mock_etf_resp = AsyncMock(spec=httpx.Response)
    mock_etf_resp.status_code = 200
    mock_etf_resp.json.return_value = {
        "Global Quote": {
            "05. price": "120.0",
            "07. latest trading day": "2026-05-01",
        },
    }

    async def fake_guarded_get(client, url, *, params=None, sem=None, delay=None, label=""):
        if "finnhub" in url:
            return None
        return mock_etf_resp

    client = AsyncMock(spec=httpx.AsyncClient)

    with (
        patch("pipeline.sources.macro.guarded_get", side_effect=fake_guarded_get),
        patch("pipeline.sources.macro.insert_signals", new_callable=AsyncMock),
        patch(
            "pipeline.sources.macro.get_close_history",
            new_callable=AsyncMock,
            return_value=close_history,
        ),
        patch("pipeline.sources.macro.SECTOR_ETFS", {"Test Sector": "XTT"}),
        caplog.at_level(logging.DEBUG, logger="pipeline.sources.macro"),
    ):
        # This must NOT raise ValueError from a malformed format string
        await _run_macro(client)

    # Verify the 20d_return log was emitted
    assert any("20d_return=" in msg for msg in caplog.messages)
