"""
tests/test_fred_source.py — Sprint P4.3

Unit tests for `pipeline/sources/fred.py`:
  • SERIES_MAP shape (3 signal types → 3 FRED series IDs)
  • `_fetch_observation` parses normal responses
  • `_fetch_observation` skips FRED's "." (no-data) sentinel
  • `_fetch_observation` handles non-200 / malformed JSON gracefully
  • `fetch_fred_signals` writes one row per series under '_MACRO_'
  • Series partial failure: one fails, other two still write
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from pipeline.sources.fred import SERIES_MAP, _fetch_observation, fetch_fred_signals

# ---------------------------------------------------------------------------
# Shape / mapping invariants
# ---------------------------------------------------------------------------

def test_series_map_contains_three_signal_types():
    assert set(SERIES_MAP.keys()) == {
        "treasury_yield_10y",
        "treasury_yield_2y",
        "ted_spread",
    }


def test_series_map_resolves_to_canonical_fred_ids():
    assert SERIES_MAP["treasury_yield_10y"] == "DGS10"
    assert SERIES_MAP["treasury_yield_2y"]  == "DGS2"
    assert SERIES_MAP["ted_spread"]         == "T10Y2Y"


# ---------------------------------------------------------------------------
# _fetch_observation
# ---------------------------------------------------------------------------

def _mock_response(status: int, body: dict | None = None):
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status
    resp.json = MagicMock(return_value=body or {})
    return resp


class TestFetchObservation:

    @pytest.mark.asyncio
    async def test_parses_normal_response(self):
        body = {
            "observations": [
                {"date": "2026-05-14", "value": "4.32"},
                {"date": "2026-05-13", "value": "4.30"},
            ]
        }
        with (
            patch("pipeline.sources.fred._FRED_KEY", "test-key"),
            patch("pipeline.sources.fred.guarded_get",
                  new=AsyncMock(return_value=_mock_response(200, body))),
        ):
            client = AsyncMock(spec=httpx.AsyncClient)
            result = await _fetch_observation(client, "DGS10")

        assert len(result) == 2
        value, obs_date = result[0]
        assert value == pytest.approx(4.32)
        assert obs_date == datetime(2026, 5, 14, tzinfo=timezone.utc)

    @pytest.mark.asyncio
    async def test_skips_dot_no_data_sentinel(self):
        """FRED uses '.' as the missing-value sentinel (holidays, not-yet-published)."""
        body = {
            "observations": [
                {"date": "2026-05-14", "value": "."},
                {"date": "2026-05-13", "value": "4.30"},
            ]
        }
        with (
            patch("pipeline.sources.fred._FRED_KEY", "test-key"),
            patch("pipeline.sources.fred.guarded_get",
                  new=AsyncMock(return_value=_mock_response(200, body))),
        ):
            client = AsyncMock(spec=httpx.AsyncClient)
            result = await _fetch_observation(client, "DGS10")

        # Only the non-"." observation survives
        assert len(result) == 1
        assert result[0][0] == pytest.approx(4.30)

    @pytest.mark.asyncio
    async def test_returns_empty_on_non_200(self):
        with (
            patch("pipeline.sources.fred._FRED_KEY", "test-key"),
            patch("pipeline.sources.fred.guarded_get",
                  new=AsyncMock(return_value=_mock_response(500, {}))),
        ):
            client = AsyncMock(spec=httpx.AsyncClient)
            assert await _fetch_observation(client, "DGS10") == []

    @pytest.mark.asyncio
    async def test_returns_empty_on_json_parse_error(self):
        broken = MagicMock(spec=httpx.Response)
        broken.status_code = 200
        broken.json = MagicMock(side_effect=ValueError("not JSON"))
        with (
            patch("pipeline.sources.fred._FRED_KEY", "test-key"),
            patch("pipeline.sources.fred.guarded_get",
                  new=AsyncMock(return_value=broken)),
        ):
            client = AsyncMock(spec=httpx.AsyncClient)
            assert await _fetch_observation(client, "DGS10") == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_api_key_unset(self):
        with patch("pipeline.sources.fred._FRED_KEY", ""):
            client = AsyncMock(spec=httpx.AsyncClient)
            assert await _fetch_observation(client, "DGS10") == []


# ---------------------------------------------------------------------------
# fetch_fred_signals — end-to-end
# ---------------------------------------------------------------------------

class TestFetchFredSignals:

    @pytest.mark.asyncio
    async def test_writes_three_rows_under_macro_ticker(self):
        captured: list = []

        async def _capture(rows):
            captured.extend(rows)

        body = {
            "observations": [
                {"date": "2026-05-14", "value": "4.32"},
            ]
        }
        with (
            patch("pipeline.sources.fred._FRED_KEY", "test-key"),
            patch("pipeline.sources.fred.guarded_get",
                  new=AsyncMock(return_value=_mock_response(200, body))),
            patch("pipeline.sources.fred.insert_signals",
                  new=AsyncMock(side_effect=_capture)),
        ):
            client = AsyncMock(spec=httpx.AsyncClient)
            n = await fetch_fred_signals(client)

        assert n == 3
        assert len(captured) == 3
        # All rows tagged _MACRO_ / source='fred'
        for ticker, sig_type, _value, source, upload_type, _ts in captured:
            assert ticker == "_MACRO_"
            assert source == "fred"
            assert upload_type == "live"
            assert sig_type in SERIES_MAP

    @pytest.mark.asyncio
    async def test_partial_failure_still_writes_remaining_rows(self):
        """If one series returns no usable data, the other two still write."""
        captured: list = []

        async def _capture(rows):
            captured.extend(rows)

        # Make DGS10 fail (empty); DGS2 + T10Y2Y succeed
        async def _selective_get(client, url, **kwargs):
            label = kwargs.get("label", "")
            if "DGS10" in label:
                return _mock_response(500, {})
            body = {"observations": [{"date": "2026-05-14", "value": "3.50"}]}
            return _mock_response(200, body)

        with (
            patch("pipeline.sources.fred._FRED_KEY", "test-key"),
            patch("pipeline.sources.fred.guarded_get", new=_selective_get),
            patch("pipeline.sources.fred.insert_signals",
                  new=AsyncMock(side_effect=_capture)),
        ):
            client = AsyncMock(spec=httpx.AsyncClient)
            n = await fetch_fred_signals(client)

        assert n == 2
        written_types = {r[1] for r in captured}
        assert "treasury_yield_10y" not in written_types
        assert "treasury_yield_2y"  in written_types
        assert "ted_spread"         in written_types
