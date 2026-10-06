"""
tests/test_short_volume.py

Unit tests for FINRA REGSHO daily short volume parsing and fetching.

All tests are pure and synchronous or use mocked HTTP — no DB, Redis, or
real network calls required.
"""
from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, patch

import pytest

from pipeline.sources.short_volume import (
    _combine_trf_data,
    _parse_short_volume,
    _url_for,
    fetch_short_volume_for_date,
    latest_short_volume,
)

# ---------------------------------------------------------------------------
# Sample FINRA file content
# ---------------------------------------------------------------------------

_CNMS_SAMPLE = """\
Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market
20260501|AAPL|12345678|100000|25000000|CNMS
20260501|MSFT|8000000|50000|18000000|CNMS
20260501|GOOG|5000000|30000|12000000|CNMS
20260501|ZZZZZ|1000|10|5000|CNMS
Total|4 records
"""

_TRF_FNYX_SAMPLE = """\
Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market
20260501|AAPL|4000000|30000|8000000|FNYX
20260501|MSFT|3000000|20000|7000000|FNYX
Total|2 records
"""

_TRF_FNSQ_SAMPLE = """\
Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market
20260501|AAPL|5000000|40000|9000000|FNSQ
20260501|MSFT|3500000|15000|6000000|FNSQ
20260501|GOOG|5000000|30000|12000000|FNSQ
Total|3 records
"""

_TRF_FNQC_SAMPLE = """\
Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market
20260501|AAPL|3345678|30000|8000000|FNQC
Total|1 records
"""


# ---------------------------------------------------------------------------
# (a) Valid CNMS file parses correctly
# ---------------------------------------------------------------------------

class TestParsing:
    """Test _parse_short_volume with valid CNMS data."""

    def test_parses_all_tickers(self):
        data = _parse_short_volume(_CNMS_SAMPLE)
        assert len(data) == 4
        assert "AAPL" in data
        assert "MSFT" in data
        assert "GOOG" in data
        assert "ZZZZZ" in data

    def test_short_volume_values(self):
        data = _parse_short_volume(_CNMS_SAMPLE)
        assert data["AAPL"]["short_volume"] == 12345678
        assert data["AAPL"]["total_volume"] == 25000000

    def test_msft_values(self):
        data = _parse_short_volume(_CNMS_SAMPLE)
        assert data["MSFT"]["short_volume"] == 8000000
        assert data["MSFT"]["total_volume"] == 18000000

    def test_header_line_skipped(self):
        """The header row 'Date|Symbol|...' should not appear in output."""
        data = _parse_short_volume(_CNMS_SAMPLE)
        assert "Date" not in data
        assert "Symbol" not in data

    def test_trailer_line_skipped(self):
        """The trailer row 'Total|4 records' should not appear in output."""
        data = _parse_short_volume(_CNMS_SAMPLE)
        assert "Total" not in data

    def test_empty_input(self):
        assert _parse_short_volume("") == {}

    def test_header_only(self):
        assert _parse_short_volume("Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n") == {}

    def test_zero_total_volume_skipped(self):
        text = "20260501|BADTK|100|0|0|CNMS\n"
        data = _parse_short_volume(text)
        assert "BADTK" not in data

    def test_malformed_line_skipped(self):
        text = "20260501|AAPL|abc|0|xyz|CNMS\n"
        data = _parse_short_volume(text)
        assert len(data) == 0

    def test_short_line_skipped(self):
        """Lines with fewer than 5 pipe-separated fields are skipped."""
        text = "20260501|AAPL|100\n"
        data = _parse_short_volume(text)
        assert len(data) == 0


# ---------------------------------------------------------------------------
# TRF combination
# ---------------------------------------------------------------------------

class TestCombineTRF:
    """Test _combine_trf_data merges volumes correctly."""

    def test_sums_across_trfs(self):
        d1 = _parse_short_volume(_TRF_FNYX_SAMPLE)
        d2 = _parse_short_volume(_TRF_FNSQ_SAMPLE)
        d3 = _parse_short_volume(_TRF_FNQC_SAMPLE)
        combined = _combine_trf_data([d1, d2, d3])

        # AAPL appears in all 3
        assert combined["AAPL"]["short_volume"] == 4000000 + 5000000 + 3345678
        assert combined["AAPL"]["total_volume"] == 8000000 + 9000000 + 8000000

    def test_ticker_in_subset_of_trfs(self):
        d1 = _parse_short_volume(_TRF_FNYX_SAMPLE)
        d2 = _parse_short_volume(_TRF_FNSQ_SAMPLE)
        combined = _combine_trf_data([d1, d2])

        # GOOG only in FNSQ
        assert combined["GOOG"]["short_volume"] == 5000000
        assert combined["GOOG"]["total_volume"] == 12000000

    def test_empty_datasets(self):
        assert _combine_trf_data([]) == {}
        assert _combine_trf_data([{}]) == {}


# ---------------------------------------------------------------------------
# URL construction
# ---------------------------------------------------------------------------

class TestURLBuilder:

    def test_cnms_url(self):
        url = _url_for("CNMSshvol", date(2026, 5, 1))
        assert url == "https://cdn.finra.org/equity/regsho/daily/CNMSshvol20260501.txt"

    def test_trf_url(self):
        url = _url_for("FNYXshvol", date(2026, 4, 30))
        assert url == "https://cdn.finra.org/equity/regsho/daily/FNYXshvol20260430.txt"


# ---------------------------------------------------------------------------
# (b) Date fallback when today's file is 404
# ---------------------------------------------------------------------------

class TestDateFallback:
    """latest_short_volume walks back up to 4 days on 404."""

    @pytest.mark.asyncio
    async def test_fallback_to_previous_day(self):
        """Friday file missing → falls back to Thursday."""

        async def mock_fetch(target_date, client):
            if target_date == date(2026, 5, 1):  # Friday → 404
                return None
            if target_date == date(2026, 4, 30):  # Thursday → success
                return _parse_short_volume(_CNMS_SAMPLE)
            return None

        with patch(
            "pipeline.sources.short_volume.fetch_short_volume_for_date",
            side_effect=mock_fetch,
        ):
            client = AsyncMock()
            result = await latest_short_volume(client, ref_date=date(2026, 5, 1))
            assert result is not None
            data, actual_date = result
            assert actual_date == date(2026, 4, 30)
            assert "AAPL" in data

    @pytest.mark.asyncio
    async def test_skips_weekends(self):
        """Sunday ref_date → skips Sat/Sun, tries Friday."""
        calls = []

        async def mock_fetch(target_date, client):
            calls.append(target_date)
            if target_date == date(2026, 4, 24):  # Friday
                return _parse_short_volume(_CNMS_SAMPLE)
            return None

        with patch(
            "pipeline.sources.short_volume.fetch_short_volume_for_date",
            side_effect=mock_fetch,
        ):
            client = AsyncMock()
            result = await latest_short_volume(client, ref_date=date(2026, 4, 26))  # Sunday
            assert result is not None
            data, actual_date = result
            assert actual_date == date(2026, 4, 24)
            # Saturday and Sunday should not have been tried
            assert date(2026, 4, 25) not in calls  # Saturday
            assert date(2026, 4, 26) not in calls  # Sunday

    @pytest.mark.asyncio
    async def test_all_days_fail_returns_none(self):
        """All 5 days fail → returns None."""

        async def mock_fetch(target_date, client):
            return None

        with patch(
            "pipeline.sources.short_volume.fetch_short_volume_for_date",
            side_effect=mock_fetch,
        ):
            client = AsyncMock()
            result = await latest_short_volume(client, ref_date=date(2026, 4, 29))  # Wednesday
            assert result is None


# ---------------------------------------------------------------------------
# (c) CNMS fail → TRF fallback
# ---------------------------------------------------------------------------

class TestCNMSToTRFFallback:
    """When CNMS returns 404, individual TRF files are fetched and combined."""

    @pytest.mark.asyncio
    async def test_trf_fallback_on_cnms_404(self):
        """CNMS 404 → fetch FNYX + FNSQ + FNQC and combine."""
        trf_responses = {
            "FNYXshvol": _TRF_FNYX_SAMPLE,
            "FNSQshvol": _TRF_FNSQ_SAMPLE,
            "FNQCshvol": _TRF_FNQC_SAMPLE,
        }

        async def mock_get(url):
            resp = AsyncMock()
            if "CNMSshvol" in url:
                resp.status_code = 404
                resp.text = ""
                return resp
            for market, content in trf_responses.items():
                if market in url:
                    resp.status_code = 200
                    resp.text = content
                    return resp
            resp.status_code = 404
            resp.text = ""
            return resp

        client = AsyncMock()
        client.get = mock_get

        data = await fetch_short_volume_for_date(date(2026, 5, 1), client)
        assert data is not None
        # AAPL appears in all 3 TRFs
        assert data["AAPL"]["short_volume"] == 4000000 + 5000000 + 3345678
        # MSFT in FNYX + FNSQ
        assert data["MSFT"]["short_volume"] == 3000000 + 3500000
        # GOOG only in FNSQ
        assert data["GOOG"]["short_volume"] == 5000000

    @pytest.mark.asyncio
    async def test_cnms_success_no_trf_fetch(self):
        """When CNMS succeeds, no TRF files should be fetched."""
        urls_fetched = []

        async def mock_get(url):
            urls_fetched.append(url)
            resp = AsyncMock()
            if "CNMSshvol" in url:
                resp.status_code = 200
                resp.text = _CNMS_SAMPLE
            else:
                resp.status_code = 200
                resp.text = ""
            return resp

        client = AsyncMock()
        client.get = mock_get

        data = await fetch_short_volume_for_date(date(2026, 5, 1), client)
        assert data is not None
        # Only CNMS URL should have been fetched
        assert len(urls_fetched) == 1
        assert "CNMS" in urls_fetched[0]


# ---------------------------------------------------------------------------
# (d) Off-universe tickers filtered out (ingest_short_volume)
# ---------------------------------------------------------------------------

class TestUniverseFiltering:
    """ingest_short_volume only writes tickers in the active universe."""

    @pytest.mark.asyncio
    async def test_filters_to_universe(self):
        """Only AAPL and MSFT are in universe; GOOG and ZZZZZ are excluded."""
        written_rows: list[tuple] = []

        async def mock_insert(rows):
            written_rows.extend(rows)

        async def mock_tickers():
            return ["AAPL", "MSFT"]

        async def mock_latest(client, ref_date=None):
            return _parse_short_volume(_CNMS_SAMPLE), date(2026, 5, 1)

        with (
            patch("pipeline.sources.short_volume.insert_signals", side_effect=mock_insert),
            patch("pipeline.sources.short_volume.get_active_tickers", side_effect=mock_tickers),
            patch("pipeline.sources.short_volume.latest_short_volume", side_effect=mock_latest),
        ):
            from pipeline.sources.short_volume import ingest_short_volume
            client = AsyncMock()
            n = await ingest_short_volume(client)

        assert n == 2  # AAPL + MSFT
        # 3 signals per ticker
        assert len(written_rows) == 6

        tickers_written = {r[0] for r in written_rows}
        assert tickers_written == {"AAPL", "MSFT"}
        assert "GOOG" not in tickers_written
        assert "ZZZZZ" not in tickers_written

    @pytest.mark.asyncio
    async def test_signal_types_correct(self):
        """All 3 signal types are written per ticker."""
        written_rows: list[tuple] = []

        async def mock_insert(rows):
            written_rows.extend(rows)

        async def mock_tickers():
            return ["AAPL"]

        async def mock_latest(client, ref_date=None):
            return _parse_short_volume(_CNMS_SAMPLE), date(2026, 5, 1)

        with (
            patch("pipeline.sources.short_volume.insert_signals", side_effect=mock_insert),
            patch("pipeline.sources.short_volume.get_active_tickers", side_effect=mock_tickers),
            patch("pipeline.sources.short_volume.latest_short_volume", side_effect=mock_latest),
        ):
            from pipeline.sources.short_volume import ingest_short_volume
            client = AsyncMock()
            await ingest_short_volume(client)

        signal_types = {r[1] for r in written_rows}
        assert signal_types == {"short_volume_otc", "short_volume_total_otc", "short_volume_ratio_otc"}

    @pytest.mark.asyncio
    async def test_source_is_finra_regsho(self):
        """All signals use source='finra_regsho'."""
        written_rows: list[tuple] = []

        async def mock_insert(rows):
            written_rows.extend(rows)

        async def mock_tickers():
            return ["AAPL"]

        async def mock_latest(client, ref_date=None):
            return _parse_short_volume(_CNMS_SAMPLE), date(2026, 5, 1)

        with (
            patch("pipeline.sources.short_volume.insert_signals", side_effect=mock_insert),
            patch("pipeline.sources.short_volume.get_active_tickers", side_effect=mock_tickers),
            patch("pipeline.sources.short_volume.latest_short_volume", side_effect=mock_latest),
        ):
            from pipeline.sources.short_volume import ingest_short_volume
            client = AsyncMock()
            await ingest_short_volume(client)

        for row in written_rows:
            assert row[3] == "finra_regsho"
            assert row[4] == "live"

    @pytest.mark.asyncio
    async def test_ratio_calculation(self):
        """short_volume_ratio_otc = short / total."""
        written_rows: list[tuple] = []

        async def mock_insert(rows):
            written_rows.extend(rows)

        async def mock_tickers():
            return ["AAPL"]

        async def mock_latest(client, ref_date=None):
            return _parse_short_volume(_CNMS_SAMPLE), date(2026, 5, 1)

        with (
            patch("pipeline.sources.short_volume.insert_signals", side_effect=mock_insert),
            patch("pipeline.sources.short_volume.get_active_tickers", side_effect=mock_tickers),
            patch("pipeline.sources.short_volume.latest_short_volume", side_effect=mock_latest),
        ):
            from pipeline.sources.short_volume import ingest_short_volume
            client = AsyncMock()
            await ingest_short_volume(client)

        ratio_row = [r for r in written_rows if r[1] == "short_volume_ratio_otc"][0]
        expected = 12345678 / 25000000
        assert abs(ratio_row[2] - expected) < 1e-9

    @pytest.mark.asyncio
    async def test_no_data_returns_zero(self):
        """When latest_short_volume returns None, ingest returns 0."""
        async def mock_tickers():
            return ["AAPL"]

        async def mock_latest(client, ref_date=None):
            return None

        with (
            patch("pipeline.sources.short_volume.get_active_tickers", side_effect=mock_tickers),
            patch("pipeline.sources.short_volume.latest_short_volume", side_effect=mock_latest),
        ):
            from pipeline.sources.short_volume import ingest_short_volume
            client = AsyncMock()
            n = await ingest_short_volume(client)

        assert n == 0
