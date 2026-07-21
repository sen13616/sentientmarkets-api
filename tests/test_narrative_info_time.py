"""Guard tests: narrative signals are stamped with INFORMATION time
(provider publication time), never fetch/scoring time (nowcasting Phase 5a).

A score can only ever lead price if its inputs are stamped when the news
broke; silently defaulting a bad timestamp to now() would fake freshness
and make ingestion latency unmeasurable.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from pipeline.orchestrator import _latest_ts
from pipeline.sources.narrative import _fetch_av_news, _fetch_finnhub_news, _parse_av_time


class TestParseAvTime:
    def test_valid_av_timestamp(self):
        ts = _parse_av_time("20260115T163000")
        assert ts == datetime(2026, 1, 15, 16, 30, tzinfo=timezone.utc)

    def test_unparseable_returns_none_never_now(self):
        for bad in ("", "garbage", "2026-01-15 16:30:00", "20260115"):
            assert _parse_av_time(bad) is None


def _mock_response(body):
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = body
    return resp


class TestAvArticleSkipping:
    async def test_bad_time_published_skips_article(self):
        body = {"feed": [
            {"url": "https://a.example/1", "time_published": "20260115T163000",
             "title": "good", "ticker_sentiment": []},
            {"url": "https://a.example/2", "time_published": "not-a-time",
             "title": "bad ts", "ticker_sentiment": []},
            {"url": "https://a.example/3", "time_published": "",
             "title": "empty ts", "ticker_sentiment": []},
        ]}
        with patch("pipeline.sources.narrative.guarded_get",
                   AsyncMock(return_value=_mock_response(body))):
            articles = await _fetch_av_news("AAPL", MagicMock())
        assert len(articles) == 1
        assert articles[0]["published_at"] == datetime(2026, 1, 15, 16, 30, tzinfo=timezone.utc)


class TestFinnhubArticleSkipping:
    async def test_bad_unix_ts_skips_article(self):
        body = [
            {"url": "https://f.example/1", "datetime": 1768494600,
             "headline": "good"},
            {"url": "https://f.example/2", "datetime": None, "headline": "no ts"},
            {"url": "https://f.example/3", "datetime": "garbage", "headline": "bad ts"},
        ]
        with patch("pipeline.sources.narrative.guarded_get",
                   AsyncMock(return_value=_mock_response(body))):
            articles = await _fetch_finnhub_news("AAPL", MagicMock())
        assert len(articles) == 1
        assert articles[0]["published_at"] == datetime.fromtimestamp(
            1768494600, tz=timezone.utc
        )


class TestNarrativeAsOfIsInformationTime:
    def test_latest_ts_uses_published_at_key(self):
        """narrative_as_of = max(published_at) of scored articles — the
        information time — not the fetch/scoring time."""
        older = datetime(2026, 1, 15, 12, 0, tzinfo=timezone.utc)
        newer = datetime(2026, 1, 15, 18, 0, tzinfo=timezone.utc)
        rows = [{"published_at": older}, {"published_at": newer},
                {"published_at": None}]
        assert _latest_ts(rows, key="published_at") == newer

    def test_latest_ts_empty_is_none(self):
        assert _latest_ts([], key="published_at") is None
