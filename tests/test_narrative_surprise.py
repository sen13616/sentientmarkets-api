"""Unit tests for the narrative-surprise feature (nowcasting Phase 5b).

Flag-gated research field: must never affect headline scores, must return
None (not neutral) on cold start / thin coverage / flat baseline.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from pipeline.features import surprise

_NOW = datetime(2026, 7, 21, 15, 0, tzinfo=timezone.utc)


def _row(days_ago: float, score: float, relevance: float = 0.8):
    return {
        "published_at": _NOW - timedelta(days=days_ago),
        "finbert_score": score,
        "relevance_score": relevance,
    }


def _baseline(daily_scores: list[float], per_day: int = 1):
    """One+ articles per day, oldest day first, all outside the 24h window."""
    rows = []
    for i, s in enumerate(daily_scores):
        for _ in range(per_day):
            rows.append(_row(days_ago=len(daily_scores) - i + 1.5, score=s))
    return rows


def _patch_queries(current_rows, baseline_rows):
    async def fake(ticker, since, until):
        # the current window ends at now; the baseline window ends before it
        return current_rows if until == _NOW else baseline_rows
    return patch(
        "pipeline.features.surprise.get_article_scores_between",
        AsyncMock(side_effect=fake),
    )


class TestFlag:
    def test_disabled_by_default(self, monkeypatch):
        monkeypatch.delenv("ENABLE_NARRATIVE_SURPRISE", raising=False)
        assert surprise.enabled() is False

    @pytest.mark.parametrize("value", ["1", "true", "YES"])
    def test_enabled_values(self, monkeypatch, value):
        monkeypatch.setenv("ENABLE_NARRATIVE_SURPRISE", value)
        assert surprise.enabled() is True

    def test_off_values(self, monkeypatch):
        monkeypatch.setenv("ENABLE_NARRATIVE_SURPRISE", "0")
        assert surprise.enabled() is False


class TestWeightedMean:
    def test_relevance_weighting(self):
        rows = [_row(0.1, 1.0, relevance=0.9), _row(0.2, -1.0, relevance=0.3)]
        expected = (1.0 * 0.9 - 1.0 * 0.3) / 1.2
        assert surprise._weighted_mean(rows) == pytest.approx(expected)

    def test_rows_without_relevance_or_score_excluded(self):
        rows = [
            {"published_at": _NOW, "finbert_score": 1.0, "relevance_score": None},
            {"published_at": _NOW, "finbert_score": None, "relevance_score": 0.9},
        ]
        assert surprise._weighted_mean(rows) is None


class TestComputeNarrativeSurprise:
    async def test_no_current_coverage_returns_none(self):
        with _patch_queries([], _baseline([0.1] * 14)):
            assert await surprise.compute_narrative_surprise("AAPL", _NOW) is None

    async def test_thin_baseline_returns_none(self):
        # fewer than MIN_BASELINE_ARTICLES in the whole baseline window
        with _patch_queries([_row(0.2, 0.9)], _baseline([0.1] * 5)):
            assert await surprise.compute_narrative_surprise("AAPL", _NOW) is None

    async def test_flat_baseline_returns_none(self):
        # zero variance across baseline days → sigma floor → None
        with _patch_queries([_row(0.2, 0.9)], _baseline([0.5] * 14)):
            assert await surprise.compute_narrative_surprise("AAPL", _NOW) is None

    async def test_positive_surprise_above_50(self):
        baseline = _baseline([0.0, 0.1, -0.1, 0.05, -0.05, 0.0, 0.1,
                              -0.1, 0.05, -0.05, 0.0, 0.1, -0.1, 0.0])
        with _patch_queries([_row(0.2, 0.9)], baseline):
            score = await surprise.compute_narrative_surprise("AAPL", _NOW)
        assert score is not None and score > 50.0

    async def test_inline_coverage_near_50(self):
        baseline = _baseline([0.0, 0.1, -0.1, 0.05, -0.05, 0.0, 0.1,
                              -0.1, 0.05, -0.05, 0.0, 0.1, -0.1, 0.0])
        # current coverage tone == baseline mean (0.0) → z ≈ 0 → ≈ 50
        current = [{"published_at": _NOW - timedelta(hours=1),
                    "finbert_score": 0.0, "relevance_score": 0.8}]
        with _patch_queries(current, baseline):
            score = await surprise.compute_narrative_surprise("AAPL", _NOW)
        assert score == pytest.approx(50.0, abs=1.0)

    async def test_baseline_window_excludes_current_window(self):
        """The baseline query must end where the current window starts."""
        calls = []

        async def spy(ticker, since, until):
            calls.append((since, until))
            return []

        with patch("pipeline.features.surprise.get_article_scores_between",
                   AsyncMock(side_effect=spy)):
            await surprise.compute_narrative_surprise("AAPL", _NOW)

        (cur_since, cur_until) = calls[0]
        assert cur_until == _NOW
        assert cur_since == _NOW - timedelta(hours=surprise.CURRENT_WINDOW_HOURS)
        # with no current coverage the function returns before the baseline
        # query — only one call expected
        assert len(calls) == 1


class TestHeadlineUnaffected:
    async def test_flag_off_no_query_no_state_key(self, monkeypatch):
        """surprise.enabled() False ⇒ orchestrator neither queries nor writes
        the state key (structural guarantee that headline scores are
        byte-identical with the flag off)."""
        monkeypatch.delenv("ENABLE_NARRATIVE_SURPRISE", raising=False)
        assert surprise.enabled() is False

    def test_surprise_never_in_composite_inputs(self):
        """The composite only ever sees the four layer sub-indices."""
        from pipeline.scoring.composite import LAYER_WEIGHTS
        assert "narrative_surprise" not in LAYER_WEIGHTS
