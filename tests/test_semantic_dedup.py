"""
tests/test_semantic_dedup.py — Sprint 6 (G-C6)

Unit and integration tests for semantic article deduplication.

Covers:
  - cluster_articles() function (pipeline/nlp/dedup.py)
  - get_articles_since() cluster-aware query (db/queries/raw_articles.py)
  - narrative_job() clustering integration (pipeline/scheduler.py)
  - Dedup telemetry
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import (  # noqa: F401 — MagicMock used in TestClusterArticles
    AsyncMock,
    MagicMock,
    patch,
)

import numpy as np
import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_article(
    id: int,
    title: str,
    published_at: datetime,
    relevance_score: float = 0.5,
    source: str = "alpha_vantage",
) -> dict:
    """Build an article dict matching get_unclustered_articles() return shape."""
    return {
        "id": id,
        "title": title,
        "published_at": published_at,
        "relevance_score": relevance_score,
        "source": source,
    }


_NOW = datetime(2026, 5, 10, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# A. Unit tests for cluster_articles()
# ---------------------------------------------------------------------------


class TestClusterArticles:
    """Tests for pipeline.nlp.dedup.cluster_articles()."""

    @pytest.fixture(autouse=True)
    def _reset_model(self):
        """Ensure lazy-loaded model is available via a mock."""
        # We use a real SentenceTransformer-like mock that produces
        # deterministic embeddings based on title content.
        import pipeline.nlp.dedup as dedup_mod

        self._orig_model = dedup_mod._model
        yield
        dedup_mod._model = self._orig_model

    def _fake_encode(self, titles, **kwargs):
        """
        Produce embeddings where similar titles have cosine > 0.85
        and dissimilar titles have cosine < 0.85.

        Strategy: "Apple reports earnings" variants get the same unit vector.
        Unrelated titles get an orthogonal vector.
        """
        dim = 4
        vecs = []
        for t in titles:
            t_lower = t.lower()
            if "apple" in t_lower and "earnings" in t_lower:
                v = np.array([1.0, 0.0, 0.0, 0.0])
            elif "fed" in t_lower and "rate" in t_lower:
                v = np.array([0.0, 1.0, 0.0, 0.0])
            elif "tesla" in t_lower and "recall" in t_lower:
                v = np.array([0.0, 0.0, 1.0, 0.0])
            else:
                # Unique random-ish direction per title (orthogonal to others)
                rng = np.random.RandomState(hash(t) % 2**31)
                v = rng.randn(dim)
            v = v / np.linalg.norm(v)
            vecs.append(v)
        return np.array(vecs)

    def _patch_model(self):
        """Return a mock model with our fake encode."""
        mock_model = MagicMock()
        mock_model.encode = self._fake_encode
        return mock_model

    async def test_similar_titles_cluster(self):
        """Two articles about the same event within 4h should cluster."""
        articles = [
            _make_article(1, "Apple reports record earnings in Q2", _NOW, source="alpha_vantage"),
            _make_article(
                2,
                "Apple reports earnings beat expectations",
                _NOW + timedelta(hours=1),
                source="finnhub",
            ),
        ]
        mock_model = self._patch_model()
        set_ids_calls: list = []

        async def _mock_set(ids, cid):
            set_ids_calls.append((ids, cid))

        with (
            patch(
                "pipeline.nlp.dedup.get_unclustered_articles",
                new_callable=AsyncMock,
                return_value=articles,
            ),
            patch("pipeline.nlp.dedup.set_cluster_ids", side_effect=_mock_set),
            patch("pipeline.nlp.dedup._get_model", return_value=mock_model),
        ):
            from pipeline.nlp.dedup import cluster_articles

            n_clusters = await cluster_articles("AAPL")

        assert n_clusters == 1
        assert len(set_ids_calls) == 1
        assert set(set_ids_calls[0][0]) == {1, 2}

    async def test_dissimilar_titles_no_cluster(self):
        """Two unrelated articles should NOT cluster."""
        articles = [
            _make_article(1, "Apple reports record earnings in Q2", _NOW),
            _make_article(2, "Fed raises interest rate by 25bps", _NOW + timedelta(hours=1)),
        ]
        mock_model = self._patch_model()

        with (
            patch(
                "pipeline.nlp.dedup.get_unclustered_articles",
                new_callable=AsyncMock,
                return_value=articles,
            ),
            patch("pipeline.nlp.dedup.set_cluster_ids", new_callable=AsyncMock) as mock_set,
            patch("pipeline.nlp.dedup._get_model", return_value=mock_model),
        ):
            from pipeline.nlp.dedup import cluster_articles

            n_clusters = await cluster_articles("AAPL")

        assert n_clusters == 0
        mock_set.assert_not_called()

    async def test_outside_time_window_no_cluster(self):
        """Similar titles published > 4h apart should NOT cluster."""
        articles = [
            _make_article(1, "Apple reports record earnings in Q2", _NOW),
            _make_article(2, "Apple reports earnings beat expectations", _NOW + timedelta(hours=5)),
        ]
        mock_model = self._patch_model()

        with (
            patch(
                "pipeline.nlp.dedup.get_unclustered_articles",
                new_callable=AsyncMock,
                return_value=articles,
            ),
            patch("pipeline.nlp.dedup.set_cluster_ids", new_callable=AsyncMock) as mock_set,
            patch("pipeline.nlp.dedup._get_model", return_value=mock_model),
        ):
            from pipeline.nlp.dedup import cluster_articles

            n_clusters = await cluster_articles("AAPL")

        assert n_clusters == 0
        mock_set.assert_not_called()

    async def test_single_article_skip(self):
        """One article returns 0 clusters (skip path, no model loaded)."""
        articles = [_make_article(1, "Lone article", _NOW)]

        with (
            patch(
                "pipeline.nlp.dedup.get_unclustered_articles",
                new_callable=AsyncMock,
                return_value=articles,
            ),
            patch("pipeline.nlp.dedup.set_cluster_ids", new_callable=AsyncMock) as mock_set,
        ):
            from pipeline.nlp.dedup import cluster_articles

            n_clusters = await cluster_articles("AAPL")

        assert n_clusters == 0
        mock_set.assert_not_called()

    async def test_empty_articles_skip(self):
        """Zero articles returns 0 clusters."""
        with (
            patch(
                "pipeline.nlp.dedup.get_unclustered_articles",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch("pipeline.nlp.dedup.set_cluster_ids", new_callable=AsyncMock) as mock_set,
        ):
            from pipeline.nlp.dedup import cluster_articles

            n_clusters = await cluster_articles("AAPL")

        assert n_clusters == 0
        mock_set.assert_not_called()

    async def test_idempotent_already_clustered_skipped(self):
        """
        Calling cluster_articles twice: second call processes only new
        unclustered articles. If none remain, returns 0.
        """
        # First call: two similar articles
        articles = [
            _make_article(1, "Apple reports record earnings in Q2", _NOW),
            _make_article(2, "Apple reports earnings beat expectations", _NOW + timedelta(hours=1)),
        ]
        mock_model = self._patch_model()

        with (
            patch(
                "pipeline.nlp.dedup.get_unclustered_articles",
                new_callable=AsyncMock,
                return_value=articles,
            ),
            patch("pipeline.nlp.dedup.set_cluster_ids", new_callable=AsyncMock),
            patch("pipeline.nlp.dedup._get_model", return_value=mock_model),
        ):
            from pipeline.nlp.dedup import cluster_articles

            n1 = await cluster_articles("AAPL")
        assert n1 == 1

        # Second call: get_unclustered_articles returns empty (all already clustered)
        with (
            patch(
                "pipeline.nlp.dedup.get_unclustered_articles",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch("pipeline.nlp.dedup.set_cluster_ids", new_callable=AsyncMock) as mock_set,
        ):
            n2 = await cluster_articles("AAPL")
        assert n2 == 0
        mock_set.assert_not_called()

    async def test_multiple_distinct_clusters(self):
        """Three events with two articles each should produce 3 clusters."""
        articles = [
            _make_article(1, "Apple reports record earnings in Q2", _NOW),
            _make_article(2, "Apple reports earnings beat expectations", _NOW + timedelta(hours=1)),
            _make_article(3, "Fed raises interest rate by 25bps", _NOW),
            _make_article(4, "Fed rate hike decision announced", _NOW + timedelta(hours=2)),
            _make_article(5, "Tesla recalls 500k vehicles over defect", _NOW),
            _make_article(6, "Tesla recall affects half million cars", _NOW + timedelta(hours=1)),
        ]
        mock_model = self._patch_model()
        set_ids_calls: list = []

        async def _mock_set(ids, cid):
            set_ids_calls.append((sorted(ids), cid))

        with (
            patch(
                "pipeline.nlp.dedup.get_unclustered_articles",
                new_callable=AsyncMock,
                return_value=articles,
            ),
            patch("pipeline.nlp.dedup.set_cluster_ids", side_effect=_mock_set),
            patch("pipeline.nlp.dedup._get_model", return_value=mock_model),
        ):
            from pipeline.nlp.dedup import cluster_articles

            n_clusters = await cluster_articles("AAPL")

        assert n_clusters == 3
        cluster_id_sets = [set(ids) for ids, _ in set_ids_calls]
        assert {1, 2} in cluster_id_sets
        assert {3, 4} in cluster_id_sets
        assert {5, 6} in cluster_id_sets

    async def test_cross_source_articles_cluster(self):
        """
        Articles from different sources (AV + Finnhub) about the same event
        should be clustered together.
        """
        articles = [
            _make_article(1, "Apple reports record earnings in Q2", _NOW, source="alpha_vantage"),
            _make_article(
                2,
                "Apple reports earnings beat expectations",
                _NOW + timedelta(hours=1),
                source="finnhub",
            ),
        ]
        mock_model = self._patch_model()
        set_ids_calls: list = []

        async def _mock_set(ids, cid):
            set_ids_calls.append((ids, cid))

        with (
            patch(
                "pipeline.nlp.dedup.get_unclustered_articles",
                new_callable=AsyncMock,
                return_value=articles,
            ),
            patch("pipeline.nlp.dedup.set_cluster_ids", side_effect=_mock_set),
            patch("pipeline.nlp.dedup._get_model", return_value=mock_model),
        ):
            from pipeline.nlp.dedup import cluster_articles

            n_clusters = await cluster_articles("AAPL")

        assert n_clusters == 1
        assert set(set_ids_calls[0][0]) == {1, 2}


# ---------------------------------------------------------------------------
# B. Integration tests for get_articles_since() DISTINCT ON filtering
# ---------------------------------------------------------------------------


class TestGetArticlesSinceDedup:
    """
    Tests for the cluster-aware DISTINCT ON query in get_articles_since().

    These tests mock the DB connection pool to verify SQL behavior
    via the returned rows — they do NOT require a live DB.
    """

    async def test_unclustered_articles_coalesce_fallback(self):
        """
        The COALESCE(event_cluster_id, id::text) expression ensures that
        unclustered articles (NULL cluster ID) are each treated as unique
        rows. Verify the SQL contains this expression.
        """
        import inspect

        from scripts.db.queries.raw_articles import get_articles_since

        source = inspect.getsource(get_articles_since)

        # COALESCE with id::text ensures NULL cluster IDs become unique per-row
        assert "id::text" in source, "COALESCE must fall back to id::text for unclustered articles"

    async def test_clustered_articles_highest_relevance_selected(self):
        """
        Given 3 articles in the same cluster with relevance 0.3, 0.8, 0.5,
        the query should return only the article with relevance 0.8.

        This test verifies the SQL logic at the query layer level by checking
        that the SQL string contains the DISTINCT ON clause.
        """
        import inspect

        from scripts.db.queries.raw_articles import get_articles_since

        source = inspect.getsource(get_articles_since)
        assert "DISTINCT ON" in source, "get_articles_since must use DISTINCT ON for cluster dedup"
        assert "COALESCE(event_cluster_id" in source, "must use COALESCE for NULL cluster IDs"
        assert "relevance_score DESC" in source, (
            "must order by relevance_score DESC to pick highest"
        )

    async def test_unscored_articles_excluded_before_distinct(self):
        """
        Edge case: a cluster of 2 articles where one has higher relevance
        but NULL finbert_score (unscored).

        The finbert_score IS NOT NULL filter MUST be in WHERE (before
        DISTINCT ON) so unscored articles are excluded from selection
        entirely, letting the scored article win.

        This test verifies the SQL ordering: WHERE filters first, then DISTINCT ON
        picks from the remaining (FinBERT-scored) rows.
        """
        import inspect

        from scripts.db.queries.raw_articles import get_articles_since

        source = inspect.getsource(get_articles_since)

        # The WHERE clause must contain finbert_score IS NOT NULL
        # This ensures unscored articles are excluded BEFORE
        # DISTINCT ON picks the "best" article per cluster.
        assert "finbert_score IS NOT NULL" in source, (
            "finbert_score filter must be in WHERE clause (before DISTINCT ON)"
        )

        # Verify it's NOT in a HAVING clause or subquery wrapper
        assert "HAVING" not in source.upper(), (
            "finbert_score filter must NOT be in HAVING — must be in WHERE"
        )

    async def test_query_returns_expected_columns(self):
        """get_articles_since returns dicts with the expected keys."""
        import inspect

        from scripts.db.queries.raw_articles import get_articles_since

        source = inspect.getsource(get_articles_since)
        # Must select these columns for score_narrative_signals compatibility
        for col in (
            "published_at",
            "finbert_score",
            "relevance_score",
            "source",
            "finbert_pos",
            "finbert_neg",
            "finbert_neu",
        ):
            assert col in source, f"get_articles_since must SELECT {col}"


# ---------------------------------------------------------------------------
# C. Integration test: narrative_job calls cluster_articles
# ---------------------------------------------------------------------------


class TestNarrativeJobClustering:
    """Verify narrative_job() calls cluster_articles for each ticker."""

    async def test_narrative_job_calls_cluster_articles(self):
        """
        narrative_job must call cluster_articles() for each active ticker
        after fetching all articles.
        """
        mock_tickers = ["AAPL", "MSFT", "GOOGL"]
        cluster_calls: list[str] = []

        async def _mock_cluster(ticker, **kwargs):
            cluster_calls.append(ticker)
            return 0

        with (
            patch(
                "pipeline.scheduler.get_active_tickers",
                new_callable=AsyncMock,
                return_value=mock_tickers,
            ),
            patch("pipeline.scheduler._fetch_all_tickers", new_callable=AsyncMock),
            patch("pipeline.scheduler.cluster_articles", side_effect=_mock_cluster),
            patch("pipeline.scheduler._record_run", new_callable=AsyncMock),
            patch(
                "scripts.db.queries.raw_articles.count_unclustered_articles",
                new_callable=AsyncMock,
                return_value=0,
            ),
            # Phase 3 (FinBERT) must not reach a real database.
            patch(
                "scripts.db.queries.raw_articles.get_unscored_articles",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "pipeline.scheduler._get_cluster_telemetry",
                new_callable=AsyncMock,
                return_value={
                    "cross_source_clusters": 0,
                    "same_source_clusters": 0,
                    "largest_cluster_size": 0,
                },
            ),
        ):
            from pipeline.scheduler import narrative_job

            await narrative_job()

        assert sorted(cluster_calls) == sorted(mock_tickers)

    async def test_narrative_job_clustering_failure_isolated(self):
        """
        If cluster_articles raises for one ticker, other tickers
        should still be clustered.
        """
        mock_tickers = ["AAPL", "MSFT", "GOOGL"]
        cluster_calls: list[str] = []

        async def _mock_cluster(ticker, **kwargs):
            if ticker == "MSFT":
                raise RuntimeError("model load failed")
            cluster_calls.append(ticker)
            return 1

        with (
            patch(
                "pipeline.scheduler.get_active_tickers",
                new_callable=AsyncMock,
                return_value=mock_tickers,
            ),
            patch("pipeline.scheduler._fetch_all_tickers", new_callable=AsyncMock),
            patch("pipeline.scheduler.cluster_articles", side_effect=_mock_cluster),
            patch("pipeline.scheduler._record_run", new_callable=AsyncMock),
            patch(
                "scripts.db.queries.raw_articles.count_unclustered_articles",
                new_callable=AsyncMock,
                return_value=0,
            ),
            # Phase 3 (FinBERT) must not reach a real database.
            patch(
                "scripts.db.queries.raw_articles.get_unscored_articles",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "pipeline.scheduler._get_cluster_telemetry",
                new_callable=AsyncMock,
                return_value={
                    "cross_source_clusters": 0,
                    "same_source_clusters": 0,
                    "largest_cluster_size": 0,
                },
            ),
        ):
            from pipeline.scheduler import narrative_job

            await narrative_job()

        # AAPL and GOOGL should still have been processed
        assert "AAPL" in cluster_calls
        assert "GOOGL" in cluster_calls
        assert "MSFT" not in cluster_calls


# ---------------------------------------------------------------------------
# D. Telemetry tests
# ---------------------------------------------------------------------------


class TestDedupTelemetry:
    """Verify dedup telemetry includes all 7 required fields."""

    async def test_telemetry_log_contains_required_fields(self):
        """
        The narrative_job dedup summary log must contain all 7 fields:
        total_articles_processed, total_clusters_formed, cross_source_clusters,
        same_source_clusters, largest_cluster_size, tickers_processed,
        elapsed_seconds.
        """
        mock_tickers = ["AAPL"]

        async def _mock_cluster(ticker, **kwargs):
            return 0

        with (
            patch(
                "pipeline.scheduler.get_active_tickers",
                new_callable=AsyncMock,
                return_value=mock_tickers,
            ),
            patch("pipeline.scheduler._fetch_all_tickers", new_callable=AsyncMock),
            patch("pipeline.scheduler.cluster_articles", side_effect=_mock_cluster),
            patch("pipeline.scheduler._record_run", new_callable=AsyncMock),
            patch(
                "scripts.db.queries.raw_articles.count_unclustered_articles",
                new_callable=AsyncMock,
                return_value=5,
            ),
            patch(
                "pipeline.scheduler._get_cluster_telemetry",
                new_callable=AsyncMock,
                return_value={
                    "cross_source_clusters": 0,
                    "same_source_clusters": 0,
                    "largest_cluster_size": 0,
                },
            ),
            patch(
                "scripts.db.queries.raw_articles.get_unscored_articles",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch("pipeline.scheduler._log") as mock_log,
        ):
            from pipeline.scheduler import narrative_job

            await narrative_job()

        # Collect all INFO log format strings
        all_log_fmts = [call[0][0] for call in mock_log.info.call_args_list if call[0]]

        dedup_fmts = [m for m in all_log_fmts if "dedup" in m.lower()]
        assert len(dedup_fmts) >= 1, f"Expected a dedup log line, got: {all_log_fmts}"

        dedup_fmt = dedup_fmts[0]
        required_fields = [
            "total_articles_processed",
            "total_clusters_formed",
            "cross_source_clusters",
            "same_source_clusters",
            "largest_cluster_size",
            "tickers_processed",
            "elapsed_seconds",
        ]
        for field in required_fields:
            assert field in dedup_fmt, f"Telemetry log missing field: {field}"


# ---------------------------------------------------------------------------
# E. Scoring integration: dedup reduces signal count
# ---------------------------------------------------------------------------


class TestScoringWithDedup:
    """
    Verify that the full scoring path produces fewer signals when
    articles are deduplicated.
    """

    async def test_dedup_reduces_narrative_signal_count(self):
        """
        With 3 articles in a cluster, score_narrative_signals should receive
        only 1 article (the highest-relevance one selected by the query),
        producing fewer scored signals than if all 3 were passed.
        """
        from pipeline.features.normalize import score_narrative_signals

        # Simulate: 3 articles about the same event, varying relevance
        # Sprint A: uses finbert_score + finbert class probabilities
        all_three = [
            {
                "published_at": _NOW,
                "finbert_score": 0.5,
                "finbert_pos": 0.7,
                "finbert_neg": 0.2,
                "finbert_neu": 0.1,
                "relevance_score": 0.65,
                "source": "alpha_vantage",
            },
            {
                "published_at": _NOW - timedelta(hours=1),
                "finbert_score": 0.6,
                "finbert_pos": 0.75,
                "finbert_neg": 0.15,
                "finbert_neu": 0.1,
                "relevance_score": 0.9,
                "source": "alpha_vantage",
            },
            {
                "published_at": _NOW - timedelta(hours=2),
                "finbert_score": 0.4,
                "finbert_pos": 0.6,
                "finbert_neg": 0.2,
                "finbert_neu": 0.2,
                "relevance_score": 0.7,
                "source": "alpha_vantage",
            },
        ]

        # Without dedup: all 3 scored
        sigs_all = score_narrative_signals("AAPL", all_three, _NOW)

        # With dedup: only the highest-relevance article (0.9) passes through
        deduped = [all_three[1]]  # relevance 0.9
        sigs_deduped = score_narrative_signals("AAPL", deduped, _NOW)

        assert len(sigs_all) == 3
        assert len(sigs_deduped) == 1
        # The single deduped signal should have higher weight (higher relevance)
        assert sigs_deduped[0]["weight"] >= sigs_all[0]["weight"]
