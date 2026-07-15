"""
pipeline/nlp/dedup.py

Stage 2 article deduplication — semantic event clustering.

Stage 1 (exact-match by URL hash) is already handled at insert time via
ON CONFLICT (ticker, content_hash) DO NOTHING in raw_articles.

Stage 2 (this module)
---------------------
For each ticker, fetch recent unclustered articles and group those that:
  (a) are published within a 4-hour window of each other, AND
  (b) have title cosine-similarity > 0.85 (same underlying event).

Articles sharing an event get the same event_cluster_id written back to
raw_articles.  Singleton articles (no close match found) are left with
event_cluster_id = NULL.

For scoring (Layer 05), the narrative module selects only the highest-
relevance article from each cluster, treating the cluster as one signal.

Algorithm
---------
Union-Find over the article set.
1. Encode all titles with sentence-transformers (all-MiniLM-L6-v2).
2. For every pair (i, j) where |t_i − t_j| ≤ window_hours:
       if cosine_similarity(emb_i, emb_j) > SIMILARITY_THRESHOLD → union(i, j)
3. Collect groups with ≥ 2 members → assign a UUID cluster_id.
4. Write cluster_ids back to the DB.

The sentence-transformers model is loaded lazily on first call (≈80 MB).
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime

from scripts.db.queries.raw_articles import get_unclustered_articles, set_cluster_ids

_log = logging.getLogger(__name__)

_model = None  # loaded lazily on first call to avoid torch import at startup

SIMILARITY_THRESHOLD = 0.85
_MODEL_NAME = "all-MiniLM-L6-v2"


def _get_model():
    """Load the sentence-transformer model on first use."""
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer  # noqa: PLC0415
        _log.info("Loading sentence-transformer model '%s' …", _MODEL_NAME)
        _model = SentenceTransformer(_MODEL_NAME)
    return _model


# ---------------------------------------------------------------------------
# Union-Find (path-compressed)
# ---------------------------------------------------------------------------

def _find(parent: list[int], x: int) -> int:
    while parent[x] != x:
        parent[x] = parent[parent[x]]   # path halving
        x = parent[x]
    return x


def _union(parent: list[int], x: int, y: int) -> None:
    px, py = _find(parent, x), _find(parent, y)
    if px != py:
        parent[px] = py


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def cluster_articles(
    ticker: str,
    window_hours: float = 4.0,
    since_hours: float = 48.0,
) -> int:
    """
    Cluster semantically similar articles for `ticker` and write
    event_cluster_id back to raw_articles.

    Parameters
    ----------
    ticker       : stock ticker symbol
    window_hours : maximum time gap (hours) between articles in the same cluster
    since_hours  : look-back window for fetching unclustered articles

    Returns
    -------
    int : number of multi-article clusters created (0 if nothing to cluster)
    """
    articles = await get_unclustered_articles(ticker, since_hours=since_hours)
    n = len(articles)

    if n < 2:
        _log.debug("cluster_articles(%s): only %d unclustered articles — skipping", ticker, n)
        return 0

    # --- Encode titles --------------------------------------------------
    import numpy as np  # noqa: PLC0415 — deferred to avoid torch at startup
    titles = [a["title"] or "" for a in articles]
    # Model load + encode are sync CPU torch work; run off the event loop so
    # the in-process FastAPI keeps serving during the narrative job.
    embeddings = await asyncio.to_thread(
        lambda: _get_model().encode(
            titles,
            normalize_embeddings=True,   # unit-vectors → cosine = dot product
            show_progress_bar=False,
            batch_size=64,
        )
    )  # shape (n, dim)

    # --- Build time-aware similarity graph via union-find ----------------
    parent = list(range(n))
    window_sec = window_hours * 3600

    for i in range(n):
        pub_i: datetime = articles[i]["published_at"]
        for j in range(i + 1, n):
            pub_j: datetime = articles[j]["published_at"]
            dt = abs((pub_i - pub_j).total_seconds())
            if dt > window_sec:
                continue
            sim = float(np.dot(embeddings[i], embeddings[j]))
            if sim > SIMILARITY_THRESHOLD:
                _union(parent, i, j)

    # --- Collect multi-member clusters -----------------------------------
    roots: dict[int, list[int]] = {}
    for i in range(n):
        r = _find(parent, i)
        roots.setdefault(r, []).append(i)

    multi_clusters = [(root, members) for root, members in roots.items() if len(members) >= 2]

    if not multi_clusters:
        _log.debug("cluster_articles(%s): no clusters found among %d articles", ticker, n)
        return 0

    # --- Write cluster IDs to DB -----------------------------------------
    for _root, members in multi_clusters:
        cluster_id = str(uuid.uuid4())
        art_ids = [articles[i]["id"] for i in members]
        await set_cluster_ids(art_ids, cluster_id)
        _log.debug(
            "cluster_articles(%s): cluster %s → %d articles",
            ticker, cluster_id[:8], len(members),
        )

    _log.info(
        "cluster_articles(%s): %d cluster(s) from %d articles",
        ticker, len(multi_clusters), n,
    )
    return len(multi_clusters)
