"""
tests/test_n_signals.py — Verify n_signals aggregation in the scoring pipeline.

Sprint 3 (global scoring tick) removed carry-forward behavior: all four layers
are always recomputed from DB state, so n_signals is simply the count of all
signals with weight > 0 across all layers.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

from pipeline.confidence.scorer import LOW_VOLUME_THRESHOLD
from pipeline.scoring.subindices import SubIndexResult


async def test_n_signals_counts_all_fresh_signals():
    """n_signals should equal the total count of weighted signals across all 4 layers."""
    now = datetime.now(timezone.utc)

    market_sigs = [
        {"signal_type": "return_1d", "score": 60, "weight": 0.9, "source": "yfinance", "value": 0.01},
        {"signal_type": "rsi_14", "score": 55, "weight": 0.85, "source": "computed", "value": 55.0},
        {"signal_type": "volume_ratio", "score": 50, "weight": 0.8, "source": "yfinance", "value": 1.0},
    ]
    narrative_sigs = [
        {"signal_type": "news_sentiment", "score": 65, "weight": 0.7, "source": "alpha_vantage", "value": 0.3},
        {"signal_type": "news_sentiment", "score": 58, "weight": 0.6, "source": "finnhub", "value": 0.1},
    ]
    influencer_sigs = [
        {"signal_type": "insider_net_shares", "score": 40, "weight": 0.5, "source": "sec", "value": -1000},
    ]
    macro_sigs = [
        {"signal_type": "vix", "score": 45, "weight": 0.9, "source": "finnhub", "value": 18.5},
    ]

    captured_n_signals = {}

    def spy_compute_confidence(**kwargs):
        captured_n_signals["value"] = kwargs["n_signals"]
        from pipeline.confidence.scorer import ConfidenceResult
        return ConfidenceResult(score=80, flags=[])

    with (
        patch("pipeline.orchestrator.read_scored_state", new_callable=AsyncMock, return_value=None),
        patch("pipeline.orchestrator.get_latest_close", new_callable=AsyncMock, return_value=150.0),
        patch("pipeline.orchestrator._score_market", new_callable=AsyncMock, return_value=(
            SubIndexResult(55.0, 3, ["yfinance"]), market_sigs, now,
        )),
        patch("pipeline.orchestrator._score_narrative", new_callable=AsyncMock, return_value=(
            SubIndexResult(62.0, 2, ["alpha_vantage", "finnhub"]), narrative_sigs, now,
        )),
        patch("pipeline.orchestrator._score_influencer", new_callable=AsyncMock, return_value=(
            SubIndexResult(40.0, 1, ["sec"]), influencer_sigs, now, now, now,
        )),
        patch("pipeline.orchestrator._score_macro", new_callable=AsyncMock, return_value=(
            SubIndexResult(45.0, 1, ["finnhub"]), macro_sigs, now,
        )),
        patch("pipeline.orchestrator.compute_confidence", side_effect=spy_compute_confidence),
        patch("pipeline.orchestrator.write_scored_state", new_callable=AsyncMock),
        patch("pipeline.orchestrator.persist_scored_state", new_callable=AsyncMock),
    ):
        from pipeline.orchestrator import _score_and_write
        await _score_and_write("AAPL")

    # 3 market + 2 narrative + 1 influencer + 1 macro = 7
    assert captured_n_signals["value"] == 7
    assert captured_n_signals["value"] >= LOW_VOLUME_THRESHOLD


async def test_all_layers_recomputed_uses_fresh_sigs_only():
    """When some layers return no data, n_signals counts only what's present."""
    now = datetime.now(timezone.utc)

    fresh_sigs = [
        {"signal_type": "return_1d", "score": 60, "weight": 0.9, "source": "yfinance", "value": 0.01},
        {"signal_type": "rsi_14", "score": 55, "weight": 0.85, "source": "computed", "value": 55.0},
    ]

    captured_n_signals = {}

    def spy_compute_confidence(**kwargs):
        captured_n_signals["value"] = kwargs["n_signals"]
        from pipeline.confidence.scorer import ConfidenceResult
        return ConfidenceResult(score=80, flags=[])

    si = SubIndexResult(55.0, 2, ["yfinance"])

    with (
        patch("pipeline.orchestrator.read_scored_state", new_callable=AsyncMock, return_value=None),
        patch("pipeline.orchestrator.get_latest_close", new_callable=AsyncMock, return_value=150.0),
        patch("pipeline.orchestrator._score_market", new_callable=AsyncMock, return_value=(si, fresh_sigs, now)),
        patch("pipeline.orchestrator._score_narrative", new_callable=AsyncMock, return_value=(None, [], None)),
        patch("pipeline.orchestrator._score_influencer", new_callable=AsyncMock, return_value=(None, [], None, None, None)),
        patch("pipeline.orchestrator._score_macro", new_callable=AsyncMock, return_value=(None, [], None)),
        patch("pipeline.orchestrator.compute_confidence", side_effect=spy_compute_confidence),
        patch("pipeline.orchestrator.write_scored_state", new_callable=AsyncMock),
        patch("pipeline.orchestrator.persist_scored_state", new_callable=AsyncMock),
    ):
        from pipeline.orchestrator import _score_and_write
        await _score_and_write("AAPL")

    # Only fresh market sigs count — no carry-forward
    assert captured_n_signals["value"] == 2
