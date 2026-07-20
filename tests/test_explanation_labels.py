"""tests/test_explanation_labels.py

The narrative layer (30% weight) emits signal_type="finbert_sentiment". It must
map to a real label + explanation phrase, not fall through to the generic
default ("a positive signal").
"""
from __future__ import annotations

from pipeline.explanation.templates import _DEFAULT_PHRASE, generate_explanation
from pipeline.scoring.drivers import DriverRecord, _label


def test_finbert_sentiment_has_label():
    assert _label("finbert_sentiment") == "News sentiment"


def test_finbert_driver_renders_news_phrase_not_default():
    driver = DriverRecord(
        signal=_label("finbert_sentiment"),
        description="News sentiment for AAPL: 0.83",
        direction="bullish",
        magnitude=0.83,
        source_layer="narrative",
        confidence=0.6,
    )
    text = generate_explanation([driver])
    assert "news sentiment" in text.lower()
    assert _DEFAULT_PHRASE["bullish"] not in text  # not the generic fallback
