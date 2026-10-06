"""
tests/test_language_detection.py — Language detection at ingestion.

Sprint A: langdetect filters non-English articles before FinBERT scoring.
"""
from __future__ import annotations

from pipeline.sources.narrative import _detect_language


class TestDetectLanguage:

    def test_english_detected(self):
        """Standard English financial text detected as 'en'."""
        text = "Apple reported strong quarterly earnings beating analyst expectations"
        assert _detect_language(text) == "en"

    def test_short_text_returns_none(self):
        """Text shorter than 20 chars returns None (too short for reliable detection)."""
        assert _detect_language("short") is None
        assert _detect_language("") is None

    def test_none_input_returns_none(self):
        """None-like empty input returns None."""
        assert _detect_language("") is None

    def test_whitespace_only_returns_none(self):
        """Whitespace-only text returns None."""
        assert _detect_language("            ") is None

    def test_returns_string_or_none(self):
        """Return type is always str or None."""
        result = _detect_language("This is a test of the language detection system")
        assert isinstance(result, str) or result is None
