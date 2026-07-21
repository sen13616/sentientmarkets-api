"""Offline evaluation harness — the release gate for scoring changes.

Reads served outputs (sentiment_history, raw_signals closes) directly from the
DB and measures calibration, dispersion, forward IC, and lead-lag location.
Must never import pipeline scoring code: it evaluates what was served, not what
the code intends to serve.
"""
