"""
tests/test_bid_ask_spread.py

Unit tests for _fetch_bid_ask_spread in pipeline/sources/market.py.

All tests mock yfinance.Ticker so no network calls are made.
"""
from __future__ import annotations

from unittest.mock import patch

from pipeline.sources.market import _fetch_bid_ask_spread


class TestFetchBidAskSpread:

    @patch("yfinance.Ticker")
    def test_valid_bid_ask_returns_correct_spread(self, mock_ticker_cls):
        mock_ticker_cls.return_value.info = {"bid": 149.50, "ask": 150.50}

        result = _fetch_bid_ask_spread("AAPL")

        assert result is not None
        assert result["bid"] == 149.50
        assert result["ask"] == 150.50
        assert result["spread"] == 1.0
        assert result["midpoint"] == 150.0
        # spread_bps = (1.0 / 150.0) * 10000 = 66.67
        assert result["spread_bps"] == 66.67

    @patch("yfinance.Ticker")
    def test_missing_bid_returns_none(self, mock_ticker_cls):
        mock_ticker_cls.return_value.info = {"ask": 150.50}

        assert _fetch_bid_ask_spread("AAPL") is None

    @patch("yfinance.Ticker")
    def test_missing_ask_returns_none(self, mock_ticker_cls):
        mock_ticker_cls.return_value.info = {"bid": 149.50}

        assert _fetch_bid_ask_spread("AAPL") is None

    @patch("yfinance.Ticker")
    def test_ask_less_than_bid_returns_none(self, mock_ticker_cls):
        mock_ticker_cls.return_value.info = {"bid": 150.50, "ask": 149.50}

        assert _fetch_bid_ask_spread("AAPL") is None

    @patch("yfinance.Ticker")
    def test_zero_bid_returns_none(self, mock_ticker_cls):
        mock_ticker_cls.return_value.info = {"bid": 0, "ask": 150.50}

        assert _fetch_bid_ask_spread("AAPL") is None

    @patch("yfinance.Ticker")
    def test_exception_returns_none(self, mock_ticker_cls):
        mock_ticker_cls.return_value.info.__getitem__ = None
        mock_ticker_cls.side_effect = RuntimeError("connection failed")

        assert _fetch_bid_ask_spread("AAPL") is None

    @patch("yfinance.Ticker")
    def test_tight_spread_computes_small_bps(self, mock_ticker_cls):
        mock_ticker_cls.return_value.info = {"bid": 200.00, "ask": 200.02}

        result = _fetch_bid_ask_spread("MSFT")

        assert result is not None
        assert result["spread"] == 0.02
        # spread_bps = (0.02 / 200.01) * 10000 ≈ 1.0
        assert result["spread_bps"] == 1.0
