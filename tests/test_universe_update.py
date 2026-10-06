"""
tests/test_universe_update.py

Universe update (migration 015): retired symbols, as-of universe for offline
jobs, API handling of retired tickers, the RSI frozen-history guard, the
update_universe planner, and the new-ticker warm-start helpers.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

UTC = timezone.utc


def _mock_pool_with(conn):
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    pool = MagicMock()
    pool.acquire.return_value = ctx
    return pool


def _sql(call) -> str:
    return " ".join(call.args[0].split())


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------


class TestUniverseQueries:
    async def _fetch(self, fn, *args):
        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[{"ticker": "AAPL"}])
        with patch(
            "scripts.db.queries.universe.get_pool",
            new=AsyncMock(return_value=_mock_pool_with(conn)),
        ):
            import scripts.db.queries.universe as u

            out = await getattr(u, fn)(*args)
        return conn, out

    async def test_live_active_excludes_retired(self):
        conn, out = await self._fetch("get_active_tickers")
        assert "delisted_at IS NULL" in _sql(conn.fetch.call_args)
        assert out == ["AAPL"]

    async def test_as_of_includes_later_retirements_and_excludes_later_additions(self):
        t = datetime(2026, 6, 23, tzinfo=UTC)
        conn, _ = await self._fetch("get_active_tickers", t)
        sql = _sql(conn.fetch.call_args)
        assert "added_at <= $1" in sql and "(delisted_at IS NULL OR delisted_at > $1)" in sql
        assert conn.fetch.call_args.args[1] == t

    async def test_universe_as_of_refuses_empty(self):
        import scripts.db.queries.universe as u

        with patch.object(u, "get_active_tickers", new=AsyncMock(return_value=[])):
            with pytest.raises(ValueError, match="universe-as-of"):
                await u.get_universe_as_of(datetime(2026, 3, 9, tzinfo=UTC))

    async def test_is_supported_false_for_retired(self):
        import scripts.db.queries.universe as u

        retired = {
            "ticker": "EA",
            "delisted_at": datetime(2026, 8, 4, 21, tzinfo=UTC),
            "successor_ticker": None,
            "delisted_reason": "x",
        }
        with patch.object(u, "get_ticker_status", new=AsyncMock(return_value=retired)):
            assert await u.is_supported_ticker("EA") is False
        with patch.object(
            u, "get_ticker_status", new=AsyncMock(return_value={**retired, "delisted_at": None})
        ):
            assert await u.is_supported_ticker("AAPL") is True


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------


@pytest.fixture
def pro_client():
    from api.auth import authenticate
    from main import app

    app.dependency_overrides[authenticate] = lambda: "pro"
    with patch("api.rate_limit.check_rate_limit", AsyncMock()):
        yield TestClient(app)
    app.dependency_overrides.clear()


def _status(delisted=None, successor=None):
    return {
        "ticker": "X",
        "delisted_at": delisted,
        "successor_ticker": successor,
        "delisted_reason": None,
    }


def test_sentiment_retired_ticker_reports_delisted_with_successor(pro_client):
    with patch(
        "api.routes.sentiment.get_ticker_status",
        AsyncMock(return_value=_status(datetime(2026, 1, 13, 21, tzinfo=UTC), "MRSH")),
    ):
        body = pro_client.get("/v1/sentiment/MMC").json()
    assert body["status"] == "delisted"
    assert "2026-01-13" in body["message"] and "successor: MRSH" in body["message"]


def test_sentiment_unknown_ticker_not_found(pro_client):
    with patch("api.routes.sentiment.get_ticker_status", AsyncMock(return_value=None)):
        assert pro_client.get("/v1/sentiment/ZZZZ").json()["status"] == "ticker_not_found"


def test_history_serves_retired_ticker_and_404s_unknown(pro_client):
    with (
        patch(
            "api.routes.history.get_ticker_status",
            AsyncMock(return_value=_status(datetime(2026, 8, 4, 21, tzinfo=UTC))),
        ),
        patch("api.routes.history.get_history", AsyncMock(return_value=[])),
    ):
        assert pro_client.get("/v1/sentiment/EA/history").status_code == 200
    with patch("api.routes.history.get_ticker_status", AsyncMock(return_value=None)):
        assert pro_client.get("/v1/sentiment/ZZZZ/history").status_code == 404


def test_tickers_include_in_sp500(pro_client):
    rows = [
        {
            "ticker": "AAPL",
            "company_name": "Apple",
            "sector": "Information Technology",
            "in_sp500": True,
        },
        {
            "ticker": "DKNG",
            "company_name": "DraftKings",
            "sector": "Consumer Discretionary",
            "in_sp500": False,
        },
    ]
    with patch("api.routes.tickers.get_all_tickers", AsyncMock(return_value=rows)):
        body = pro_client.get("/v1/tickers").json()
    assert body["universe_size"] == 2
    assert [t["in_sp500"] for t in body["tickers"]] == [True, False]


# ---------------------------------------------------------------------------
# RSI frozen-history guard
# ---------------------------------------------------------------------------


async def _run_market_rows(close_history, ohlcv=None):
    import pipeline.sources.market as mkt

    with (
        patch.object(mkt, "is_market_hours", return_value=False),
        patch.object(mkt, "get_close_history", new=AsyncMock(return_value=close_history)),
        patch.object(mkt, "get_volume_history", new=AsyncMock(return_value=[1e6] * 20)),
        patch.object(mkt, "insert_signals", new=AsyncMock()) as ins,
    ):
        await mkt._run_market("XYZ", MagicMock(), ohlcv_batch={"XYZ": ohlcv} if ohlcv else None)
    return [r[1] for r in ins.call_args.args[0]]


async def test_rsi_skipped_for_frozen_history_without_current_bar():
    old = datetime.now(UTC) - timedelta(days=30)
    hist = [(old - timedelta(days=i), 100.0 + i % 3) for i in range(40, 0, -1)]
    assert "rsi_14" not in await _run_market_rows(hist)


async def test_rsi_computed_for_recent_history_or_current_bar():
    recent = datetime.now(UTC) - timedelta(days=1)
    hist = [(recent - timedelta(days=i), 100.0 + i % 3) for i in range(40, 0, -1)]
    assert "rsi_14" in await _run_market_rows(hist)
    old = datetime.now(UTC) - timedelta(days=30)
    stale = [(old - timedelta(days=i), 100.0 + i % 3) for i in range(40, 0, -1)]
    bar = {
        "open": 1.0,
        "high": 2.0,
        "low": 0.5,
        "close": 1.5,
        "volume": 1e6,
        "timestamp": datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0),
        "source": "yfinance",
    }
    assert "rsi_14" in await _run_market_rows(stale, ohlcv=bar)


# ---------------------------------------------------------------------------
# update_universe planner
# ---------------------------------------------------------------------------


def _row(**kw):
    base = {
        "company_name": "Co",
        "sector": "Industrials",
        "delisted_at": None,
        "in_sp500": False,
        "sp500_added": None,
    }
    return {**base, **kw}


def test_plan_update_retire_flag_add_and_successor():
    from scripts.tools.update_universe import last_trade_ts, plan_update

    universe = {
        "AAPL": _row(sector="Information Technology"),
        "DKNG": _row(),
        "FI": _row(company_name="Fiserv", sector="Financials"),
        "EA": _row(),
    }
    constituents = {
        "AAPL": {
            "security": "Apple",
            "gics_sector": "Information Technology",
            "date_added": "1982-11-30",
        },
        "TGT": {
            "security": "Target",
            "gics_sector": "Consumer Staples",
            "date_added": "1976-12-31",
        },
    }
    changes = {
        "FI": {
            "last_trading_day": "2025-11-10",
            "successor_ticker": "FISV",
            "reason": "ticker change",
        },
        "EA": {"last_trading_day": "2026-08-04", "successor_ticker": "", "reason": "taken private"},
    }

    plan = plan_update(universe, constituents, changes)
    assert {r["ticker"]: r["successor_ticker"] for r in plan["retire"]} == {
        "EA": None,
        "FI": "FISV",
    }
    assert plan["retire"][0]["delisted_at"] == last_trade_ts("2026-08-04")
    assert {f["ticker"]: f["in_sp500"] for f in plan["flag"]} == {
        "AAPL": True
    }  # DKNG stays non-member, kept
    assert plan["resector"] == []
    adds = {a["ticker"]: a for a in plan["add"]}
    assert adds["TGT"]["in_sp500"] and adds["TGT"]["sector"] == "Consumer Staples"
    assert adds["FISV"] == {
        "ticker": "FISV",
        "company_name": "Fiserv",
        "sector": "Financials",
        "in_sp500": False,
        "sp500_added": None,
    }

    # idempotent: applying the plan's values yields an empty plan
    applied = {**universe}
    for r in plan["retire"]:
        applied[r["ticker"]] = {**applied[r["ticker"]], "delisted_at": r["delisted_at"]}
    for f in plan["flag"]:
        applied[f["ticker"]] = {
            **applied[f["ticker"]],
            "in_sp500": f["in_sp500"],
            "sp500_added": f["sp500_added"],
        }
    for a in plan["add"]:
        applied[a["ticker"]] = _row(
            **{k: a[k] for k in ("company_name", "sector", "in_sp500", "sp500_added")}
        )
    again = plan_update(applied, constituents, changes)
    assert again == {"retire": [], "flag": [], "resector": [], "add": []}


def test_plan_update_resyncs_member_sector_only():
    from scripts.tools.update_universe import plan_update

    universe = {
        "FIS": _row(sector="Information Technology", in_sp500=True, sp500_added=date(2001, 1, 1)),
        "DKNG": _row(sector="Consumer Discretionary"),
    }
    constituents = {
        "FIS": {"security": "FIS", "gics_sector": "Financials", "date_added": "2001-01-01"}
    }
    plan = plan_update(universe, constituents, {})
    assert plan["resector"] == [
        {"ticker": "FIS", "from": "Information Technology", "sector": "Financials"}
    ]


def test_plan_update_rejects_sector_without_etf():
    from scripts.tools.update_universe import plan_update

    with pytest.raises(ValueError, match="sector ETF"):
        plan_update({}, {"NEW": {"security": "N", "gics_sector": "Crypto", "date_added": ""}}, {})


def test_committed_snapshots_are_consistent():
    from pipeline.sources.macro import SECTOR_ETFS
    from scripts.tools.update_universe import load_changes, load_constituents

    cons, ch = load_constituents(), load_changes()
    assert len(cons) == 503
    assert all(r["gics_sector"] in SECTOR_ETFS for r in cons.values())
    assert len(ch) == 28 and all(r["source"].startswith("https://") for r in ch.values())
    assert not (set(ch) & set(cons)), "a retired symbol can't be a current member"


# ---------------------------------------------------------------------------
# Warm-start helpers
# ---------------------------------------------------------------------------


def test_daily_bar_rows_match_live_layout():
    from scripts.backfill.reconstruct_signals import daily_bar_rows

    bars = [
        {
            "start": datetime(2026, 9, d, tzinfo=UTC),
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 10.0,
        }
        for d in (29, 30)
    ]
    rows = daily_bar_rows(
        "TGT", bars, datetime(2026, 9, 30, tzinfo=UTC), datetime(2026, 10, 1, tzinfo=UTC)
    )
    assert {r[1] for r in rows} == {"yf_open", "yf_high", "yf_low", "yf_close", "yf_volume"}
    assert all(
        r[5] == datetime(2026, 9, 30, tzinfo=UTC) and r[3:5] == ("yfinance", "manual_backfill")
        for r in rows
    )


async def test_short_volume_ticker_mode_skips_date_idempotency_and_dedups():
    import scripts.backfill.backfill_short_volume as sv

    pool = MagicMock()
    with (
        patch.object(sv, "init_pool", new=AsyncMock()),
        patch.object(sv, "close_pool", new=AsyncMock()),
        patch.object(sv, "get_pool", new=AsyncMock(return_value=pool)),
        patch.object(
            sv, "_existing_dates", new=AsyncMock(return_value={date(2026, 9, 29)})
        ) as existing,
        patch.object(
            sv,
            "fetch_short_volume_for_date",
            new=AsyncMock(
                return_value={
                    "TGT": {"short_volume": 50, "total_volume": 100},
                    "AAPL": {"short_volume": 1, "total_volume": 2},
                }
            ),
        ),
        patch.object(sv, "insert_signals", new=AsyncMock()) as ins,
        patch.object(sv.asyncio, "sleep", new=AsyncMock()),
    ):
        await sv.backfill(start=date(2026, 9, 29), end=date(2026, 9, 30), tickers={"TGT"})

    existing.assert_not_called()
    assert ins.await_count == 2  # both dates fetched
    assert {r[0] for call in ins.call_args_list for r in call.args[0]} == {"TGT"}
