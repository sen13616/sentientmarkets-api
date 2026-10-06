"""
tests/test_market_history_fix.py

Market-history fix (2026-10): market_job re-writes today's partial bar every
15 min under the same timestamp, so the history readers must pick the
latest-written row per date and exclude the current session:
  - get_close_history / get_volume_history: per-date latest created_at,
    `before` bound; get_latest_close: created_at tie-break
  - market._run_market and macro._run_macro pass the current bar's date
  - reconstruct_signals --interval 15m / --replace support
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

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


class TestHistoryQueries:
    async def _run(self, fn_name, *args, **kwargs):
        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])
        conn.fetchval = AsyncMock(return_value=None)
        with patch(
            "scripts.db.queries.raw_signals.get_pool",
            new=AsyncMock(return_value=_mock_pool_with(conn)),
        ):
            import scripts.db.queries.raw_signals as rs

            await getattr(rs, fn_name)(*args, **kwargs)
        return conn

    async def test_close_history_latest_written_per_date_and_before(self):
        conn = await self._run("get_close_history", "AAPL", limit=50, before=date(2026, 9, 22))
        sql = _sql(conn.fetch.call_args)
        assert "DISTINCT ON (DATE(timestamp))" in sql
        assert "ORDER BY DATE(timestamp) DESC, timestamp DESC, created_at DESC" in sql
        assert "timestamp < $3::timestamptz" in sql
        assert conn.fetch.call_args.args[1:] == ("AAPL", 50, datetime(2026, 9, 22, tzinfo=UTC))

    async def test_close_history_without_before_is_unbounded(self):
        conn = await self._run("get_close_history", "XLK", limit=22)
        assert conn.fetch.call_args.args[-1] is None

    async def test_volume_history_is_per_session_not_per_row(self):
        conn = await self._run("get_volume_history", "AAPL", limit=20, before=date(2026, 9, 22))
        sql = _sql(conn.fetch.call_args)
        assert "DISTINCT ON (DATE(timestamp))" in sql and "created_at DESC" in sql
        assert "'ohlcv_volume', 'yf_volume'" in sql
        assert conn.fetch.call_args.args[1:] == ("AAPL", 20, datetime(2026, 9, 22, tzinfo=UTC))

    async def test_latest_close_breaks_ties_by_created_at(self):
        conn = await self._run("get_latest_close", "AAPL")
        assert "ORDER BY timestamp DESC, created_at DESC" in _sql(conn.fetchval.call_args)


# ---------------------------------------------------------------------------
# Callers pass the current session's date
# ---------------------------------------------------------------------------


async def test_run_market_excludes_current_session_from_history():
    import pipeline.sources.market as mkt

    bar_ts = datetime(2026, 9, 22, tzinfo=UTC)
    ohlcv = {
        "AAPL": {
            "open": 340.0,
            "high": 345.0,
            "low": 338.0,
            "close": 339.75,
            "volume": 40_000_000.0,
            "timestamp": bar_ts,
            "source": "yfinance",
        }
    }
    closes = [(bar_ts - timedelta(days=i), 330.0 + i) for i in range(30, 0, -1)]
    with (
        patch.object(mkt, "is_market_hours", return_value=False),
        patch.object(mkt, "get_close_history", new=AsyncMock(return_value=closes)) as gch,
        patch.object(mkt, "get_volume_history", new=AsyncMock(return_value=[4.2e7] * 20)) as gvh,
        patch.object(mkt, "insert_signals", new=AsyncMock()) as ins,
    ):
        await mkt._run_market("AAPL", MagicMock(), ohlcv_batch=ohlcv)

    assert gch.call_args.kwargs["before"] == date(2026, 9, 22)
    assert gvh.call_args.kwargs["before"] == date(2026, 9, 22)
    vr = [r for r in ins.call_args.args[0] if r[1] == "volume_ratio"]
    assert vr and vr[0][2] == round(40_000_000.0 / 4.2e7, 4)


async def test_run_macro_excludes_current_session_from_etf_history():
    import pipeline.sources.macro as mac

    etf_ts = datetime(2026, 9, 22, tzinfo=UTC)
    with (
        patch.object(mac, "_vix_yfinance", new=AsyncMock(return_value=(15.0, "yfinance"))),
        patch.object(mac, "_etf_close_av", new=AsyncMock(return_value=(250.0, etf_ts))),
        patch.object(mac, "get_close_history", new=AsyncMock(return_value=[])) as gch,
        patch.object(mac, "insert_signals", new=AsyncMock()),
    ):
        await mac._run_macro(MagicMock())

    assert gch.await_count == len(mac.SECTOR_ETFS)
    assert all(c.kwargs["before"] == date(2026, 9, 22) for c in gch.call_args_list)


# ---------------------------------------------------------------------------
# reconstruct_signals: 15-min cadence + replace
# ---------------------------------------------------------------------------


def test_15m_marks_mirror_market_job_cron():
    from scripts.backfill import reconstruct_signals as rs

    marks = rs.intraday_marks(date(2026, 9, 22), 15)
    assert len(marks) == 28
    assert marks[0] == datetime(2026, 9, 22, 14, 0, tzinfo=UTC)
    assert marks[-1] == datetime(2026, 9, 22, 20, 45, tzinfo=UTC)
    assert [m.hour for m in rs.intraday_marks(date(2026, 9, 22))] == list(range(14, 21))


def test_15m_partial_bars_and_type_filter():
    from scripts.backfill import reconstruct_signals as rs

    day0 = datetime(2026, 9, 22, 13, 30, tzinfo=UTC)
    bars = [
        {
            "start": day0 + timedelta(minutes=15 * i),
            "open": 100.0 + i,
            "high": 101.0 + i,
            "low": 99.0 + i,
            "close": 100.5 + i,
            "volume": 10.0,
        }
        for i in range(26)
    ]
    bar = rs.partial_bar(bars, datetime(2026, 9, 22, 14, 0, tzinfo=UTC), minutes=15)
    assert bar["close"] == 101.5 and bar["volume"] == 20.0  # 13:30 + 13:45 bars finished by 14:00
    assert rs.hourly_bar_end(bars[-1]["start"], 15) == datetime(2026, 9, 22, 20, 0, tzinfo=UTC)

    daily = [
        {
            "start": datetime(2026, 8, 1, tzinfo=UTC) + timedelta(days=i),
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 90.0 + i,
            "volume": 1000.0,
        }
        for i in range(53)
    ]  # through Sep 22
    rows = rs.build_ticker_rows(
        "AAPL",
        bars,
        daily,
        datetime(2026, 9, 22, tzinfo=UTC),
        datetime(2026, 9, 23, tzinfo=UTC),
        eod=False,
        minutes=15,
        types=set(rs.REPLACE_MARKET_TYPES),
    )
    assert rows and {r[1] for r in rows} <= set(rs.REPLACE_MARKET_TYPES)
    assert min(r[5] for r in rows) == datetime(2026, 9, 22, 14, 0, tzinfo=UTC)


async def test_replace_signals_deletes_then_inserts_in_one_transaction():
    import scripts.db.queries.raw_signals as q

    conn = MagicMock()
    conn.execute = AsyncMock(return_value="DELETE 7")
    conn.executemany = AsyncMock()
    tx = MagicMock()
    tx.__aenter__ = AsyncMock()
    tx.__aexit__ = AsyncMock(return_value=False)
    conn.transaction.return_value = tx
    rows = [
        (
            "AAPL",
            "rsi_14",
            55.0,
            "computed",
            "manual_backfill",
            datetime(2026, 9, 22, 14, tzinfo=UTC),
        )
    ]
    s, e = datetime(2026, 9, 1, tzinfo=UTC), datetime(2026, 10, 3, tzinfo=UTC)
    with patch.object(q, "get_pool", new=AsyncMock(return_value=_mock_pool_with(conn))):
        deleted = await q.replace_signals("AAPL", ["rsi_14"], s, e, rows)

    assert deleted == 7
    assert "DELETE FROM raw_signals" in _sql(conn.execute.call_args)
    assert conn.execute.call_args.args[1:] == ("AAPL", ["rsi_14"], s, e)
    assert conn.executemany.call_args.args[1] == rows
    tx.__aenter__.assert_awaited_once()


async def test_archive_is_reused_when_verified_meta_exists(tmp_path, monkeypatch):
    from scripts.backfill import reconstruct_signals as rs

    s, e = datetime(2026, 9, 1, tzinfo=UTC), datetime(2026, 10, 3, tzinfo=UTC)
    path = tmp_path / "a.csv.gz"
    path.write_bytes(b"")
    path.with_suffix("").with_suffix(".meta.json").write_text(json.dumps({"rows": 1234}))
    monkeypatch.setattr(rs, "_archive_path", lambda start, end: path)
    with patch.object(rs, "get_pool", new=AsyncMock()) as gp:
        assert await rs.archive_replaced(s, e, ["AAPL"]) == 1234
    gp.assert_not_called()
