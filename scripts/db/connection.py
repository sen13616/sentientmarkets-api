from __future__ import annotations

import os

import asyncpg
from dotenv import load_dotenv

load_dotenv(override=False)

_pool: asyncpg.Pool | None = None

# Per-query timeout (seconds) for the app's pool. Without one, a connection
# that dies silently mid-query (half-open TCP to the Railway Postgres) awaits
# forever — on 2026-09-17 that froze 3 tickers inside the scoring tick, which
# then never completed and blocked every later tick via max_instances=1.
APP_COMMAND_TIMEOUT_S = float(os.environ.get("DB_COMMAND_TIMEOUT_S", "60"))


def _dsn() -> str:
    url = os.environ["DATABASE_URL"]
    # asyncpg expects postgresql:// — strip SQLAlchemy-style dialect suffix if present
    return url.replace("postgresql+asyncpg://", "postgresql://").replace(
        "postgres+asyncpg://", "postgres://"
    )


async def init_pool(command_timeout: float | None = None, max_size: int = 10) -> None:
    """
    Create the shared pool. ``command_timeout`` bounds every query on it;
    the app passes APP_COMMAND_TIMEOUT_S. Scripts that auto-init via
    get_pool() keep no timeout, since eval/backfill queries can run long.
    ``max_size`` lets offline replays run more concurrent queries than the
    app's default of 10.
    """
    global _pool
    if _pool is not None:
        return
    _pool = await asyncpg.create_pool(
        dsn=_dsn(), command_timeout=command_timeout, max_size=max_size,
    )


async def get_pool() -> asyncpg.Pool:
    """Return the connection pool, initialising it automatically if needed."""
    global _pool
    if _pool is None:
        await init_pool()
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
