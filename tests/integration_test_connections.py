"""
tests/integration_test_connections.py

Integration tests for DB and Redis connectivity.
Requires local PostgreSQL and Redis to be running.

Run with:
    pytest -m integration
"""

import pytest

from scripts.db.connection import close_pool, get_pool, init_pool
from scripts.db.redis import close_redis, get_redis, init_redis


@pytest.mark.integration
async def test_postgres_pool_acquires_connection():
    try:
        await init_pool()
        pool = await get_pool()
        async with pool.acquire() as conn:
            result = await conn.fetchval("SELECT 1")
        assert result == 1
    finally:
        await close_pool()


@pytest.mark.integration
async def test_postgres_pool_is_idempotent():
    try:
        await init_pool()
        await init_pool()  # second call must not raise or replace the pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            result = await conn.fetchval("SELECT 42")
        assert result == 42
    finally:
        await close_pool()


@pytest.mark.integration
async def test_redis_ping():
    try:
        await init_redis()
        client = get_redis()
        pong = await client.ping()
        assert pong is True
    finally:
        await close_redis()


@pytest.mark.integration
async def test_redis_set_get_delete():
    try:
        await init_redis()
        client = get_redis()
        await client.set("test:block1", "ok")
        value = await client.get("test:block1")
        assert value == "ok"
        await client.delete("test:block1")
    finally:
        await close_redis()


@pytest.mark.integration
async def test_redis_is_idempotent():
    try:
        await init_redis()
        await init_redis()  # second call must not raise or replace the client
        client = get_redis()
        assert await client.ping() is True
    finally:
        await close_redis()


@pytest.mark.integration
async def test_get_pool_auto_inits():
    # get_pool() must initialise the pool automatically when called cold
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            result = await conn.fetchval("SELECT 99")
        assert result == 99
    finally:
        await close_pool()


@pytest.mark.integration
async def test_get_redis_raises_before_init():
    with pytest.raises(RuntimeError, match="init_redis"):
        get_redis()
