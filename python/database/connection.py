import asyncio
import os
import asyncpg
from typing import Optional

DB_NAME = os.getenv("POSTGRES_DB", "chandystore_db")
DB_USER = os.getenv("POSTGRES_USER", os.getenv("USER", "tejashr"))
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

pool: Optional[asyncpg.Pool] = None

async def init_db_pool() -> asyncpg.Pool:
    global pool
    loop = asyncio.get_running_loop()
    if pool is not None and (pool._loop.is_closed() or pool._loop != loop):
        try:
            await pool.close()
        except Exception:
            pass
        pool = None

    if pool is None:
        pool = await asyncpg.create_pool(
            database=DB_NAME,
            user=DB_USER,
            host=DB_HOST,
            port=DB_PORT,
            password=DB_PASSWORD if DB_PASSWORD else None,
            min_size=2,
            max_size=20
        )
    return pool

async def get_db_pool() -> asyncpg.Pool:
    global pool
    loop = asyncio.get_running_loop()
    if pool is None or pool._loop.is_closed() or pool._loop != loop:
        await init_db_pool()
    return pool

async def close_db_pool():
    global pool
    if pool is not None:
        try:
            await pool.close()
        except Exception:
            pass
        pool = None
