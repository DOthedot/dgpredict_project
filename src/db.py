"""
Database connection pool manager.

Pipeline position: used by bronze/loader.py, silver/transform.py, gold/mart.py, main.py.

Wraps asyncpg to provide a single shared connection pool across the entire pipeline.
One pool is created at startup and reused — creating a new pool per phase would waste
connections and add latency from repeated handshakes.

Why asyncpg over psycopg2:
    asyncpg speaks PostgreSQL's binary wire protocol natively, making it 3-5x faster
    than psycopg2 for bulk reads/writes. It is fully async, so DB operations don't
    block the event loop while waiting for Postgres to respond.
"""

import json
from pathlib import Path

import asyncpg

from src.config import DB_DSN, DB_POOL_MIN, DB_POOL_MAX, DB_ACQUIRE_TIMEOUT
from src.logger import get_logger

logger = get_logger("db")

# Module-level pool singleton — shared across all pipeline phases.
# Never create a second pool; use get_pool() everywhere.
_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    """
    Return the shared asyncpg connection pool, creating it on first call.

    Pool sizing:
        min_size: keeps this many connections warm (avoids cold-start latency
                  on the first DB call of each phase).
        max_size: caps total connections so we don't exceed Postgres's
                  max_connections limit (default 100).

    Returns:
        asyncpg.Pool ready to use with `async with pool.acquire() as conn`.
    """
    global _pool
    if _pool is None:
        logger.info(
            f"Creating asyncpg pool | host={DB_DSN.split('@')[-1]} "
            f"min={DB_POOL_MIN} max={DB_POOL_MAX}"
        )
        _pool = await asyncpg.create_pool(
            DB_DSN,
            min_size=DB_POOL_MIN,
            max_size=DB_POOL_MAX,
            # command_timeout: abort any single query that takes longer than 60s.
            # Protects against runaway migrations or stuck REFRESH commands.
            command_timeout=60,
            # Register JSONB codec on every connection in the pool.
            # By default asyncpg returns JSONB columns as raw strings — doing
            # row['raw_payload']['id'] would then crash with "string indices must
            # be integers". This codec auto-decodes JSONB → Python dict/list on
            # read and encodes dict/list → JSON string on write, everywhere.
            init=_init_conn,
        )
    return _pool


async def _init_conn(conn: asyncpg.Connection) -> None:
    """
    Initialise each pool connection with a JSONB codec.

    Called automatically by asyncpg for every new connection in the pool.
    Registers json.loads as the decoder so JSONB columns come back as
    Python dicts/lists rather than raw JSON strings.
    """
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
        # format='text' is required — without it asyncpg uses binary encoding
        # which is incompatible with json.dumps/json.loads and silently returns
        # raw strings instead of decoded dicts.
        format="text",
    )


async def close_pool() -> None:
    """
    Gracefully close the connection pool at pipeline shutdown.

    Always call this in a finally block in main.py so connections are
    released cleanly even if the pipeline raises an exception.
    """
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.debug("Connection pool closed")


def acquire(pool: asyncpg.Pool):
    """
    Acquire a connection from the pool with a timeout.

    Use this everywhere instead of pool.acquire() directly.
    Without a timeout, pool.acquire() blocks forever when all connections
    are busy — this caps the wait at DB_ACQUIRE_TIMEOUT seconds and raises
    asyncio.TimeoutError, which the pipeline can catch and report clearly.

    Usage:
        async with acquire(pool) as conn:
            await conn.execute(...)
    """
    return pool.acquire(timeout=DB_ACQUIRE_TIMEOUT)


async def run_sql_file(pool: asyncpg.Pool, path: Path) -> None:
    """
    Read and execute a SQL file against the database.

    Used by main.py to run schema migrations (bronze/silver/gold DDL).
    Each file is executed as a single transaction so a partial migration
    either fully succeeds or fully rolls back.

    Args:
        pool: asyncpg connection pool.
        path: Absolute path to the .sql file.
    """
    sql = path.read_text()
    async with acquire(pool) as conn:
        await conn.execute(sql)
    logger.debug(f"Executed SQL file: {path.name}")
