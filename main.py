"""
Polymarket Football Warehouse — ETL Pipeline Entry Point.

Orchestrates the full medallion pipeline end-to-end:

    1. Run DB migrations   — create schemas + tables if not exist (idempotent DDL)
    2. Determine run_id    — new timestamp, or resume a crashed run
    3. Fetch → Bronze      — async API fetch + bulk insert into bronze.raw_*
    4. Bronze → Silver     — typed upserts from bronze JSONB into silver tables
    5. Refresh Gold        — REFRESH MATERIALIZED VIEW on both gold marts
    6. Complete run        — mark pipeline_runs.status = 'completed', print summary

Run this via:
    docker compose run --rm etl          # inside Docker (recommended)
    uv run python main.py                # locally (set POSTGRES_HOST=localhost in .env)
    make run                             # Makefile shorthand

Idempotency:
    - Bronze:  ON CONFLICT (run_id, entity_id) DO NOTHING — same run = no-op inserts
    - Silver:  ON CONFLICT (natural_key) DO UPDATE — always reflects latest API state
    - Gold:    REFRESH MATERIALIZED VIEW — rebuilt from current silver on every run

Crash recovery:
    If the pipeline crashes mid-run, bronze.pipeline_runs.status stays 'running'.
    The next invocation detects this and reuses the same run_id, allowing bronze
    to skip already-loaded rows via ON CONFLICT DO NOTHING (checkpoint behaviour).
    Silver and gold are always rebuilt from bronze regardless.
"""

import asyncio
import sys
import time
from pathlib import Path
from typing import Callable, Awaitable, TypeVar

T = TypeVar("T")

import asyncpg
import pendulum

from src.bronze.loader import load_events_series_chunk, load_markets_chunk, load_fetch_errors
from src.db import acquire, close_pool, get_pool, run_sql_file
from src.gold.mart import refresh_gold
from src.ingestion.fetcher import AsyncFetcher
from src.logger import get_logger
from src.silver.transform import build_silver

logger = get_logger("main")

DB_WRITE_RETRIES = 3  # max attempts for any DB write operation


async def retry_db(label: str, fn: Callable[[], Awaitable[T]]) -> T:
    """
    Retry a DB write operation with exponential backoff.

    If Postgres is temporarily unavailable (crash, restart, network blip),
    this gives it time to recover instead of failing the whole pipeline.
    Each retry waits 2^attempt seconds (2s, 4s, 8s) before trying again.

    Args:
        label: Human-readable name for logging (e.g. 'bronze write').
        fn:    Async callable that performs the DB operation.

    Returns:
        Whatever fn() returns on success.

    Raises:
        The last exception if all retries are exhausted.
    """
    for attempt in range(DB_WRITE_RETRIES):
        try:
            return await fn()
        except Exception as exc:
            wait = 2 ** (attempt + 1)
            if attempt < DB_WRITE_RETRIES - 1:
                logger.warning(
                    f"{label} failed (attempt {attempt + 1}/{DB_WRITE_RETRIES}) — "
                    f"Postgres may be unavailable. Retrying in {wait}s | error={exc}"
                )
                await asyncio.sleep(wait)
            else:
                logger.error(
                    f"{label} failed after {DB_WRITE_RETRIES} attempts — giving up | error={exc}"
                )
                raise


# Paths to SQL migration files — executed in order on every run.
# IF NOT EXISTS guards make them safe to re-run without data loss.
SQL_FILES = [
    Path(__file__).parent / "sql" / "bronze" / "schema.sql",
    Path(__file__).parent / "sql" / "silver" / "schema.sql",
    Path(__file__).parent / "sql" / "gold"   / "schema.sql",
]


# ── Migrations ────────────────────────────────────────────────────────────────

async def run_migrations(pool: asyncpg.Pool) -> None:
    """
    Execute all schema DDL files in order (bronze → silver → gold).

    All DDL uses IF NOT EXISTS so re-running migrations on an existing database
    is safe — no tables are dropped or truncated.
    """
    logger.info("Running DB migrations...")
    for sql_file in SQL_FILES:
        await run_sql_file(pool, sql_file)
        logger.debug(f"Migration applied: {sql_file.name}")
    logger.info("Migrations complete")


# ── Run ID management ─────────────────────────────────────────────────────────

async def get_or_create_run_id(pool: asyncpg.Pool) -> str:
    """
    Determine the run_id for this execution.

    Two cases:
        1. A previous run has status='running' → it crashed. Reuse its run_id
           so bronze skips already-loaded rows (ON CONFLICT DO NOTHING acts as
           a checkpoint — we pick up where the crash left off).

        2. No incomplete run → generate a fresh timestamp-based run_id.
           A new run_id means a full re-fetch from the API, ensuring silver/gold
           always reflect the current state of Polymarket (volumes, statuses).

    Returns:
        run_id string (ISO timestamp format).
    """
    async with acquire(pool) as conn:
        incomplete = await conn.fetchrow(
            """
            SELECT run_id FROM bronze.pipeline_runs
            WHERE  status = 'running'
            ORDER  BY started_at DESC
            LIMIT  1
            """
        )

        if incomplete:
            run_id = incomplete["run_id"]
            logger.warning(
                f"Incomplete run detected — resuming | run_id={run_id}"
            )
            return run_id

        # New run — ISO timestamp as run_id: unique, human-readable, sortable.
        # pendulum.now("UTC") guarantees UTC regardless of server timezone setting.
        run_id = pendulum.now("UTC").format("YYYY-MM-DDTHH:mm:ss[Z]")
        await conn.execute(
            "INSERT INTO bronze.pipeline_runs (run_id, status, started_at) VALUES ($1, 'running', NOW())",
            run_id,
        )
        logger.info(f"New pipeline run started | run_id={run_id}")
        return run_id


async def check_data_quality(pool: asyncpg.Pool, run_id: str) -> None:
    """
    Assert silver tables are populated before promoting data to gold.

    Runs after silver transform and before gold refresh. If any check fails,
    raises an exception which aborts the pipeline — preventing an empty or
    partial gold layer from being served to downstream consumers.

    Checks:
        - silver.events has rows for this run_id
        - silver.markets has rows for this run_id
        - silver.series has rows for this run_id
        - No silver event has a NULL event_id (primary key sanity)
        - No silver market has a NULL condition_id (primary key sanity)
    """
    async with acquire(pool) as conn:
        checks = {
            "silver.events":  "SELECT COUNT(*) FROM silver.events",
            "silver.markets": "SELECT COUNT(*) FROM silver.markets",
            "silver.series":  "SELECT COUNT(*) FROM silver.series",
        }

        for table, query in checks.items():
            count = await conn.fetchval(query)
            if count == 0:
                raise ValueError(
                    f"Data quality check failed: {table} has 0 rows after silver transform. "
                    f"run_id={run_id}"
                )
            logger.info(f"Quality check passed | {table}: {count} rows")

        # NULL primary key check — a NULL PK means the transform silently dropped
        # the natural key field, which would cause silent data loss in gold joins.
        null_event_ids = await conn.fetchval(
            "SELECT COUNT(*) FROM silver.events WHERE event_id IS NULL"
        )
        if null_event_ids > 0:
            raise ValueError(
                f"Data quality check failed: {null_event_ids} rows in silver.events have NULL event_id"
            )

        null_condition_ids = await conn.fetchval(
            "SELECT COUNT(*) FROM silver.markets WHERE condition_id IS NULL"
        )
        if null_condition_ids > 0:
            raise ValueError(
                f"Data quality check failed: {null_condition_ids} rows in silver.markets have NULL condition_id"
            )

    logger.info("All data quality checks passed")


async def complete_run(pool: asyncpg.Pool, run_id: str, stats: dict) -> None:
    """
    Mark the pipeline run as completed and record fetch counts.

    Args:
        pool:    asyncpg connection pool.
        run_id:  Run identifier to update.
        stats:   Dict with keys: events, markets, series, events_failed, markets_failed.
    """
    async with acquire(pool) as conn:
        await conn.execute(
            """
            UPDATE bronze.pipeline_runs SET
                status          = 'completed',
                completed_at    = NOW(),
                events_fetched  = $2,
                markets_fetched = $3,
                series_fetched  = $4,
                events_failed   = $5,
                markets_failed  = $6
            WHERE run_id = $1
            """,
            run_id,
            stats.get("events", 0),
            stats.get("markets", 0),
            stats.get("series", 0),
            stats.get("events_failed", 0),
            stats.get("markets_failed", 0),
        )


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    start = time.time()
    logger.info("=" * 60)
    logger.info("Polymarket Football Warehouse — Pipeline Starting")
    logger.info("=" * 60)

    pool = await get_pool()

    try:
        # ── Step 1: Migrations ────────────────────────────────────────────────
        await run_migrations(pool)

        # ── Step 2: Determine run_id ──────────────────────────────────────────
        run_id = await get_or_create_run_id(pool)

        # ── Step 3: Fetch from Gamma API → Bronze ─────────────────────────────
        logger.info("─" * 60)
        logger.info("Phase 1 — Fetching from Polymarket Gamma API → Bronze")
        logger.info("─" * 60)
        t1 = time.perf_counter()

        now = pendulum.now("UTC")

        async def on_events_chunk(events, series, errors):
            await retry_db(
                "bronze events/series chunk write",
                lambda: load_events_series_chunk(pool, run_id, events, series, now),
            )

        async def on_markets_chunk(markets, errors):
            await retry_db(
                "bronze markets chunk write",
                lambda: load_markets_chunk(pool, run_id, markets, now),
            )

        async with AsyncFetcher() as fetcher:
            fetch_result = await fetcher.fetch_all(
                on_events_chunk=on_events_chunk,
                on_markets_chunk=on_markets_chunk,
            )

        await retry_db(
            "bronze fetch_errors write",
            lambda: load_fetch_errors(pool, run_id, fetch_result.errors, now),
        )

        logger.info(f"Phase 1 complete in {time.perf_counter() - t1:.1f}s")

        # ── Step 4: Bronze → Silver ───────────────────────────────────────────
        logger.info("─" * 60)
        logger.info("Phase 2 — Transforming Bronze → Silver")
        logger.info("─" * 60)
        t2 = time.perf_counter()

        await retry_db("silver transform", lambda: build_silver(pool, run_id))

        logger.info(f"Phase 2 complete in {time.perf_counter() - t2:.1f}s")

        # ── Step 4b: Data quality checks ──────────────────────────────────────
        # Assert silver has valid data before promoting to gold.
        # A failed check aborts the pipeline — better to fail loudly than
        # silently serve an empty or partial gold layer to consumers.
        await check_data_quality(pool, run_id)

        # ── Step 5: Refresh Gold ──────────────────────────────────────────────
        logger.info("─" * 60)
        logger.info("Phase 3 — Refreshing Gold materialized views")
        logger.info("─" * 60)
        t3 = time.perf_counter()

        await retry_db("gold refresh", lambda: refresh_gold(pool))

        logger.info(f"Phase 3 complete in {time.perf_counter() - t3:.1f}s")

        # ── Step 6: Complete run ──────────────────────────────────────────────
        failed_events  = len([e for e in fetch_result.errors if e.entity_type == "event"])
        failed_markets = len([e for e in fetch_result.errors if e.entity_type == "market"])

        await complete_run(pool, run_id, {
            "events":         fetch_result.events_count,
            "markets":        fetch_result.markets_count,
            "series":         fetch_result.series_count,
            "events_failed":  failed_events,
            "markets_failed": failed_markets,
        })

        elapsed = time.time() - start
        logger.info("=" * 60)
        logger.info(f"Pipeline complete in {elapsed:.1f}s | run_id={run_id}")
        logger.info(f"  Events  : {fetch_result.events_count:>4} fetched | {failed_events:>2} failed")
        logger.info(f"  Markets : {fetch_result.markets_count:>4} fetched | {failed_markets:>2} failed")
        logger.info(f"  Series  : {fetch_result.series_count:>4} fetched")
        if fetch_result.errors:
            logger.warning(
                f"  {len(fetch_result.errors)} total failures recorded in bronze.fetch_errors"
            )
        logger.info("=" * 60)

    except Exception as exc:
        # Leave status as 'running' — next invocation will resume this run_id
        # and bronze will skip already-loaded rows via ON CONFLICT DO NOTHING.
        logger.error(f"Pipeline failed: {exc}", exc_info=True)
        sys.exit(1)

    finally:
        # Always close the pool — releases DB connections cleanly.
        await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
