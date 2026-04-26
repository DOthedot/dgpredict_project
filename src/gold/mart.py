"""
Gold layer mart builder.

Pipeline position: silver.* → [mart.py] → gold.market_enriched + gold.event_summary

Reads from  : silver.events, silver.markets, silver.series, silver.event_series
              (indirectly, via the materialized view definitions in sql/gold/schema.sql)

Writes to   : gold.market_enriched (materialized view — refreshed each run)
              gold.event_summary   (materialized view — refreshed each run)

Why REFRESH MATERIALIZED VIEW instead of recreating:
    Recreating the view on every run would require a DROP + CREATE, which locks
    the view and makes gold unavailable during the rebuild.
    REFRESH CONCURRENTLY keeps old data queryable while the new data is computed —
    zero downtime between pipeline runs.

First-run handling:
    CONCURRENTLY requires the view to have been populated at least once.
    Views are created WITH NO DATA in the schema DDL (empty on first migration).
    On the first pipeline run we detect the empty state via the exception raised
    by SELECT COUNT(*) and fall back to a plain REFRESH to seed it.
"""

import asyncpg
from src.db import acquire
from src.logger import get_logger

logger = get_logger("gold.mart")


async def refresh_gold(pool: asyncpg.Pool) -> None:
    """
    Refresh both gold materialized views from current silver data.

    Called at the end of every pipeline run after silver is fully built.
    Uses CONCURRENTLY so old gold data stays readable during the refresh.

    Args:
        pool: asyncpg connection pool.
    """
    async with acquire(pool) as conn:
        await _refresh_view(conn, "gold.market_enriched")
        await _refresh_view(conn, "gold.event_summary")

    logger.info("Gold layer refresh complete")


async def _refresh_view(conn: asyncpg.Connection, view_name: str) -> None:
    """
    Refresh a single materialized view, handling the first-run empty case.

    REFRESH CONCURRENTLY fails if the view has never been populated (created
    WITH NO DATA). We detect this by attempting SELECT COUNT(*) — Postgres raises
    ObjectNotInPrerequisiteStateError on an unpopulated view, which we catch to
    fall back to a plain REFRESH. All subsequent runs use CONCURRENTLY.

    Args:
        conn:      asyncpg connection.
        view_name: Fully qualified view name (e.g. 'gold.market_enriched').
    """
    try:
        # This raises ObjectNotInPrerequisiteStateError if the view has never
        # been populated (created WITH NO DATA). On success, view is already
        # seeded and we can safely use REFRESH CONCURRENTLY.
        await conn.fetchval(f"SELECT COUNT(*) FROM {view_name}")

        # View is populated — CONCURRENTLY keeps old rows queryable during rebuild.
        # Requires the unique index created in sql/gold/schema.sql.
        logger.info(f"Refreshing {view_name} CONCURRENTLY ...")
        await conn.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name}")

    except Exception:
        # First run: view was created WITH NO DATA and has never been refreshed.
        # Plain REFRESH seeds it; all future runs will take the CONCURRENTLY path.
        logger.info(f"{view_name} not yet populated — using plain REFRESH to seed it")
        await conn.execute(f"REFRESH MATERIALIZED VIEW {view_name}")

    new_count = await conn.fetchval(f"SELECT COUNT(*) FROM {view_name}")
    logger.info(f"{view_name}: {new_count} rows after refresh")
