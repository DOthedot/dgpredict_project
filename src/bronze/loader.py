"""
Bronze layer loader.

Pipeline position: fetcher callbacks → [loader.py] → bronze.raw_events / raw_markets / raw_series / fetch_errors

Reads from:
    - Events, series, and markets streamed per-chunk from src/ingestion/fetcher.py
      via the on_events_chunk and on_markets_chunk callbacks.

Writes to:
    - bronze.raw_events   — one row per event, full JSON payload preserved as JSONB
    - bronze.raw_markets  — one row per market (Pass 1 fields + volume24hr from Pass 2)
    - bronze.raw_series   — one row per unique series (deduplicated by fetcher)
    - bronze.fetch_errors — dead-letter table for all failed API fetches

Design:
    - Streaming writes: each chunk is written immediately as it arrives from the API.
      Full records are never accumulated in Python memory — only counts are tracked.
      Memory stays flat at ~1 chunk at a time regardless of dataset size.
    - raw_events / raw_series use ON CONFLICT DO NOTHING — same run_id means the
      row is already written (crash-recovery checkpoint). No point overwriting with
      identical data.
    - raw_markets uses ON CONFLICT DO UPDATE — if the fetcher later gets a better
      record for the same market (e.g. real volume24hr vs the fallback 0), the update
      ensures the richer data wins over the partial row.
    - Raw payloads stored as JSONB — silver can re-derive from bronze without
      hitting the API again if transforms need to change.
    - fetch_errors uses ON CONFLICT DO UPDATE so retrying a failed entity updates
      the error record rather than accumulating duplicates.
"""

import asyncio

import asyncpg
import pendulum

from src.config import GAMMA_BASE_URL
from src.db import acquire
from src.ingestion.fetcher import FetchError
from src.logger import get_logger

logger = get_logger("bronze.loader")


async def load_events_series_chunk(
    pool: asyncpg.Pool,
    run_id: str,
    events: list[dict],
    series: list[dict],
    now: pendulum.DateTime,
) -> None:
    """
    Write one chunk of events + series to bronze immediately after it arrives.

    Called per-chunk from the fetcher callback. Events and series are written
    via separate connections so they run concurrently.

    Args:
        pool:   asyncpg connection pool.
        run_id: Pipeline run identifier.
        events: Event dicts from one API chunk response.
        series: Deduplicated series dicts from this chunk.
        now:    Consistent ingestion timestamp for this pipeline run.
    """
    async def _ev():
        async with acquire(pool) as conn:
            await _load_raw_events(conn, run_id, events, now)

    async def _sr():
        async with acquire(pool) as conn:
            await _load_raw_series(conn, run_id, series, now)

    await asyncio.gather(_ev(), _sr())


async def load_markets_chunk(
    pool: asyncpg.Pool,
    run_id: str,
    markets: list[dict],
    now: pendulum.DateTime,
) -> None:
    """
    Write one chunk of markets to bronze immediately after the fetcher processes them.

    Each market record has been enriched by the fetcher:
        - If /markets returned the market: real volume24hr from the API.
        - If /markets did not return it: volume24hr=0 fallback (Pass 1 data only).

    ON CONFLICT DO UPDATE ensures a later richer record overwrites an earlier
    partial one within the same run — no data is silently lost.

    Args:
        pool:    asyncpg connection pool.
        run_id:  Pipeline run identifier.
        markets: Enriched market dicts (Pass 1 fields merged with Pass 2 volume24hr).
        now:     Consistent ingestion timestamp for this pipeline run.
    """
    async with acquire(pool) as conn:
        await _load_raw_markets(conn, run_id, markets, now)


async def load_fetch_errors(
    pool: asyncpg.Pool,
    run_id: str,
    errors: list[FetchError],
    now: pendulum.DateTime,
) -> None:
    """
    Dead-letter all failed fetches into bronze.fetch_errors.

    Called once after all chunks complete — errors from all chunks are
    accumulated in FetchResult.errors and written in one shot at the end.

    ON CONFLICT DO UPDATE: rerunning the same run_id updates the error
    record rather than accumulating duplicates.
    """
    if not errors:
        return
    async with acquire(pool) as conn:
        await _load_fetch_errors(conn, run_id, errors, now)


async def _load_raw_events(
    conn: asyncpg.Connection,
    run_id: str,
    events: list[dict],
    now: pendulum.DateTime,
) -> None:
    if not events:
        return

    rows = [
        (
            run_id,
            str(event["id"]),
            f"{GAMMA_BASE_URL}/events",
            now,
            event,
            {"id": event["id"]},
        )
        for event in events
    ]

    await conn.executemany(
        """
        INSERT INTO bronze.raw_events
            (run_id, event_id, source_endpoint, ingested_at, raw_payload, request_params)
        VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb)
        ON CONFLICT (run_id, event_id) DO NOTHING
        """,
        rows,
    )
    logger.info(f"Bronze raw_events: {len(rows)} rows written | run_id={run_id}")


async def _load_raw_markets(
    conn: asyncpg.Connection,
    run_id: str,
    markets: list[dict],
    now: pendulum.DateTime,
) -> None:
    if not markets:
        return

    rows = [
        (
            run_id,
            str(market["id"]),
            str(market.get("_event_id", "")),
            f"{GAMMA_BASE_URL}/markets",
            now,
            market,
            {"id": market["id"]},
        )
        for market in markets
    ]

    await conn.executemany(
        """
        INSERT INTO bronze.raw_markets
            (run_id, market_id, event_id, source_endpoint, ingested_at, raw_payload, request_params)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb)
        ON CONFLICT (run_id, market_id) DO UPDATE SET
            raw_payload  = EXCLUDED.raw_payload,
            ingested_at  = EXCLUDED.ingested_at
        """,
        rows,
    )
    logger.info(f"Bronze raw_markets: {len(rows)} rows written | run_id={run_id}")


async def _load_raw_series(
    conn: asyncpg.Connection,
    run_id: str,
    series: list[dict],
    now: pendulum.DateTime,
) -> None:
    if not series:
        return

    rows = [
        (
            run_id,
            str(s["id"]),
            f"{GAMMA_BASE_URL}/events",
            now,
            s,
        )
        for s in series
    ]

    await conn.executemany(
        """
        INSERT INTO bronze.raw_series
            (run_id, series_id, source_endpoint, ingested_at, raw_payload)
        VALUES ($1, $2, $3, $4, $5::jsonb)
        ON CONFLICT (run_id, series_id) DO NOTHING
        """,
        rows,
    )
    logger.info(f"Bronze raw_series: {len(rows)} rows written | run_id={run_id}")


async def _load_fetch_errors(
    conn: asyncpg.Connection,
    run_id: str,
    errors: list[FetchError],
    now: pendulum.DateTime,
) -> None:
    if not errors:
        return

    rows = [
        (
            run_id,
            e.entity_type,
            e.entity_id,
            e.endpoint,
            e.error_type,
            e.error_message or "",
            e.retry_count,
            now,
        )
        for e in errors
    ]

    await conn.executemany(
        """
        INSERT INTO bronze.fetch_errors
            (run_id, entity_type, entity_id, endpoint, error_type, error_message, retry_count, attempted_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (run_id, entity_type, entity_id) DO UPDATE SET
            error_type    = EXCLUDED.error_type,
            error_message = EXCLUDED.error_message,
            retry_count   = EXCLUDED.retry_count,
            attempted_at  = EXCLUDED.attempted_at
        """,
        rows,
    )
    logger.warning(
        f"Bronze fetch_errors: {len(errors)} failures dead-lettered | run_id={run_id}"
    )
