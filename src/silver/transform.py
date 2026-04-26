"""
Silver layer transformer.

Pipeline position: bronze.raw_* → [transform.py] → silver.*

All transformations run as server-side SQL INSERT...SELECT statements.
No data leaves PostgreSQL — bronze rows are read, transformed, and written
entirely within the database engine, eliminating the ETL container round-trip.

SQL type coercions used:
    Timestamps : NULLIF(raw_payload->>'field', '')::timestamptz
    Numerics   : COALESCE(NULLIF(raw_payload->>'field', '')::numeric, 0)
    Booleans   : COALESCE((raw_payload->>'field')::bool, false)
    JSONB      : NULLIF(raw_payload->>'field', '')::jsonb
                 Works for both native JSONB arrays and JSON-encoded strings
                 ("[\\"Yes\\", \\"No\\"]") — Postgres parses both.
    Text       : raw_payload->>'field'

Insert order is critical for foreign key integrity:
    series → events → event_series → markets
"""

import asyncpg

from src.db import acquire
from src.logger import get_logger

logger = get_logger("silver.transform")


async def build_silver(pool: asyncpg.Pool, run_id: str) -> None:
    """
    Transform all bronze payloads for the given run into clean silver tables.

    Each step is a single INSERT...SELECT that runs entirely inside Postgres.
    """
    async with acquire(pool) as conn:
        await _transform_series(conn, run_id)
        await _transform_events(conn, run_id)
        await _transform_event_series(conn, run_id)
        await _transform_markets(conn, run_id)


async def _transform_series(conn: asyncpg.Connection, run_id: str) -> None:
    """Upsert series directly from bronze.raw_series → silver.series."""
    result = await conn.execute(
        """
        INSERT INTO silver.series
            (series_id, slug, title, series_type, recurrence, active, closed,
             volume24hr, volume, liquidity, created_at, updated_at)
        SELECT
            raw_payload->>'id',
            raw_payload->>'slug',
            TRIM(COALESCE(raw_payload->>'title', '')),
            raw_payload->>'seriesType',
            raw_payload->>'recurrence',
            COALESCE((raw_payload->>'active')::bool, false),
            COALESCE((raw_payload->>'closed')::bool, false),
            COALESCE(NULLIF(raw_payload->>'volume24hr', '')::numeric, 0),
            COALESCE(NULLIF(raw_payload->>'volume',     '')::numeric, 0),
            COALESCE(NULLIF(raw_payload->>'liquidity',  '')::numeric, 0),
            NULLIF(raw_payload->>'createdAt', '')::timestamptz,
            NULLIF(raw_payload->>'updatedAt', '')::timestamptz
        FROM bronze.raw_series
        WHERE run_id = $1
        ON CONFLICT (series_id) DO UPDATE SET
            title       = EXCLUDED.title,
            active      = EXCLUDED.active,
            closed      = EXCLUDED.closed,
            volume24hr  = EXCLUDED.volume24hr,
            volume      = EXCLUDED.volume,
            liquidity   = EXCLUDED.liquidity,
            updated_at  = EXCLUDED.updated_at,
            ingested_at = NOW()
        """,
        run_id,
    )
    count = int(result.split()[-1])
    if count == 0:
        logger.info("No series in bronze for this run — skipping")
    else:
        logger.info(f"Silver series: upserted {count} rows")


async def _transform_events(conn: asyncpg.Connection, run_id: str) -> None:
    """Upsert events directly from bronze.raw_events → silver.events."""
    result = await conn.execute(
        """
        INSERT INTO silver.events
            (event_id, ticker, slug, title, description, game_start_time,
             end_date, created_at, updated_at, active, closed, neg_risk, volume, series_slug)
        SELECT
            raw_payload->>'id',
            raw_payload->>'ticker',
            raw_payload->>'slug',
            TRIM(COALESCE(raw_payload->>'title', '')),
            raw_payload->>'description',
            -- startTime (gameStartTime) is the actual match kickoff.
            -- COALESCE tries startTime first, falls back to gameStartTime.
            NULLIF(
                COALESCE(
                    NULLIF(raw_payload->>'startTime',     ''),
                    NULLIF(raw_payload->>'gameStartTime', '')
                ), ''
            )::timestamptz,
            NULLIF(raw_payload->>'endDate',   '')::timestamptz,
            NULLIF(raw_payload->>'createdAt', '')::timestamptz,
            NULLIF(raw_payload->>'updatedAt', '')::timestamptz,
            COALESCE((raw_payload->>'active')::bool,  false),
            COALESCE((raw_payload->>'closed')::bool,  false),
            COALESCE((raw_payload->>'negRisk')::bool, false),
            COALESCE(NULLIF(raw_payload->>'volume', '')::numeric, 0),
            raw_payload->>'seriesSlug'
        FROM bronze.raw_events
        WHERE run_id = $1
        ON CONFLICT (event_id) DO UPDATE SET
            title       = EXCLUDED.title,
            active      = EXCLUDED.active,
            closed      = EXCLUDED.closed,
            volume      = EXCLUDED.volume,
            updated_at  = EXCLUDED.updated_at,
            ingested_at = NOW()
        """,
        run_id,
    )
    count = int(result.split()[-1])
    if count == 0:
        logger.warning("No events in bronze for this run — skipping")
    else:
        logger.info(f"Silver events: upserted {count} rows")


async def _transform_event_series(conn: asyncpg.Connection, run_id: str) -> None:
    """
    Build the event_series junction from nested series[] in bronze.raw_events.

    WITH ORDINALITY assigns a 1-based index to each element of the series array.
    ordinality = 1 marks is_primary = TRUE, matching the original Python idx == 0 logic.
    """
    result = await conn.execute(
        """
        INSERT INTO silver.event_series (event_id, series_id, is_primary)
        SELECT
            raw_payload->>'id',
            s_elem->>'id',
            (ordinality = 1)
        FROM bronze.raw_events,
             LATERAL jsonb_array_elements(
                 COALESCE(raw_payload->'series', '[]'::jsonb)
             ) WITH ORDINALITY AS t(s_elem, ordinality)
        WHERE run_id = $1
          AND jsonb_array_length(COALESCE(raw_payload->'series', '[]'::jsonb)) > 0
        ON CONFLICT (event_id, series_id) DO UPDATE SET
            is_primary = EXCLUDED.is_primary
        """,
        run_id,
    )
    count = int(result.split()[-1])
    if count == 0:
        logger.info("No event-series relationships found — skipping")
    else:
        logger.info(f"Silver event_series: upserted {count} relationships")


async def _transform_markets(conn: asyncpg.Connection, run_id: str) -> None:
    """
    Upsert markets directly from bronze.raw_markets → silver.markets.

    volumeNum/liquidityNum are preferred over volume/liquidity (which the API
    returns as strings in event-nested markets). COALESCE tries the float field
    first, falls back to the string field, then defaults to 0.

    outcomes/outcomePrices: the API sometimes returns these as JSON-encoded strings
    ("[\"Yes\", \"No\"]") and sometimes as native arrays. Extracting via ->> and
    re-casting with ::jsonb normalises both forms inside Postgres.
    """
    result = await conn.execute(
        """
        INSERT INTO silver.markets
            (condition_id, market_id, event_id, question, slug, end_date, game_start_time,
             volume, volume24hr, liquidity, active, closed, accepting_orders, neg_risk,
             outcomes, outcome_prices, clob_token_ids,
             last_trade_price, best_bid, best_ask, created_at, updated_at)
        SELECT
            COALESCE(NULLIF(raw_payload->>'conditionId', ''), 'market_' || (raw_payload->>'id')),
            raw_payload->>'id',
            raw_payload->>'_event_id',
            raw_payload->>'question',
            raw_payload->>'slug',
            NULLIF(raw_payload->>'endDate',       '')::timestamptz,
            NULLIF(raw_payload->>'gameStartTime', '')::timestamptz,
            -- Prefer volumeNum (float field), fall back to volume (string field)
            COALESCE(
                NULLIF(raw_payload->>'volumeNum', '')::numeric,
                NULLIF(raw_payload->>'volume',    '')::numeric,
                0
            ),
            COALESCE(NULLIF(raw_payload->>'volume24hr',   '')::numeric, 0),
            -- Prefer liquidityNum (float field), fall back to liquidity (string field)
            COALESCE(
                NULLIF(raw_payload->>'liquidityNum', '')::numeric,
                NULLIF(raw_payload->>'liquidity',    '')::numeric,
                0
            ),
            COALESCE((raw_payload->>'active')::bool,          false),
            COALESCE((raw_payload->>'closed')::bool,          false),
            COALESCE((raw_payload->>'acceptingOrders')::bool, false),
            COALESCE((raw_payload->>'negRisk')::bool,         false),
            -- ->> extracts as text then ::jsonb re-parses, normalising both
            -- native JSONB arrays and JSON-encoded string forms from the API.
            NULLIF(raw_payload->>'outcomes',      '')::jsonb,
            NULLIF(raw_payload->>'outcomePrices', '')::jsonb,
            NULLIF(raw_payload->>'clobTokenIds',  '')::jsonb,
            COALESCE(NULLIF(raw_payload->>'lastTradePrice', '')::numeric, 0),
            COALESCE(NULLIF(raw_payload->>'bestBid',        '')::numeric, 0),
            COALESCE(NULLIF(raw_payload->>'bestAsk',        '')::numeric, 0),
            NULLIF(raw_payload->>'createdAt', '')::timestamptz,
            NULLIF(raw_payload->>'updatedAt', '')::timestamptz
        FROM bronze.raw_markets
        WHERE run_id = $1
        ON CONFLICT (condition_id) DO UPDATE SET
            volume           = EXCLUDED.volume,
            volume24hr       = EXCLUDED.volume24hr,
            liquidity        = EXCLUDED.liquidity,
            active           = EXCLUDED.active,
            closed           = EXCLUDED.closed,
            accepting_orders = EXCLUDED.accepting_orders,
            last_trade_price = EXCLUDED.last_trade_price,
            best_bid         = EXCLUDED.best_bid,
            best_ask         = EXCLUDED.best_ask,
            updated_at       = EXCLUDED.updated_at,
            ingested_at      = NOW()
        """,
        run_id,
    )
    count = int(result.split()[-1])
    if count == 0:
        logger.warning("No markets in bronze for this run — skipping")
    else:
        logger.info(f"Silver markets: upserted {count} rows")
