-- =============================================================================
-- Silver Layer Schema
-- =============================================================================
-- Purpose : Cleaned, typed, relational tables derived from bronze raw payloads.
--           Silver is the single source of truth for gold — gold never reads
--           from bronze directly.
--
-- Design  : Natural primary keys (event_id, condition_id, series_id).
--           All timestamps stored as TIMESTAMPTZ (UTC).
--           Nested API fields (outcomes, prices, token IDs) stored as JSONB.
--           Upsert on natural key: reruns always reflect the latest API state.
--
-- Relationships:
--           silver.series       ← referenced by silver.event_series
--           silver.events       ← referenced by silver.event_series + silver.markets
--           silver.event_series ← junction table (N:N between events and series)
--           silver.markets      ← child of silver.events
--
-- Insert order matters for FK integrity:
--           series → events → event_series → markets
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS silver;

-- ---------------------------------------------------------------------------
-- Series
-- A series groups related recurring events (e.g. "Bolivia 1" league).
-- Created before events because event_series references it.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.series (
    series_id   TEXT        PRIMARY KEY,
    slug        TEXT,
    title       TEXT,
    series_type TEXT,                   -- 'single' | 'multi' etc.
    recurrence  TEXT,                   -- 'daily' | 'weekly' etc.
    active      BOOL,
    closed      BOOL,
    volume24hr  NUMERIC(20, 6),
    volume      NUMERIC(20, 6),
    liquidity   NUMERIC(20, 6),
    created_at  TIMESTAMPTZ,
    updated_at  TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Events
-- One row per football event (match). Upserted on event_id so reruns
-- update fields like active/closed/volume rather than creating duplicates.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.events (
    event_id        TEXT        PRIMARY KEY,
    ticker          TEXT,
    slug            TEXT,
    title           TEXT,
    description     TEXT,
    -- game_start_time: the actual match kickoff (from gameStartTime field).
    -- More useful than startDate (market creation time) or endDate (resolution deadline).
    game_start_time TIMESTAMPTZ,
    end_date        TIMESTAMPTZ,
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ,
    active          BOOL,
    closed          BOOL,
    -- neg_risk: football events use negRisk market structure where all outcomes
    -- share one liquidity pool. Important for understanding volume aggregation.
    neg_risk        BOOL,
    volume          NUMERIC(20, 6),
    series_slug     TEXT,               -- links to series.slug for reference
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_series_slug ON silver.events (series_slug);
CREATE INDEX IF NOT EXISTS idx_events_active       ON silver.events (active, closed);

-- ---------------------------------------------------------------------------
-- Event ↔ Series junction table
-- An event can belong to multiple series (e.g. a match in both a league
-- and a cup). is_primary = TRUE marks the first/main series so gold layer
-- joins only one series per event without duplicating rows.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.event_series (
    event_id    TEXT NOT NULL REFERENCES silver.events(event_id)  ON DELETE CASCADE,
    series_id   TEXT NOT NULL REFERENCES silver.series(series_id) ON DELETE CASCADE,
    -- is_primary ensures gold can LEFT JOIN exactly one series per event.
    -- Without this flag, an event in 2 series would produce 2 rows in
    -- market_enriched per market — doubling all volume figures silently.
    is_primary  BOOL NOT NULL DEFAULT FALSE,

    PRIMARY KEY (event_id, series_id)
);

CREATE INDEX IF NOT EXISTS idx_event_series_series_id  ON silver.event_series (series_id);
CREATE INDEX IF NOT EXISTS idx_event_series_is_primary ON silver.event_series (event_id, is_primary);

-- ---------------------------------------------------------------------------
-- Markets
-- One row per market (a single outcome question within an event).
-- condition_id is the natural key — a hex string that uniquely identifies
-- the market on the Polymarket CLOB. market_id is the numeric internal ID.
--
-- volume/liquidity: stored as NUMERIC because the API returns them as strings
-- ("1057.074537"). We cast via volumeNum/liquidityNum floats in the transform.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.markets (
    condition_id     TEXT        PRIMARY KEY,    -- hex PK from API (conditionId)
    market_id        TEXT        NOT NULL,       -- numeric internal ID
    event_id         TEXT        NOT NULL REFERENCES silver.events(event_id) ON DELETE CASCADE,
    question         TEXT,
    slug             TEXT,
    end_date         TIMESTAMPTZ,
    game_start_time  TIMESTAMPTZ,

    -- Volume and liquidity — use NUMERIC for financial precision.
    -- volume: lifetime total trading volume (USDC)
    -- volume24hr: rolling 24-hour trading volume (fetched from /markets endpoint)
    -- liquidity: current order book depth (USDC)
    volume           NUMERIC(20, 6),
    volume24hr       NUMERIC(20, 6),
    liquidity        NUMERIC(20, 6),

    active           BOOL,
    closed           BOOL,
    accepting_orders BOOL,
    neg_risk         BOOL,

    -- Nested API arrays stored as JSONB — avoids complex child tables for
    -- binary Yes/No outcomes. Easy to query: outcomes->0 = 'Yes'.
    outcomes         JSONB,
    outcome_prices   JSONB,
    clob_token_ids   JSONB,

    last_trade_price NUMERIC(10, 6),
    best_bid         NUMERIC(10, 6),
    best_ask         NUMERIC(10, 6),

    created_at       TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_markets_event_id        ON silver.markets (event_id);
CREATE INDEX IF NOT EXISTS idx_markets_active          ON silver.markets (active, closed);
CREATE INDEX IF NOT EXISTS idx_markets_volume24hr      ON silver.markets (volume24hr DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_markets_accepting_orders ON silver.markets (accepting_orders);
