-- =============================================================================
-- Bronze Layer Schema
-- =============================================================================
-- Purpose : Raw ingestion layer — preserves API payloads exactly as received.
--           Nothing is transformed here; bronze is the source of truth for
--           re-deriving silver without hitting the API again.
--
-- Design  : One table per API source endpoint.
--           All tables are regular (logged) so bronze payloads survive crashes
--           and remain queryable across runs for historical auditing.
--           pipeline_runs and fetch_errors are also logged for recovery.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS bronze;

-- ---------------------------------------------------------------------------
-- Pipeline run tracker
-- Tracks every ETL execution. Status stays 'running' if the pipeline crashes,
-- which the next run detects to resume from the checkpoint (skip already-loaded
-- bronze rows via ON CONFLICT DO NOTHING).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.pipeline_runs (
    run_id          TEXT        PRIMARY KEY,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    status          TEXT        NOT NULL DEFAULT 'running', -- 'running' | 'completed' | 'failed'
    events_fetched  INT         NOT NULL DEFAULT 0,
    markets_fetched INT         NOT NULL DEFAULT 0,
    series_fetched  INT         NOT NULL DEFAULT 0,
    events_failed   INT         NOT NULL DEFAULT 0,
    markets_failed  INT         NOT NULL DEFAULT 0
);

-- ---------------------------------------------------------------------------
-- Raw events — one row per event per run
-- raw_payload preserves the full API JSON including nested markets[] and series[].
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_events (
    id              BIGSERIAL   PRIMARY KEY,
    run_id          TEXT        NOT NULL,
    event_id        TEXT        NOT NULL,
    source_endpoint TEXT        NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_payload     JSONB       NOT NULL,
    request_params  JSONB,

    -- Unique on (run_id, event_id) enables ON CONFLICT DO NOTHING for safe reruns:
    -- rerunning the same run_id skips already-loaded events (checkpoint recovery).
    UNIQUE (run_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_events_run_id ON bronze.raw_events (run_id);

-- ---------------------------------------------------------------------------
-- Raw markets — one row per market per run
-- Merged payload: base fields from /events nested markets +
-- volume24hr from /markets endpoint (fetched separately).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_markets (
    id              BIGSERIAL   PRIMARY KEY,
    run_id          TEXT        NOT NULL,
    market_id       TEXT        NOT NULL,
    event_id        TEXT        NOT NULL,
    source_endpoint TEXT        NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_payload     JSONB       NOT NULL,
    request_params  JSONB,

    UNIQUE (run_id, market_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_markets_run_id    ON bronze.raw_markets (run_id);
CREATE INDEX IF NOT EXISTS idx_raw_markets_event_id  ON bronze.raw_markets (event_id);

-- ---------------------------------------------------------------------------
-- Raw series — one row per unique series per run
-- Series are embedded inside the /events response (series[] array).
-- Deduplicated by series_id before insert.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_series (
    id              BIGSERIAL   PRIMARY KEY,
    run_id          TEXT        NOT NULL,
    series_id       TEXT        NOT NULL,
    source_endpoint TEXT        NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_payload     JSONB       NOT NULL,

    UNIQUE (run_id, series_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_series_run_id ON bronze.raw_series (run_id);

-- ---------------------------------------------------------------------------
-- Fetch errors — dead-letter table for failed API calls
-- Logged table — we want these to survive crashes so the
-- next pipeline run can inspect and retry only the failed entities.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.fetch_errors (
    id            BIGSERIAL   PRIMARY KEY,
    run_id        TEXT        NOT NULL,
    entity_type   TEXT        NOT NULL, -- 'event' | 'market' | 'series'
    entity_id     TEXT        NOT NULL,
    endpoint      TEXT        NOT NULL,
    error_type    TEXT        NOT NULL, -- '429' | '5xx' | 'timeout' | '404' | 'network' | 'json_error'
    error_message TEXT,
    retry_count   INT         NOT NULL DEFAULT 0,
    attempted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Upsert on (run_id, entity_type, entity_id) so rerunning the same
    -- run_id updates the error record rather than duplicating it.
    UNIQUE (run_id, entity_type, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_fetch_errors_run_id      ON bronze.fetch_errors (run_id);
CREATE INDEX IF NOT EXISTS idx_fetch_errors_entity_type ON bronze.fetch_errors (entity_type);
