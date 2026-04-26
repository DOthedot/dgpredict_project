-- =============================================================================
-- Gold Layer Schema
-- =============================================================================
-- Purpose : Business-facing marts for analysis and terminal-style serving.
--           Gold is the ONLY layer queried for business questions — it never
--           reads from bronze directly.
--
-- Design  : Materialized views over silver tables.
--           Regular VIEWs recompute on every query (slow at scale).
--           Materialized views store results on disk — gold queries are instant.
--           REFRESH MATERIALIZED VIEW CONCURRENTLY runs at the end of each
--           pipeline run, keeping gold in sync with the latest silver data
--           without blocking reads during the refresh.
--
--           CONCURRENTLY requires a unique index on each materialized view.
--           Views are created with no initial data (populated on first REFRESH).
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- ---------------------------------------------------------------------------
-- market_enriched
-- One row per market in the 500-event football universe.
-- Joins market data with event and series context for full analytical picture.
-- Used for: Q1 (top 25 markets by volume24hr), terminal-style serving.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.market_enriched AS
SELECT
    m.condition_id,
    m.market_id,
    m.event_id,
    m.question          AS market_question,
    e.title             AS event_title,
    s.title             AS series_title,
    -- Use game_start_time (actual kickoff) as event_time.
    -- Falls back to event end_date if game_start_time is null (rare for older events).
    COALESCE(m.game_start_time, e.game_start_time, e.end_date) AS event_time,
    m.active,
    m.closed,
    m.accepting_orders,
    m.volume24hr,
    m.volume            AS total_volume,
    m.liquidity,
    m.last_trade_price,
    m.best_bid,
    m.best_ask,
    m.outcomes,
    m.outcome_prices,
    e.neg_risk,
    e.series_slug
FROM silver.markets m
JOIN silver.events e
    ON e.event_id = m.event_id
-- LEFT JOIN on is_primary = TRUE ensures exactly one series row per market.
-- Without this filter an event in 2 series would produce 2 rows per market,
-- doubling all volume figures in downstream aggregations.
LEFT JOIN silver.event_series es
    ON es.event_id = e.event_id AND es.is_primary = TRUE
LEFT JOIN silver.series s
    ON s.series_id = es.series_id
WITH NO DATA;

-- Required for REFRESH MATERIALIZED VIEW CONCURRENTLY.
-- CONCURRENTLY keeps old data queryable during refresh (no read locks).
CREATE UNIQUE INDEX IF NOT EXISTS idx_market_enriched_condition_id
    ON gold.market_enriched (condition_id);

-- Support fast ORDER BY volume24hr for Q1
CREATE INDEX IF NOT EXISTS idx_market_enriched_volume24hr
    ON gold.market_enriched (volume24hr DESC NULLS LAST);

-- ---------------------------------------------------------------------------
-- event_summary
-- One row per event with aggregated market-level metrics.
-- Used for: Q2 (top events by volume/market count), Q3 (concentration).
--
-- total_volume24hr: SUM of market.volume24hr across all markets in the event.
-- NOTE: For negRisk football events, one trade touches all sibling markets
-- simultaneously, so this SUM may slightly overcount vs true event-level
-- 24hr volume. However, it is internally consistent for ranking and the
-- concentration ratio (max/total) remains valid for the same reason.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.event_summary AS
SELECT
    e.event_id,
    e.title             AS event_title,
    s.title             AS series_title,
    COALESCE(e.game_start_time, e.end_date) AS event_time,

    -- Market counts
    COUNT(m.condition_id)                                           AS market_count,
    COUNT(m.condition_id) FILTER (WHERE m.active AND NOT m.closed) AS active_market_count,
    COUNT(m.condition_id) FILTER (WHERE m.accepting_orders)        AS accepting_orders_count,

    -- Volume aggregations across markets for this event
    COALESCE(SUM(m.volume24hr), 0)   AS total_volume24hr,
    COALESCE(SUM(m.volume),     0)   AS total_volume,

    -- Liquidity aggregations
    COALESCE(AVG(m.liquidity),  0)   AS avg_liquidity,

    -- Max per-market values — used for concentration calculation in Q3:
    -- concentration_pct = max_volume24hr / total_volume24hr * 100
    COALESCE(MAX(m.volume24hr), 0)   AS max_volume24hr,
    COALESCE(MAX(m.liquidity),  0)   AS max_liquidity

FROM silver.events e
LEFT JOIN silver.markets m
    ON m.event_id = e.event_id
LEFT JOIN silver.event_series es
    ON es.event_id = e.event_id AND es.is_primary = TRUE
LEFT JOIN silver.series s
    ON s.series_id = es.series_id
GROUP BY
    e.event_id,
    e.title,
    e.game_start_time,
    e.end_date,
    s.title
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_event_summary_event_id
    ON gold.event_summary (event_id);

CREATE INDEX IF NOT EXISTS idx_event_summary_volume24hr
    ON gold.event_summary (total_volume24hr DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS idx_event_summary_market_count
    ON gold.event_summary (market_count DESC);
