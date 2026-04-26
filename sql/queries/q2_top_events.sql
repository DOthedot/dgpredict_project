\pset pager off
\x
-- =============================================================================
-- Q2: Top Events by Volume and Market Count
-- =============================================================================
-- Two lenses on event popularity:
--   Q2a — Top 20 events by total 24hr volume  → where is money flowing today?
--   Q2b — Top 20 events by market count        → which events are most complex?
--
-- Reads from : gold.event_summary
-- Note       : total_volume24hr = SUM of volume24hr across all markets in the event.
--              For negRisk events one trade touches all sibling markets, so this
--              may slightly overcount vs true event-level 24hr volume — but it is
--              internally consistent for ranking across events.
-- =============================================================================

-- Q2a — Top 20 events by total 24-hour trading volume
SELECT
    event_id,
    event_title,
    series_title,
    event_time,
    total_volume24hr,
    total_volume,
    avg_liquidity,
    market_count,
    active_market_count,
    accepting_orders_count
FROM gold.event_summary
ORDER BY total_volume24hr DESC NULLS LAST
LIMIT 20;


-- =============================================================================

-- Q2b — Top 20 events by number of markets (most complex / multi-outcome events)
SELECT
    event_id,
    event_title,
    series_title,
    event_time,
    market_count,
    active_market_count,
    accepting_orders_count,
    total_volume24hr,
    total_volume
FROM gold.event_summary
ORDER BY market_count DESC
LIMIT 20;
