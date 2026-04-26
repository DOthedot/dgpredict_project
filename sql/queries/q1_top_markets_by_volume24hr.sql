\pset pager off
\x
-- =============================================================================
-- Q1: Top 25 Football Markets by 24-Hour Volume
-- =============================================================================
-- Which individual markets are generating the most trading activity right now?
--
-- Reads from : gold.market_enriched
-- Sorted by  : volume24hr DESC — highest rolling 24hr trading volume first
-- Use case   : Identify the hottest markets for terminal-style serving,
--              real-time dashboards, or market prioritisation.
-- =============================================================================

SELECT
    market_question,
    event_title,
    series_title,
    event_time,
    volume24hr,
    total_volume,
    liquidity,
    last_trade_price,
    best_bid,
    best_ask,
    active,
    closed,
    accepting_orders
FROM gold.market_enriched
ORDER BY volume24hr DESC NULLS LAST
LIMIT 25;
