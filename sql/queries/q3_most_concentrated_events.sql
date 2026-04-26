\pset pager off
\x
-- =============================================================================
-- Q3: Top 20 Most Concentrated Events
-- =============================================================================
-- Which events have their trading volume dominated by a single market?
--
-- Concentration is defined as:
--     concentration_pct = (max_volume24hr / total_volume24hr) * 100
--
-- A concentration of 95% means one market accounts for 95% of the event's
-- total 24hr volume — a highly concentrated event where traders overwhelmingly
-- prefer one outcome question (e.g. "Will Team A win?" over "Correct Score?").
--
-- A concentration near 33% (for a 3-market event) means volume is spread
-- evenly across all markets — no single outcome is being bet on heavily.
--
-- Reads from : gold.event_summary
-- Filter     : total_volume24hr > 0 — events with no 24hr trading are excluded
--              since concentration is undefined when the denominator is zero.
-- Sorted by  : concentration_pct DESC — most dominated events first
-- =============================================================================

SELECT
    event_id,
    event_title,
    series_title,
    event_time,
    market_count,
    total_volume24hr,
    max_volume24hr,
    ROUND(
        (max_volume24hr / total_volume24hr) * 100,
        2
    ) AS concentration_pct
FROM gold.event_summary
WHERE total_volume24hr > 0
ORDER BY concentration_pct DESC
LIMIT 20;
