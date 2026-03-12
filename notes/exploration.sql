-- =============================================
-- Venatus Challenge — Data Exploration Notes
-- Run in: http://localhost:8123/play
-- =============================================

-- 1. Row counts and event types
SELECT event_type, count(*) FROM raw.ad_events GROUP BY event_type;

-- 2. Check for duplicate event_ids
SELECT event_id, count(*) as cnt
FROM raw.ad_events
GROUP BY event_id
HAVING cnt > 1;

-- 3. NULL campaign_id / fill logic
SELECT is_filled, isNull(campaign_id) as no_campaign, count(*)
FROM raw.ad_events
GROUP BY is_filled, no_campaign;

-- 4. Revenue anomalies
SELECT min(revenue_usd), max(revenue_usd), avg(revenue_usd),
       countIf(revenue_usd < 0) as negative_rev_count
FROM raw.ad_events;

-- 5. Suspicious traffic patterns
SELECT country_code, browser, device_type,
       count(*) as events, sum(revenue_usd) as rev
FROM raw.ad_events
GROUP BY country_code, browser, device_type
ORDER BY events DESC LIMIT 30;

-- 6. Publisher-level health
SELECT publisher_id, count(*) as impressions, sum(revenue_usd) as revenue
FROM raw.ad_events
WHERE event_type = 'impression'
GROUP BY publisher_id
ORDER BY impressions DESC;

-- 7. Temporal patterns
SELECT toDate(event_timestamp) as dt, count(*) as events, sum(revenue_usd) as revenue
FROM raw.ad_events
GROUP BY dt ORDER BY dt;

-- 8. Dimension duplicate checks
SELECT count(*), countDistinct(publisher_id) FROM raw.publishers;
SELECT count(*), countDistinct(campaign_id)  FROM raw.campaigns;

-- FINDINGS:
-- - Found X duplicate event_ids
-- - N rows with revenue_usd < 0
-- - Publisher X shows high impressions / zero revenue (suspected bot traffic)
-- - Browser "HeadlessChrome" appears N times with $0 revenue