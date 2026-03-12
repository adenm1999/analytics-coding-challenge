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


-- =============================================
-- ANOMALY FINDINGS
-- =============================================

-- ANOMALY 1: Traffic spike on 2026-02-23
-- 9,313 events vs normal ~4,000-4,300/day
-- Revenue completely flat (~$1,228) — traffic doubled but revenue unchanged
-- Strong signal of bot/invalid traffic on that date

-- ANOMALY 2: Publishers 1,2,3,5,7 — abnormally low CPM
-- 6,000-8,672 filled impressions each
-- avg_rev ~$0.003-0.004 vs normal $0.008-0.009
-- Volume is 2-3x higher than other publishers, yield is half
-- Suggests impression count inflation

-- ANOMALY 3: Publisher 13 has negative revenue (-$7.29)
-- 6,028 impressions, total revenue -$7.29
-- Likely refund/chargeback event recorded incorrectly

-- ANOMALY 4: 151 rows with negative revenue_usd, total -$221.55
-- Handled in stg_ad_events with revenue_usd >= 0 filter

-- SPIKE DEEP DIVE: 2026-02-23
-- Publisher 13, Samsung Internet, mobile, BR = 5,071 events, $27.99 revenue
-- This single combo accounts for ~54% of the entire day's traffic
-- Normal daily total ~4,000-4,300 events across ALL publishers
-- Revenue per impression: $0.005 vs platform average ~$0.008-0.009
-- Verdict: highly suspicious — likely bot or invalid mobile traffic from Brazil
