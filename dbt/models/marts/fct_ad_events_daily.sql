-- Grain: one row per event_date + publisher_id + device_type + country_code
-- This grain supports all dashboard questions without extra query-time aggregation

with events as (
    select * from {{ ref('stg_ad_events') }}
)

select
    event_date,
    publisher_id,
    device_type,
    country_code,

    -- Core volume metrics
    countIf(event_type = 'impression')             as impressions,
    countIf(event_type = 'click')                  as clicks,
    countIf(event_type = 'viewable_impression')    as viewable_impressions,

    -- Revenue
    sum(revenue_usd)                               as revenue_usd,

    -- Fill rate: filled impressions / total impressions
    countIf(event_type = 'impression' and is_filled = true)
        / nullIf(countIf(event_type = 'impression'), 0)  as fill_rate,

    -- Click-through rate
    toFloat64(countIf(event_type = 'click'))
        / nullIf(countIf(event_type = 'impression'), 0)  as ctr,

    -- Viewability rate
    toFloat64(countIf(event_type = 'viewable_impression'))
        / nullIf(countIf(event_type = 'impression'), 0)  as viewability_rate

from events
group by event_date, publisher_id, device_type, country_code
