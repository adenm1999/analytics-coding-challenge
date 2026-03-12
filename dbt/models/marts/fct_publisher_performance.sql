-- Grain: one row per event_date + publisher_id
-- Higher-level rollup of fct_ad_events_daily for dashboard use

with daily as (
    select * from {{ ref('fct_ad_events_daily') }}
),

publishers as (
    select * from {{ ref('dim_publishers') }}
)

select
    d.event_date,
    d.publisher_id,
    p.publisher_name,
    p.publisher_category,
    p.country,

    sum(d.impressions)           as impressions,
    sum(d.clicks)                as clicks,
    sum(d.viewable_impressions)  as viewable_impressions,
    sum(d.revenue_usd)           as revenue_usd,

    -- Weighted rates rolled up from daily grain
    toFloat64(sum(d.clicks))
        / nullIf(sum(d.impressions), 0)              as ctr,

    toFloat64(sum(d.viewable_impressions))
        / nullIf(sum(d.impressions), 0)              as viewability_rate,

    toFloat64(sumIf(d.impressions, d.fill_rate > 0))
        / nullIf(sum(d.impressions), 0)              as fill_rate

from daily d
left join publishers p using (publisher_id)
group by d.event_date, d.publisher_id, p.publisher_name, p.publisher_category, p.country
