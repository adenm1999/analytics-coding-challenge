with source as (
    select * from {{ source('raw', 'ad_events') }}
),

-- Remove duplicate events, keep the most recently loaded row
deduplicated as (
    select *,
        row_number() over (
            partition by event_id
            order by _loaded_at desc
        ) as rn
    from source
),

cleaned as (
    select
        event_id,
        event_type,
        event_timestamp,
        toDate(event_timestamp)                               as event_date,
        publisher_id,
        site_domain,
        ad_unit_id,
        campaign_id,                         -- NULL = unfilled, this is valid
        advertiser_id,
        lower(trim(device_type))             as device_type,
        upper(trim(country_code))            as country_code,
        lower(trim(browser))                 as browser,
        toFloat64(revenue_usd)               as revenue_usd,
        toFloat64(bid_floor_usd)             as bid_floor_usd,
        -- Normalise is_filled to boolean
        case when is_filled = 1 then true else false end      as is_filled,
        _loaded_at
    from deduplicated
    where rn = 1
        -- Exclude clearly invalid revenue rows (ETL errors)
        and revenue_usd >= 0
)

select * from cleaned
