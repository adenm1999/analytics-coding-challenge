with source as (
    select * from {{ source('raw', 'campaigns') }}
),

cleaned as (
    select
        campaign_id,
        trim(campaign_name)             as campaign_name,
        advertiser_id,
        trim(advertiser_name)           as advertiser_name,
        campaign_start_date,
        campaign_end_date,
        toFloat64(campaign_budget_usd)  as campaign_budget_usd,
        trim(campaign_status)           as campaign_status,
        trim(targeting_device_types)    as targeting_device_types,
        trim(targeting_countries)       as targeting_countries,
        created_at
    from source
)

select * from cleaned
