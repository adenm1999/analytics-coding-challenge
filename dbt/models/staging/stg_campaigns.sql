with source as (
    select * from {{ source('raw', 'campaigns') }}
),

cleaned as (
    select
        campaign_id,
        advertiser_id,
        trim(campaign_name)    as campaign_name,
        trim(campaign_status)  as campaign_status,
        start_date,
        end_date
    from source
)

select * from cleaned
