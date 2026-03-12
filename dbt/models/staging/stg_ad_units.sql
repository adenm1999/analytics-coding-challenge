with source as (
    select * from {{ source('raw', 'ad_units') }}
),

cleaned as (
    select
        ad_unit_id,
        publisher_id,
        trim(ad_unit_name)  as ad_unit_name,
        trim(ad_unit_type)  as ad_unit_type,
        trim(placement)     as placement
    from source
)

select * from cleaned
