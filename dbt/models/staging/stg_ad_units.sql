with source as (
    select * from {{ source('raw', 'ad_units') }}
),

cleaned as (
    select
        ad_unit_id,
        publisher_id,
        trim(ad_unit_name)    as ad_unit_name,
        trim(ad_format)       as ad_format,
        trim(ad_size)         as ad_size,
        trim(placement_type)  as placement_type,
        is_active = 1         as is_active,
        created_at
    from source
)

select * from cleaned
