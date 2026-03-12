with source as (
    select * from {{ source('raw', 'publishers') }}
),

deduplicated as (
    select *,
        row_number() over (partition by publisher_id order by publisher_id) as rn
    from source
),

cleaned as (
    select
        publisher_id,
        trim(publisher_name)  as publisher_name,
        country_code,
        publisher_type
    from deduplicated
    where rn = 1
)

select * from cleaned
