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
        trim(publisher_name)      as publisher_name,
        trim(publisher_category)  as publisher_category,
        trim(primary_domain)      as primary_domain,
        trim(account_manager)     as account_manager,
        trim(country)             as country,
        created_at,
        updated_at
    from deduplicated
    where rn = 1
)

select * from cleaned
