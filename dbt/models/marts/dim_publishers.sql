with publishers as (
    select * from {{ ref('stg_publishers') }}
)

select
    publisher_id,
    publisher_name,
    publisher_category,
    primary_domain,
    account_manager,
    country,
    created_at,
    updated_at
from publishers
