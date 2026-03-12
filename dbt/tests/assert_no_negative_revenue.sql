-- This test fails if any negative revenue rows exist in staging
-- Revenue < 0 indicates an ETL error and should never reach analytics models
select count(*) as failures
from {{ ref('stg_ad_events') }}
where revenue_usd < 0
