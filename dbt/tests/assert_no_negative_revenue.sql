{{ config(severity='warn') }}

-- Investigative test: flags negative revenue rows (likely ETL errors)
-- Documented in DESIGN.md as a data quality finding
select count(*) as failures
from {{ ref('stg_ad_events') }}
where revenue_usd < 0
