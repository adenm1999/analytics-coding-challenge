-- Flags filled impressions with a campaign but zero revenue
-- A large failure count = suspected bot / invalid traffic
-- This is an investigative test — document findings in DESIGN.md
select count(*) as failures
from {{ ref('stg_ad_events') }}
where event_type = 'impression'
  and is_filled = true
  and campaign_id is not null
  and revenue_usd = 0
