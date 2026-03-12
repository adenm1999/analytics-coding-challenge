{{ config(severity='warn') }}

-- Investigative test: flags filled impressions with zero revenue
-- High failure count = suspected bot / invalid traffic
-- Documented in DESIGN.md as the anomaly investigation finding
select count(*) as failures
from {{ ref('stg_ad_events') }}
where event_type = 'impression'
  and is_filled = true
  and campaign_id is not null
  and revenue_usd = 0
