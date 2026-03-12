# DESIGN.md

## 1. Data Modeling Approach

I used a standard staging → marts pattern to clearly separate raw data
cleaning from business logic.

**Staging layer** (`models/staging/`) handles:
- Deduplication on primary keys using ROW_NUMBER()
- Type casting and string normalisation (trim, lower, upper)
- Filtering invalid rows with documented decisions

**Mart layer** (`models/marts/`) handles:
- Business-level aggregations and derived metrics
- Dimension tables for publisher and campaign reporting

**Grain of `fct_ad_events_daily`:** One row per `event_date`, `publisher_id`,
`device_type`, and `country_code`. This grain supports all three dashboard
questions (revenue over time, fill rate by publisher, anomaly investigation
by country/device) without requiring additional aggregation at query time.

**Grain of `fct_publisher_performance`:** One row per `event_date` and
`publisher_id`. A higher-level rollup that joins publisher dimension data
for reporting-ready output in Lightdash.

---

## 2. Data Quality Issues Found

| Issue | What I found | How I handled it |
|-------|-------------|-----------------|
| Negative revenue | 151 rows, total -$221.55 | Excluded in stg_ad_events with `revenue_usd >= 0` filter |
| Publisher 13 negative revenue | 6,028 impressions, -$7.29 total revenue | Caught by negative revenue filter above |
| Traffic spike 2026-02-23 | 9,313 events vs normal ~4,200/day | Flagged — see anomaly section below |
| Low CPM publishers | Publishers 1,2,3,5,7 avg $0.003-0.004 vs $0.008-0.009 platform avg | Flagged — see anomaly section below |

### Anomaly Investigation: Invalid Traffic

**What I found:**

On 2026-02-23, total daily events spiked to 9,313 — more than double the
normal daily average of ~4,000-4,300 events. Investigating the breakdown
revealed a single combination responsible for ~54% of that day's traffic:

- Publisher: 13
- Browser: Samsung Internet
- Device type: Mobile
- Country: BR (Brazil)
- Event count: 5,071
- Revenue: $27.99
- Revenue per impression: ~$0.005 vs platform average ~$0.008-0.009

Additionally, Publisher 13 consistently shows negative revenue across the
full dataset (-$7.29 on 6,028 filled impressions), and Publishers 1, 2, 3,
5, and 7 all show abnormally low CPM (~$0.003-0.004) despite having the
highest impression volumes (6,000-8,672 per publisher).

**Why it matters from a business perspective:**

1. Inflated impression counts distort fill rate calculations — a publisher
   appearing to serve many ads may simply be generating invalid traffic
2. Low CPM on high-volume publishers suppresses overall platform revenue
   metrics and could lead to incorrect publisher payment calculations
3. If advertisers are charged for invalid impressions, this is a billing
   integrity issue
4. The spike pattern (single day, single browser/country/publisher combo)
   is consistent with a bot traffic burst or a misconfigured ad SDK

**How I would handle it in production:**

1. Create a separate `invalid_traffic` staging model to quarantine
   suspected bot events rather than silently filtering them
2. Define clear IVT (Invalid Traffic) classification rules:
   - Revenue per impression < $0.001 on filled impressions
   - Daily volume > 3x rolling 7-day average for a publisher
3. Exclude quarantined events from all revenue and fill rate calculations
4. Add a dbt test alerting when any publisher exceeds 20% zero/near-zero
   revenue on filled impressions
5. Escalate to the ad-tech team to implement server-side IVT filtering
   at the bid request level

---

## 3. Trade-offs

- **No incremental materialisation** — all models are views for simplicity.
  In production, `fct_ad_events_daily` should be an incremental table
  partitioned by `event_date`.
- **Invalid traffic flagged but not quarantined** — due to time constraints
  I set investigative tests to warn severity rather than building a full
  quarantine model. A production pipeline would isolate these rows.
- **fill_rate in fct_publisher_performance** is approximated from the
  daily grain rather than recalculated from raw events. This is slightly
  less accurate but avoids double-aggregation complexity.
- **dim_campaigns not fully tested** — added unique/not_null tests but
  did not add relationships tests back to fct_ad_events_daily due to
  nullable campaign_id on unfilled impressions.

---

## 4. Production Readiness

To move this to production I would:

- **Incremental materialisation** on `fct_ad_events_daily`, partitioned
  by `event_date` to avoid full table scans as data grows
- **Source freshness checks** on `raw.ad_events` to alert on pipeline
  delays — e.g. warn if data is > 6 hours stale
- **Row count anomaly tests** using `dbt-expectations` to catch sudden
  drops or spikes in daily event volume (>2x or <0.5x rolling average)
- **CI/CD pipeline** — run `dbt test` on every PR merge via GitHub Actions,
  block merges if any ERROR-severity tests fail
- **Separate `invalid_traffic` model** to quarantine bot events with full
  audit trail rather than filtering in staging
- **Exposure definitions** in dbt for the Lightdash dashboard so lineage
  is tracked end-to-end from raw source to BI layer
- **Column-level access controls** — revenue data should be restricted
  to finance and data team roles in ClickHouse
