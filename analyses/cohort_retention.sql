-- Cohort Retention Analysis
-- This analysis calculates month-over-month customer retention by cohort.
-- Run this in dbt Cloud IDE or export to your BI tool.

with

cohorts as (
    select * from {{ ref('customer_cohorts') }}
),

cohort_sizes as (
    select
        cohort_month,
        sum(new_customers) as cohort_size
    from cohorts
    where months_since_cohort_start = 0
    group by 1
),

retention as (
    select
        cohorts.cohort_month,
        cohorts.order_month,
        cohorts.months_since_cohort_start,
        cohorts.active_customers,
        cohort_sizes.cohort_size,
        round(
            cohorts.active_customers::numeric / nullif(cohort_sizes.cohort_size, 0) * 100,
            1
        ) as retention_rate_pct,
        cohorts.total_revenue,
        cohorts.avg_order_value
    from cohorts
    left join cohort_sizes on cohorts.cohort_month = cohort_sizes.cohort_month
)

select
    cohort_month,
    order_month,
    months_since_cohort_start,
    cohort_size,
    active_customers,
    retention_rate_pct,
    total_revenue,
    avg_order_value
from retention
order by cohort_month, months_since_cohort_start
