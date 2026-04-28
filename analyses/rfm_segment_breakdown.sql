-- RFM Segment Breakdown Analysis
-- Shows the distribution of customers across RFM segments with average LTV.
-- Run ad-hoc to understand customer base health before campaigns.

with

rfm as (
    select * from {{ ref('customer_rfm_segments') }}
),

customers as (
    select
        customer_id,
        lifetime_spend,
        count_lifetime_orders
    from {{ ref('customers') }}
),

joined as (
    select
        rfm.rfm_segment,
        rfm.recency_score,
        rfm.frequency_score,
        rfm.monetary_score,
        customers.lifetime_spend,
        customers.count_lifetime_orders
    from rfm
    left join customers on rfm.customer_id = customers.customer_id
),

segment_summary as (
    select
        rfm_segment,
        count(*)                            as customer_count,
        round(avg(lifetime_spend), 2)       as avg_lifetime_spend,
        round(avg(count_lifetime_orders), 1) as avg_order_count,
        round(avg(recency_score), 2)        as avg_recency_score,
        round(avg(frequency_score), 2)      as avg_frequency_score,
        round(avg(monetary_score), 2)       as avg_monetary_score,
        sum(lifetime_spend)                 as total_segment_revenue
    from joined
    group by 1
)

select
    *,
    round(
        customer_count * 100.0 / sum(customer_count) over (),
        1
    ) as pct_of_customers,
    round(
        total_segment_revenue * 100.0 / sum(total_segment_revenue) over (),
        1
    ) as pct_of_revenue
from segment_summary
order by total_segment_revenue desc
