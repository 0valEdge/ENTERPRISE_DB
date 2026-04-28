-- Location Revenue Trend Analysis
-- Week-over-week revenue comparison per location.
-- Use this to identify which stores are growing or declining.

with

daily as (
    select * from {{ ref('daily_revenue') }}
),

weekly as (
    select
        location_id,
        location_name,
        date_trunc('week', order_date)      as week_start,
        sum(gross_revenue)                  as weekly_revenue,
        sum(total_orders)                   as weekly_orders,
        sum(unique_customers)               as weekly_customers,
        avg(avg_order_revenue)              as avg_order_value
    from daily
    group by 1, 2, 3
),

with_prior_week as (
    select
        *,
        lag(weekly_revenue) over (
            partition by location_id
            order by week_start
        )                                   as prior_week_revenue,

        lag(weekly_orders) over (
            partition by location_id
            order by week_start
        )                                   as prior_week_orders

    from weekly
),

final as (
    select
        *,
        case
            when prior_week_revenue > 0
            then round(
                (weekly_revenue - prior_week_revenue) / prior_week_revenue * 100,
                1
            )
            else null
        end                                 as revenue_wow_pct_change,

        case
            when prior_week_orders > 0
            then round(
                (weekly_orders - prior_week_orders)::numeric / prior_week_orders * 100,
                1
            )
            else null
        end                                 as orders_wow_pct_change

    from with_prior_week
)

select * from final
order by location_name, week_start
