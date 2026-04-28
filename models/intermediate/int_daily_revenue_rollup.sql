with

orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('int_order_items_enriched') }}
),

daily_order_summary as (
    select
        orders.ordered_at                                           as order_date,
        orders.location_id,

        count(distinct orders.order_id)                             as total_orders,
        count(distinct orders.customer_id)                         as unique_customers,

        sum(order_items.product_price)                             as gross_revenue,
        sum(order_items.supply_cost)                               as total_supply_cost,
        sum(order_items.gross_margin)                              as gross_margin,

        sum(case when order_items.is_food_item
            then order_items.product_price else 0 end)             as food_revenue,
        sum(case when order_items.is_drink_item
            then order_items.product_price else 0 end)             as drink_revenue,

        count(distinct case when order_items.is_food_item
            then order_items.order_id end)                         as food_orders,
        count(distinct case when order_items.is_drink_item
            then order_items.order_id end)                         as drink_orders,

        count(order_items.order_item_id)                           as total_items_sold,

        -- rolling 7-day order count per location
        sum(count(distinct orders.order_id)) over (
            partition by orders.location_id
            order by orders.ordered_at
            rows between 6 preceding and current row
        )                                                           as rolling_7d_orders,

        -- revenue per order
        case
            when count(distinct orders.order_id) > 0
            then round(
                sum(order_items.product_price)
                / count(distinct orders.order_id), 2
            )
            else 0
        end                                                         as avg_order_revenue

    from orders
    left join order_items on orders.order_id = order_items.order_id
    group by 1, 2
)

select * from daily_order_summary
