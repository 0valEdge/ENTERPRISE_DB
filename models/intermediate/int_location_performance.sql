with

locations as (
    select * from {{ ref('stg_locations') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('int_order_items_enriched') }}
),

location_order_summary as (
    select
        orders.location_id,
        count(distinct orders.order_id)             as total_orders,
        count(distinct orders.customer_id)          as unique_customers,
        sum(order_items.product_price)              as total_revenue,
        sum(order_items.supply_cost)                as total_cost,
        sum(order_items.gross_margin)               as total_gross_margin,
        min(orders.ordered_at)                      as first_order_at,
        max(orders.ordered_at)                      as last_order_at,
        avg(order_items.product_price)              as avg_item_price,
        sum(case when order_items.is_food_item
            then order_items.product_price else 0
        end)                                        as food_revenue,
        sum(case when order_items.is_drink_item
            then order_items.product_price else 0
        end)                                        as drink_revenue
    from orders
    left join order_items on orders.order_id = order_items.order_id
    group by 1
),

joined as (
    select
        locations.location_id,
        locations.location_name,
        locations.tax_rate,
        locations.opened_date,

        coalesce(location_order_summary.total_orders, 0)        as total_orders,
        coalesce(location_order_summary.unique_customers, 0)    as unique_customers,
        coalesce(location_order_summary.total_revenue, 0)       as total_revenue,
        coalesce(location_order_summary.total_cost, 0)          as total_cost,
        coalesce(location_order_summary.total_gross_margin, 0)  as total_gross_margin,
        location_order_summary.first_order_at,
        location_order_summary.last_order_at,
        coalesce(location_order_summary.avg_item_price, 0)      as avg_item_price,
        coalesce(location_order_summary.food_revenue, 0)        as food_revenue,
        coalesce(location_order_summary.drink_revenue, 0)       as drink_revenue,

        case
            when coalesce(location_order_summary.total_revenue, 0) > 0
            then round(
                coalesce(location_order_summary.total_gross_margin, 0)
                / location_order_summary.total_revenue * 100, 2
            )
            else 0
        end as gross_margin_pct

    from locations
    left join location_order_summary
        on locations.location_id = location_order_summary.location_id
)

select * from joined
