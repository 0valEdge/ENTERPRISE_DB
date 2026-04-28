with

daily as (
    select * from {{ ref('int_daily_revenue_rollup') }}
),

locations as (
    select
        location_id,
        location_name
    from {{ ref('stg_locations') }}
),

joined as (
    select
        daily.order_date,
        daily.location_id,
        locations.location_name,
        daily.total_orders,
        daily.unique_customers,
        daily.gross_revenue,
        daily.total_supply_cost,
        daily.gross_margin,
        daily.food_revenue,
        daily.drink_revenue,
        daily.food_orders,
        daily.drink_orders,
        daily.total_items_sold,
        daily.avg_order_revenue,
        daily.rolling_7d_orders
    from daily
    left join locations on daily.location_id = locations.location_id
)

select * from joined
