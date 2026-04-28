-- Product Margin Deep Dive
-- Ranks products by gross margin and flags underperformers.
-- Useful for merchandising and pricing decisions.

with

products as (
    select * from {{ ref('product_performance') }}
),

ranked as (
    select
        product_id,
        product_name,
        product_type,
        is_food_item,
        is_drink_item,
        total_units_sold,
        total_revenue,
        total_supply_cost,
        total_gross_margin,
        gross_margin_pct,
        avg_selling_price,

        rank() over (order by total_revenue desc)       as revenue_rank,
        rank() over (order by gross_margin_pct desc)    as margin_rank,
        rank() over (order by total_units_sold desc)    as volume_rank,

        case
            when gross_margin_pct < 10 then 'low_margin'
            when gross_margin_pct between 10 and 30 then 'medium_margin'
            else 'high_margin'
        end as margin_tier,

        case
            when total_units_sold = 0 then 'no_sales'
            when total_units_sold < 10 then 'low_volume'
            when total_units_sold < 100 then 'medium_volume'
            else 'high_volume'
        end as volume_tier

    from products
)

select * from ranked
order by revenue_rank
