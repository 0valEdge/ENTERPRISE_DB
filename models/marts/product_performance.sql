with

order_items as (
    select * from {{ ref('int_order_items_enriched') }}
),

product_summary as (
    select
        product_id,
        product_name,
        product_type,
        is_food_item,
        is_drink_item,

        count(distinct order_item_id)           as total_units_sold,
        count(distinct order_id)                as total_orders,
        count(distinct customer_id)             as unique_customers,

        sum(product_price)                      as total_revenue,
        sum(supply_cost)                        as total_supply_cost,
        sum(gross_margin)                       as total_gross_margin,

        avg(product_price)                      as avg_selling_price,
        avg(gross_margin)                       as avg_gross_margin,

        min(ordered_at)                         as first_sold_at,
        max(ordered_at)                         as last_sold_at,

        case
            when sum(product_price) > 0
            then round(sum(gross_margin) / sum(product_price) * 100, 2)
            else 0
        end as gross_margin_pct

    from order_items
    group by 1, 2, 3, 4, 5
)

select * from product_summary
