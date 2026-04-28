with

order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

supplies as (
    select * from {{ ref('stg_supplies') }}
),

supply_costs as (
    select
        product_id,
        sum(supply_cost) as total_supply_cost,
        count(*) as supply_count,
        sum(case when is_perishable_supply then supply_cost else 0 end) as perishable_supply_cost
    from supplies
    group by 1
),

enriched as (
    select
        order_items.order_item_id,
        order_items.order_id,
        order_items.product_id,

        orders.customer_id,
        orders.location_id,
        orders.ordered_at,

        products.product_name,
        products.product_type,
        products.product_price,
        products.is_food_item,
        products.is_drink_item,

        supply_costs.total_supply_cost as supply_cost,
        supply_costs.supply_count,
        supply_costs.perishable_supply_cost,

        -- derived revenue metrics
        products.product_price - coalesce(supply_costs.total_supply_cost, 0) as gross_margin,
        case
            when products.product_price > 0
            then round(
                (products.product_price - coalesce(supply_costs.total_supply_cost, 0))
                / products.product_price * 100, 2
            )
            else 0
        end as gross_margin_pct

    from order_items
    left join orders on order_items.order_id = orders.order_id
    left join products on order_items.product_id = products.product_id
    left join supply_costs on order_items.product_id = supply_costs.product_id
)

select * from enriched
