with

customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('int_order_items_enriched') }}
),

order_summary as (
    select
        order_id,
        sum(product_price)                          as order_revenue,
        sum(supply_cost)                            as order_cost,
        sum(gross_margin)                           as order_gross_margin,
        sum(case when is_food_item then product_price else 0 end) as food_revenue,
        sum(case when is_drink_item then product_price else 0 end) as drink_revenue,
        count(order_item_id)                        as item_count
    from order_items
    group by 1
),

customer_orders as (
    select
        orders.customer_id,
        orders.order_id,
        orders.ordered_at,
        orders.location_id,
        order_summary.order_revenue,
        order_summary.order_cost,
        order_summary.order_gross_margin,
        order_summary.food_revenue,
        order_summary.drink_revenue,
        order_summary.item_count,

        row_number() over (
            partition by orders.customer_id
            order by orders.ordered_at asc
        ) as customer_order_number,

        min(orders.ordered_at) over (
            partition by orders.customer_id
        ) as first_order_date,

        -- cohort month: month of first order
        date_trunc('month', min(orders.ordered_at) over (
            partition by orders.customer_id
        )) as cohort_month

    from orders
    left join order_summary on orders.order_id = order_summary.order_id
),

joined as (
    select
        customer_orders.*,
        customers.customer_name,
        case
            when customer_order_number = 1 then 'new'
            else 'returning'
        end as order_customer_type
    from customer_orders
    left join customers on customer_orders.customer_id = customers.customer_id
)

select * from joined
