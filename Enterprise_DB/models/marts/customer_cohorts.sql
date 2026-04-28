with

customer_orders as (
    select * from {{ ref('int_customer_order_history') }}
),

cohort_summary as (
    select
        customer_id,
        cohort_month,
        date_trunc('month', ordered_at)                     as order_month,

        count(distinct customer_id)                         as active_customers,
        count(distinct case when customer_order_number = 1
            then customer_id end)                           as new_customers,
        count(distinct order_id)                            as total_orders,

        sum(order_revenue)                                  as total_revenue,
        sum(order_gross_margin)                             as total_gross_margin,
        avg(order_revenue)                                  as avg_order_value,

        -- months since cohort started (for cohort retention analysis)
         dbt_utils.datediff('cohort_month', "date_trunc('month', ordered_at)", 'month') 
            as months_since_cohort_start

    from customer_orders
    group by 1, 2,3
)

select * from cohort_summary
