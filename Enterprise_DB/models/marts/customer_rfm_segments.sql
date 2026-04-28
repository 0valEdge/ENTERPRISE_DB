with

rfm as (
    select * from {{ ref('int_customer_rfm') }}
)

select * from rfm