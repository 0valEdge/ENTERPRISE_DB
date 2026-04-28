{% snapshot orders_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='order_id',
        strategy='timestamp',
        updated_at='ordered_at',
        invalidate_hard_deletes=True
    )
}}

select
    order_id,
    customer_id,
    location_id,
    subtotal,
    tax_paid,
    order_total,
    ordered_at
from {{ ref('stg_orders') }}

{% endsnapshot %}
