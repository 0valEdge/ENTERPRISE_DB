{% snapshot customers_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=[
            'count_lifetime_orders',
            'lifetime_spend_pretax',
            'lifetime_spend',
            'customer_type'
        ],
        invalidate_hard_deletes=True
    )
}}

select
    customer_id,
    customer_name,
    count_lifetime_orders,
    first_ordered_at,
    last_ordered_at,
    lifetime_spend_pretax,
    lifetime_tax_paid,
    lifetime_spend,
    customer_type
from {{ ref('customers') }}

{% endsnapshot %}
