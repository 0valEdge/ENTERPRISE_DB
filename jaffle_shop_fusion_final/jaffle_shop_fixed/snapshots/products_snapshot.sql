{% snapshot products_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='check',
        check_cols=['product_price', 'product_name', 'is_food_item', 'is_drink_item'],
        invalidate_hard_deletes=True
    )
}}

select
    product_id,
    product_name,
    product_type,
    product_description,
    product_price,
    is_food_item,
    is_drink_item
from {{ ref('stg_products') }}

{% endsnapshot %}
